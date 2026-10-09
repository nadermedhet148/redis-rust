//! `rkv-backup` against an in-process leader: the real binary, the real protocol.

use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;

use rkv::db::{Db, StoreKind};
use rkv::wal::FsyncPolicy;

async fn start_leader() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rkv::server::run(listener, Db::new(StoreKind::Sharded)));
    addr
}

struct Client {
    lines: tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl Client {
    async fn connect(addr: SocketAddr) -> Self {
        let (r, w) = TcpStream::connect(addr).await.unwrap().into_split();
        Self {
            lines: BufReader::new(r).lines(),
            writer: w,
        }
    }

    /// Send many commands in one write, then read all the replies (pipelining).
    async fn pipeline(&mut self, cmds: impl Iterator<Item = String>) {
        let cmds: Vec<String> = cmds.collect();
        self.writer
            .write_all(
                format!(
                    "{}
",
                    cmds.join(
                        "
"
                    )
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        for _ in &cmds {
            self.lines.next_line().await.unwrap().unwrap();
        }
    }

    async fn send(&mut self, cmd: &str) -> String {
        self.writer
            .write_all(format!("{cmd}\n").as_bytes())
            .await
            .unwrap();
        self.lines.next_line().await.unwrap().unwrap()
    }
}

/// Run `rkv-backup <args>`; returns (exit code, stdout).
async fn backup(args: &[&str]) -> (i32, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_rkv-backup"))
        .args(args)
        .output()
        .await
        .unwrap();
    (
        out.status.code().unwrap(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

/// What a server started with `--wal <file>` would load (on a copy, so the
/// backup itself is left untouched).
fn restore(file: &Path) -> Db {
    let copy = file.with_extension("restored");
    std::fs::copy(file, &copy).unwrap();
    Db::open(StoreKind::Sharded, &copy, FsyncPolicy::Never)
        .unwrap()
        .0
}

fn digest_in(output: &str) -> &str {
    output
        .split([' ', ','])
        .find_map(|w| w.strip_prefix("digest="))
        .unwrap_or_else(|| panic!("no digest in {output:?}"))
}

#[tokio::test]
async fn snapshot_restores_to_identical_data() {
    let leader = start_leader().await;
    let mut c = Client::connect(leader).await;
    for i in 0..500 {
        c.send(&format!("SET k{i} value number {i}")).await;
    }
    for i in (0..500).step_by(3) {
        c.send(&format!("DEL k{i}")).await;
    }
    let digest = c.send("DIGEST").await;

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("backup.rkv");
    let (code, out) = backup(&[
        "snapshot",
        "--from",
        &leader.to_string(),
        "--out",
        file.to_str().unwrap(),
    ])
    .await;
    assert_eq!(code, 0, "{out}");
    assert_eq!(digest_in(&out), digest);

    let db = restore(&file);
    assert_eq!(format!("{:016x}", db.digest()), digest);
    assert_eq!(db.len(), 333);
    assert_eq!(db.get("k1").unwrap(), "value number 1");
    assert!(!dir.path().join("backup.rkv.tmp").exists());
}

/// Writers keep updating pairs (a_i, b_i) in order: SET a_i n, then SET b_i n.
/// In any real point in time, a_i == b_i or a_i == b_i + 1. A fuzzy snapshot
/// alone can break that (b_i copied after an update, a_i before it). Because
/// the backup also replays the stream up to `consistent_at`, the restored data
/// must satisfy it for every pair.
///
/// 200k filler keys make copying the snapshot take milliseconds, so many pair
/// updates land in the middle of it. (Checked: with the replay to
/// `consistent_at` removed from rkv-backup, this test fails.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_taken_under_load_is_a_consistent_point_in_time() {
    const PAIRS: usize = 32;
    let leader = start_leader().await;
    let mut filler = Client::connect(leader).await;
    for chunk in 0..200 {
        filler
            .pipeline((0..1000).map(|i| format!("SET filler{chunk}-{i} some filler value")))
            .await;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..PAIRS)
        .map(|i| {
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut c = Client::connect(leader).await;
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    c.send(&format!("SET a{i} {n}")).await;
                    c.send(&format!("SET b{i} {n}")).await;
                    n += 1;
                }
                n
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("backup.rkv");
    let (code, out) = backup(&[
        "snapshot",
        "--from",
        &leader.to_string(),
        "--out",
        file.to_str().unwrap(),
    ])
    .await;
    stop.store(true, Ordering::Relaxed);
    let mut total = 0;
    for w in writers {
        total += w.await.unwrap();
    }
    assert_eq!(code, 0, "{out}");
    assert!(total > 100, "writers barely ran ({total} rounds)");

    let db = restore(&file);
    let num = |k: String| -> i64 {
        db.get(&k)
            .map_or(-1, |v| std::str::from_utf8(&v).unwrap().parse().unwrap())
    };
    for i in 0..PAIRS {
        let (a, b) = (num(format!("a{i}")), num(format!("b{i}")));
        assert!(
            a == b || a == b + 1,
            "pair {i}: a={a} b={b} is not a real state"
        );
    }
}

#[tokio::test]
async fn verify_flags_a_torn_tail_and_restore_cuts_it() {
    let leader = start_leader().await;
    let mut c = Client::connect(leader).await;
    for i in 0..50 {
        c.send(&format!("SET k{i} {i}")).await;
    }
    let digest = c.send("DIGEST").await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("backup.rkv");
    let path = file.to_str().unwrap();
    backup(&["snapshot", "--from", &leader.to_string(), "--out", path]).await;

    let (code, out) = backup(&["verify", path]).await;
    assert_eq!(code, 0, "{out}");
    assert_eq!(digest_in(&out), digest);
    assert!(out.contains("torn_bytes=0"), "{out}");

    // Half a record at the end, as if the backup process died mid-write.
    let mut bytes = std::fs::read(&file).unwrap();
    bytes.extend_from_slice(&rkv::wal::encode_set("half", b"written")[..9]);
    std::fs::write(&file, &bytes).unwrap();

    let (code, out) = backup(&["verify", path]).await;
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("torn_bytes=9"), "{out}");
    assert_eq!(format!("{:016x}", restore(&file).digest()), digest);
}

#[tokio::test]
async fn follow_keeps_the_backup_up_to_date() {
    let leader = start_leader().await;
    let mut c = Client::connect(leader).await;
    c.send("SET before follow").await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("live.rkv");
    let mut child = Command::new(env!("CARGO_BIN_EXE_rkv-backup"))
        .args(["follow", "--from", &leader.to_string(), "--out"])
        .arg(&file)
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !file.exists() {
        assert!(tokio::time::Instant::now() < deadline, "no backup file");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for i in 0..100 {
        c.send(&format!("SET after{i} {i}")).await;
    }
    c.send("DEL before").await;
    let digest = c.send("DIGEST").await;

    loop {
        let (_, out) = backup(&["verify", file.to_str().unwrap()]).await;
        if digest_in(&out) == digest {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "backup never caught up: {out}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    child.kill().await.unwrap();
    assert_eq!(restore(&file).len(), 100);
}
