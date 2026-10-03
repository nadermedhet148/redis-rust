//! Crash recovery against the real `rkv` binary: kill -9 the server while
//! clients are writing, restart it on the same WAL, and check that every
//! acknowledged write survived.
//!
//! `Child::kill` is SIGKILL on Unix and TerminateProcess on Windows: the process
//! gets no chance to flush or clean up, exactly like `kill -9`.

use std::io::Write;
use std::net::{SocketAddr, TcpListener as StdListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const POLICIES: [&str; 3] = ["always", "every-sec", "never"];

struct Server {
    child: Child,
    addr: SocketAddr,
}

impl Server {
    async fn start(wal: &Path, fsync: &str) -> Self {
        // Grab a free port from the OS, release it, and hand it to the server.
        let addr = StdListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_rkv"))
            .args(["--addr", &addr.to_string(), "--store", "sharded"])
            .args(["--fsync", fsync, "--wal"])
            .arg(wal)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(addr).await.is_err() {
            assert!(Instant::now() < deadline, "server did not start");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Self { child, addr }
    }

    /// kill -9: no shutdown, no flush.
    fn kill9(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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

    /// `None` once the server is gone.
    async fn send(&mut self, cmd: &str) -> Option<String> {
        self.writer
            .write_all(format!("{cmd}\n").as_bytes())
            .await
            .ok()?;
        self.lines.next_line().await.ok()?
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill9_mid_write_loses_no_acknowledged_write() {
    const CLIENTS: usize = 8;
    for fsync in POLICIES {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join("rkv.wal");
        let server = Server::start(&wal, fsync).await;

        // Each client writes k{c}:{i} = v{i} in order and records how many were acked.
        let acked: Arc<Vec<AtomicUsize>> =
            Arc::new((0..CLIENTS).map(|_| AtomicUsize::new(0)).collect());
        let writers: Vec<_> = (0..CLIENTS)
            .map(|c| {
                let acked = acked.clone();
                let addr = server.addr;
                tokio::spawn(async move {
                    let mut cl = Client::connect(addr).await;
                    for i in 0.. {
                        match cl.send(&format!("SET k{c}:{i} v{i}")).await.as_deref() {
                            Some("OK") => acked[c].store(i + 1, Ordering::SeqCst),
                            None => return, // server died
                            Some(other) => panic!("unexpected reply {other:?}"),
                        }
                    }
                })
            })
            .collect();

        // Let writes flow, then pull the plug mid-stream.
        tokio::time::sleep(Duration::from_millis(300)).await;
        server.kill9();
        for w in writers {
            w.await.unwrap();
        }
        let acked: Vec<usize> = acked.iter().map(|a| a.load(Ordering::SeqCst)).collect();
        let total: usize = acked.iter().sum();
        assert!(total > 100, "{fsync}: only {total} writes before the kill");

        let server = Server::start(&wal, fsync).await;
        let mut cl = Client::connect(server.addr).await;
        for (c, &n) in acked.iter().enumerate() {
            for i in 0..n {
                assert_eq!(
                    cl.send(&format!("GET k{c}:{i}")).await.as_deref(),
                    Some(format!("v{i}").as_str()),
                    "{fsync}: acknowledged write k{c}:{i} lost after kill -9"
                );
            }
            // The one write in flight at the kill may or may not have made it;
            // nothing after it can exist.
            assert_eq!(
                cl.send(&format!("GET k{c}:{}", n + 1)).await.as_deref(),
                Some("(nil)"),
                "{fsync}: write k{c}:{} appeared but was never sent",
                n + 1
            );
        }
        println!("fsync={fsync}: {total} acknowledged writes, all recovered after kill -9");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deletes_and_overwrites_survive_restart() {
    for fsync in POLICIES {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join("rkv.wal");

        let server = Server::start(&wal, fsync).await;
        let mut cl = Client::connect(server.addr).await;
        for cmd in [
            "SET a 1",
            "SET b 2",
            "SET a 3",
            "DEL b",
            "SET c spaces are kept",
        ] {
            cl.send(cmd).await.unwrap();
        }
        server.kill9();

        let server = Server::start(&wal, fsync).await;
        let mut cl = Client::connect(server.addr).await;
        assert_eq!(cl.send("GET a").await.as_deref(), Some("3"), "{fsync}");
        assert_eq!(cl.send("GET b").await.as_deref(), Some("(nil)"), "{fsync}");
        assert_eq!(
            cl.send("GET c").await.as_deref(),
            Some("spaces are kept"),
            "{fsync}"
        );
    }
}

/// A crash in the middle of writing a record leaves half of it on disk. The
/// server must start, keep every complete record, and drop the partial one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torn_record_at_tail_is_discarded_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal: PathBuf = dir.path().join("rkv.wal");

    let server = Server::start(&wal, "always").await;
    let mut cl = Client::connect(server.addr).await;
    for i in 0..100 {
        assert_eq!(
            cl.send(&format!("SET k{i} v{i}")).await.as_deref(),
            Some("OK")
        );
    }
    server.kill9();
    let good_len = std::fs::metadata(&wal).unwrap().len();

    // Half a record: a header promising 50 bytes, then only 10 of them.
    let mut f = std::fs::OpenOptions::new().append(true).open(&wal).unwrap();
    f.write_all(&50u32.to_le_bytes()).unwrap();
    f.write_all(&0xDEAD_BEEFu32.to_le_bytes()).unwrap();
    f.write_all(&[1; 10]).unwrap();
    drop(f);

    let server = Server::start(&wal, "always").await;
    assert_eq!(
        std::fs::metadata(&wal).unwrap().len(),
        good_len,
        "partial record was not truncated"
    );
    let mut cl = Client::connect(server.addr).await;
    for i in 0..100 {
        assert_eq!(
            cl.send(&format!("GET k{i}")).await.as_deref(),
            Some(format!("v{i}").as_str())
        );
    }
    // New writes after recovery land on a clean tail and survive another crash.
    assert_eq!(cl.send("SET after recovery").await.as_deref(), Some("OK"));
    server.kill9();
    let server = Server::start(&wal, "always").await;
    let mut cl = Client::connect(server.addr).await;
    assert_eq!(cl.send("GET after").await.as_deref(), Some("recovery"));
    assert_eq!(cl.send("GET k99").await.as_deref(), Some("v99"));
}
