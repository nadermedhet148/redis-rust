//! Concurrency safety: every test runs against every store kind.
//!
//! What "safe" means here:
//! - no lost writes: every acknowledged SET is visible afterwards
//! - no torn values: a GET returns exactly one value that was SET, never a mix
//! - read-your-writes on a connection, even while others hammer the same shards
//! - no deadlocks: everything finishes under a timeout

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use rkv::db::{Db, StoreKind};

const KINDS: [StoreKind; 3] = [StoreKind::Mutex, StoreKind::Rwlock, StoreKind::Sharded];
const TIMEOUT: Duration = Duration::from_secs(60);

fn threads() -> usize {
    thread::available_parallelism()
        .map_or(8, |n| n.get())
        .max(4)
}

/// A value whose bytes are all `id`: if two writes ever interleave, a reader sees mixed bytes.
fn homogeneous(id: u8, len: usize) -> Bytes {
    Bytes::from(vec![id; len])
}

fn assert_homogeneous(v: &[u8], len: usize) {
    assert_eq!(v.len(), len, "value has wrong length");
    assert!(
        v.iter().all(|&b| b == v[0]),
        "torn value: {:?}...",
        &v[..16]
    );
}

// ---------- Db directly, with OS threads (maximum contention, no network) ----------

#[test]
fn no_lost_writes_across_threads() {
    for kind in KINDS {
        let db = Db::new(kind);
        let per_thread = 5_000;
        thread::scope(|s| {
            for t in 0..threads() {
                let db = db.clone();
                s.spawn(move || {
                    for i in 0..per_thread {
                        db.set(format!("t{t}:k{i}"), Bytes::from(format!("{t}:{i}")));
                    }
                });
            }
        });
        for t in 0..threads() {
            for i in 0..per_thread {
                assert_eq!(
                    db.get(&format!("t{t}:k{i}")).as_deref(),
                    Some(format!("{t}:{i}").as_bytes()),
                    "{kind:?}: lost write t{t}:k{i}"
                );
            }
        }
    }
}

#[test]
fn no_torn_values_on_contended_keys() {
    const LEN: usize = 1024;
    const KEYS: usize = 8; // few keys => heavy contention on each
    for kind in KINDS {
        let db = Db::new(kind);
        for k in 0..KEYS {
            db.set(format!("hot{k}"), homogeneous(0, LEN));
        }
        let stop = AtomicBool::new(false);
        thread::scope(|s| {
            let writers = threads() / 2;
            for t in 0..writers {
                let db = db.clone();
                let stop = &stop;
                s.spawn(move || {
                    let mut i = 0usize;
                    while !stop.load(Ordering::Relaxed) {
                        db.set(format!("hot{}", i % KEYS), homogeneous(t as u8 + 1, LEN));
                        i += 1;
                    }
                });
            }
            for _ in writers..threads() {
                let db = db.clone();
                let stop = &stop;
                s.spawn(move || {
                    let mut i = 0usize;
                    while !stop.load(Ordering::Relaxed) {
                        let v = db.get(&format!("hot{}", i % KEYS)).expect("key vanished");
                        assert_homogeneous(&v, LEN);
                        i += 1;
                    }
                });
            }
            thread::sleep(Duration::from_millis(500));
            stop.store(true, Ordering::Relaxed);
        });
    }
}

#[test]
fn read_your_writes_and_deletes_under_contention() {
    for kind in KINDS {
        let db = Db::new(kind);
        thread::scope(|s| {
            for t in 0..threads() {
                let db = db.clone();
                s.spawn(move || {
                    // Every thread uses the same small key-name pattern, so with
                    // sharding they all land on the same few shards.
                    for round in 0..2_000 {
                        let key = format!("k{}:{t}", round % 4);
                        let val = Bytes::from(format!("{t}/{round}"));
                        db.set(key.clone(), val.clone());
                        assert_eq!(db.get(&key), Some(val), "{kind:?}: lost own write");
                        assert!(db.del(&key), "{kind:?}: own key missing on DEL");
                        assert_eq!(db.get(&key), None, "{kind:?}: DEL not visible");
                        assert!(!db.del(&key), "{kind:?}: double DEL succeeded");
                    }
                });
            }
        });
    }
}

#[test]
fn db_clones_share_one_store() {
    for kind in KINDS {
        let a = Db::new(kind);
        let b = a.clone();
        a.set("x".into(), Bytes::from_static(b"1"));
        assert_eq!(b.get("x").as_deref(), Some(&b"1"[..]));
        assert!(b.del("x"));
        assert_eq!(a.get("x"), None);
        // A fresh Db is independent.
        assert_eq!(Db::new(kind).get("x"), None);
    }
}

// ---------- Through the TCP server ----------

async fn start_server(kind: StoreKind) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rkv::server::run(listener, Db::new(kind)));
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

    async fn send(&mut self, cmd: &str) -> String {
        self.writer
            .write_all(format!("{cmd}\n").as_bytes())
            .await
            .unwrap();
        self.lines.next_line().await.unwrap().unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn server_100_clients_read_your_writes() {
    for kind in KINDS {
        let addr = start_server(kind).await;
        let tasks: Vec<_> = (0..100)
            .map(|c| {
                tokio::spawn(async move {
                    let mut cl = Client::connect(addr).await;
                    for i in 0..200 {
                        let key = format!("c{c}:{}", i % 10);
                        assert_eq!(cl.send(&format!("SET {key} v{i}")).await, "OK");
                        assert_eq!(cl.send(&format!("GET {key}")).await, format!("v{i}"));
                    }
                })
            })
            .collect();
        tokio::time::timeout(TIMEOUT, async {
            for t in tasks {
                t.await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{kind:?}: deadlock or stall"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn server_contended_key_never_returns_torn_value() {
    const LEN: usize = 512;
    for kind in KINDS {
        let addr = start_server(kind).await;
        Client::connect(addr)
            .await
            .send(&format!("SET hot {}", "a".repeat(LEN)))
            .await;

        let mut tasks = Vec::new();
        for c in 0..20u8 {
            tasks.push(tokio::spawn(async move {
                let mut cl = Client::connect(addr).await;
                let mine = ((b'a' + c % 26) as char).to_string().repeat(LEN);
                for _ in 0..300 {
                    if c % 2 == 0 {
                        assert_eq!(cl.send(&format!("SET hot {mine}")).await, "OK");
                    } else {
                        assert_homogeneous(cl.send("GET hot").await.as_bytes(), LEN);
                    }
                }
            }));
        }
        tokio::time::timeout(TIMEOUT, async {
            for t in tasks {
                t.await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{kind:?}: deadlock or stall"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abrupt_disconnects_do_not_affect_other_clients() {
    for kind in KINDS {
        let addr = start_server(kind).await;
        let mut survivor = Client::connect(addr).await;
        assert_eq!(survivor.send("SET k before").await, "OK");

        // Clients that send half a line, or garbage, then vanish.
        for i in 0..50 {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let junk: &[u8] = match i % 3 {
                0 => b"SET partial-line-no-newline",
                1 => b"\xff\xfe not utf8 \n",
                _ => b"",
            };
            let _ = s.write_all(junk).await;
            drop(s);
        }

        assert_eq!(survivor.send("GET k").await, "before");
        assert_eq!(survivor.send("PING").await, "PONG");
    }
}
