//! Memory tests, measured with a counting global allocator: exact live heap
//! bytes, no OS/RSS noise. Run with `--nocapture` to see the numbers.
//!
//! Two counters:
//! - `local()`: bytes allocated minus freed *by the current thread*. Single-threaded
//!   `Db` tests use it, so the test harness spawning other test threads can't add noise.
//! - `live()`: the whole process. The server test needs it (tokio worker threads),
//!   takes `SERIAL`, and allows a small tolerance.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use rkv::db::{Db, StoreKind};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

thread_local! {
    // const-initialized: accessing it never allocates, so it's safe inside the allocator.
    static LOCAL: Cell<isize> = const { Cell::new(0) };
}

fn track(delta: isize) {
    LIVE.fetch_add(delta, Ordering::Relaxed);
    // try_with: the TLS slot may already be gone while a thread is exiting.
    let _ = LOCAL.try_with(|c| c.set(c.get() + delta));
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            track(layout.size() as isize);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        track(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            track(new_size as isize - layout.size() as isize);
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// tokio's Mutex: held across .await in async tests, `blocking_lock()` in sync ones.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn live() -> isize {
    LIVE.load(Ordering::SeqCst)
}

fn local() -> isize {
    LOCAL.with(|c| c.get())
}

const KINDS: [StoreKind; 3] = [StoreKind::Mutex, StoreKind::Rwlock, StoreKind::Sharded];
const N: usize = 100_000;
const VALUE_LEN: usize = 100;

fn key(i: usize) -> String {
    format!("key:{i}")
}

fn fill(db: &Db) -> usize {
    let mut payload = 0;
    for i in 0..N {
        let k = key(i);
        payload += k.len() + VALUE_LEN;
        db.set(k, Bytes::from(vec![b'x'; VALUE_LEN])).unwrap();
    }
    payload
}

/// Bytes the store spends per key beyond the key and value themselves
/// (hash table slots, empty capacity, per-shard maps).
const MAX_OVERHEAD_PER_KEY: isize = 128;

#[test]
fn per_key_overhead_is_bounded() {
    let _g = SERIAL.blocking_lock();
    for kind in KINDS {
        let base = local();
        let db = Db::new(kind);
        let payload = fill(&db) as isize;
        let used = local() - base;
        let overhead = (used - payload) / N as isize;
        println!(
            "{kind:?}: {N} keys, payload {:.1} MB, heap {:.1} MB, overhead {overhead} B/key",
            payload as f64 / 1e6,
            used as f64 / 1e6
        );
        assert!(
            overhead <= MAX_OVERHEAD_PER_KEY,
            "{kind:?}: {overhead} B/key overhead > {MAX_OVERHEAD_PER_KEY}"
        );
        drop(db);
    }
}

#[test]
fn dropping_the_store_frees_everything() {
    let _g = SERIAL.blocking_lock();
    for kind in KINDS {
        let base = local();
        let db = Db::new(kind);
        fill(&db);
        let clone = db.clone();
        drop(db);
        assert!(local() - base > 0, "a live clone must keep the data alive");
        drop(clone);
        let leaked = local() - base;
        println!("{kind:?}: leaked after drop: {leaked} B");
        assert_eq!(leaked, 0, "{kind:?}: {leaked} bytes leaked");
    }
}

#[test]
fn del_frees_keys_and_values() {
    let _g = SERIAL.blocking_lock();
    for kind in KINDS {
        let base = local();
        let db = Db::new(kind);
        let payload = fill(&db) as isize;
        let full = local();
        for i in 0..N {
            assert!(db.del(&key(i)).unwrap());
        }
        let freed = full - local();
        let retained = local() - base;
        println!(
            "{kind:?}: DEL freed {:.1} MB, table capacity retained {:.1} MB",
            freed as f64 / 1e6,
            retained as f64 / 1e6
        );
        // Every key and value must be freed. HashMap keeps its bucket array
        // (it never shrinks on remove); that is expected and reported above.
        assert!(
            freed >= payload,
            "{kind:?}: freed {freed} < payload {payload}"
        );
    }
}

#[test]
fn overwriting_does_not_grow_memory() {
    let _g = SERIAL.blocking_lock();
    for kind in KINDS {
        let db = Db::new(kind);
        let write_all = || {
            for i in 0..1_000 {
                db.set(key(i), Bytes::from(vec![b'y'; VALUE_LEN])).unwrap();
            }
        };
        write_all();
        // One overwrite round before the baseline: hashbrown's insert() calls
        // reserve(1) *before* checking whether the key exists, so a map that is
        // exactly full grows once even on an overwrite. With 64 shards some
        // usually are. That one-time resize is not a leak; continued growth is.
        write_all();
        let baseline = local();
        for _ in 0..100 {
            write_all();
        }
        let growth = local() - baseline;
        println!("{kind:?}: growth after 100 overwrite rounds: {growth} B");
        assert_eq!(growth, 0, "{kind:?}: overwrites leak {growth} bytes");
    }
}

// ---------- Per-connection memory through the real server ----------

const CONNS: usize = 500;
/// BufReader (8 KiB) + task + socket registration, with headroom.
const MAX_BYTES_PER_CONN: isize = 32 * 1024;

async fn open_conns(addr: std::net::SocketAddr, n: usize) -> Vec<TcpStream> {
    let mut conns = Vec::with_capacity(n);
    for _ in 0..n {
        let mut s = TcpStream::connect(addr).await.unwrap();
        // Round-trip a PING so the server-side task is definitely running.
        s.write_all(b"PING\n").await.unwrap();
        let mut line = String::new();
        BufReader::new(&mut s).read_line(&mut line).await.unwrap();
        assert_eq!(line, "PONG\n");
        conns.push(s);
    }
    conns
}

/// Wait until live bytes settle at or below `target` (tasks exit asynchronously).
async fn settle_below(target: isize) -> isize {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let now = live();
        if now <= target || Instant::now() > deadline {
            return now;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connections_release_their_memory() {
    let _g = SERIAL.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rkv::server::run(listener, Db::default()));

    // Warm-up round so one-time runtime allocations are not counted as leaks.
    drop(open_conns(addr, CONNS).await);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let base = live();

    let conns = open_conns(addr, CONNS).await;
    let per_conn = (live() - base) / CONNS as isize;
    println!("{CONNS} open connections: {per_conn} B/connection (client + server side)");
    assert!(
        per_conn <= MAX_BYTES_PER_CONN,
        "{per_conn} B per connection > {MAX_BYTES_PER_CONN}"
    );

    drop(conns);
    let slack = 64 * 1024;
    let residue = settle_below(base + slack).await - base;
    println!("after closing all connections: {residue} B above baseline");
    assert!(
        residue <= slack,
        "{residue} bytes still held after {CONNS} connections closed"
    );
}
