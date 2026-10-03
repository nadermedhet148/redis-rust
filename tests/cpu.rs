//! CPU usage tests, measured as this process's CPU time (sysinfo).
//! Kept in their own test binary so no other test's work is counted.
//! The two tests also take `SERIAL` so they don't overlap each other.
//!
//! Thresholds are loose enough for a debug build; the real numbers are
//! printed (`--nocapture`) and recorded in docs/PERFORMANCE.md.

use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use rkv::db::Db;

/// tokio's Mutex: held across .await in async tests, `blocking_lock()` in sync ones.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn process_cpu_ms() -> u64 {
    let pid = sysinfo::get_current_pid().unwrap();
    let mut sys = sysinfo::System::new();
    sys.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        true,
        sysinfo::ProcessRefreshKind::nothing().with_cpu(),
    );
    sys.process(pid).unwrap().accumulated_cpu_time()
}

async fn start_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rkv::server::run(listener, Db::default()));
    addr
}

/// An idle server must sleep in the OS, not spin: no busy loops, no polling timers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_server_with_open_connections_uses_no_cpu() {
    let _g = SERIAL.lock().await;
    let addr = start_server().await;
    let mut conns = Vec::new();
    for _ in 0..200 {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"PING\n").await.unwrap();
        conns.push(s);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let window = Duration::from_secs(2);
    let before = process_cpu_ms();
    tokio::time::sleep(window).await;
    let used = process_cpu_ms() - before;
    let pct = used as f64 / window.as_millis() as f64 * 100.0;
    println!("idle, 200 open connections: {used} ms CPU in {window:?} ({pct:.1}% of one core)");
    // 5% of one core; OS timer granularity (~15.6 ms on Windows) makes 0 unrealistic.
    assert!(
        used <= 100,
        "idle server burned {used} ms CPU in {window:?}"
    );
}

/// Total CPU (client + server, same process) per request must stay bounded.
/// Catches regressions like accidental O(n) work or per-request allocation storms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpu_time_per_request_is_bounded() {
    let _g = SERIAL.lock().await;
    let addr = start_server().await;
    const CLIENTS: usize = 16;
    const OPS: usize = 2_000;

    let before = process_cpu_ms();
    let start = Instant::now();
    let tasks: Vec<_> = (0..CLIENTS)
        .map(|c| {
            tokio::spawn(async move {
                let (r, mut w) = TcpStream::connect(addr).await.unwrap().into_split();
                let mut lines = BufReader::new(r).lines();
                for i in 0..OPS {
                    let cmd = if i % 5 == 0 {
                        format!("SET k{c}:{} value-{i}\n", i % 100)
                    } else {
                        format!("GET k{c}:{}\n", i % 100)
                    };
                    w.write_all(cmd.as_bytes()).await.unwrap();
                    lines.next_line().await.unwrap().unwrap();
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    let elapsed = start.elapsed();
    let cpu_us_per_op = (process_cpu_ms() - before) as f64 * 1000.0 / (CLIENTS * OPS) as f64;
    println!(
        "{} requests in {elapsed:?}: {cpu_us_per_op:.1} us CPU per request (client+server)",
        CLIENTS * OPS
    );
    assert!(
        cpu_us_per_op < 500.0,
        "{cpu_us_per_op:.1} us CPU per request is too high"
    );
}
