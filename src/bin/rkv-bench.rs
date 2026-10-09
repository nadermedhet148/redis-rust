//! Load generator: N concurrent clients, each doing M request/response ops.
//!
//!   cargo run --release --bin rkv-bench -- --addr 127.0.0.1:6380 -c 50 -n 20000
//!
//! `--compare` starts its own `rkv` server process per configuration and prints
//! one table, including that server process's CPU time and memory:
//!
//!   cargo build --release
//!   target/release/rkv-bench --compare stores   # mutex vs rwlock vs sharded (ep02)
//!   target/release/rkv-bench --compare fsync    # no WAL vs fsync never/every-sec/always (ep03)
//!   target/release/rkv-bench --compare replicas # leader with 0 / 1 / 2 replicas (ep04)

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Parser, Debug, Clone)]
#[command(name = "rkv-bench", about = "Benchmark an rkv server")]
struct Args {
    /// Server to benchmark
    #[arg(long, default_value = "127.0.0.1:6380")]
    addr: String,
    /// Number of concurrent clients (N)
    #[arg(short = 'c', long, default_value_t = 50)]
    clients: usize,
    /// Operations per client (M)
    #[arg(short = 'n', long, default_value_t = 20_000)]
    ops: usize,
    /// Fraction of operations that are GETs (the rest are SETs)
    #[arg(long, default_value_t = 0.8)]
    read_ratio: f64,
    /// Size of the key space
    #[arg(long, default_value_t = 10_000)]
    keys: usize,
    /// Value size in bytes
    #[arg(long, default_value_t = 64)]
    value_size: usize,
    /// Spawn an rkv server per configuration and compare them in one table
    #[arg(long, value_enum)]
    compare: Option<Compare>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Compare {
    /// --store mutex | rwlock | sharded, in memory
    Stores,
    /// --store sharded with no WAL, then WAL with each --fsync policy
    Fsync,
    /// a sharded leader with 0, 1 and 2 replicas: throughput and catch-up time
    Replicas,
}

/// One row of a comparison: a label and the extra args for `rkv`.
struct Config {
    label: &'static str,
    server_args: Vec<String>,
    wal: Option<std::path::PathBuf>,
}

impl Config {
    fn new(label: &'static str, store: &str, fsync: Option<&str>) -> Self {
        let mut server_args = vec!["--store".to_string(), store.to_string()];
        let wal = fsync.map(|policy| {
            let path =
                std::env::temp_dir().join(format!("rkv-bench-{}-{label}.wal", std::process::id()));
            let _ = std::fs::remove_file(&path);
            server_args.extend(["--fsync".into(), policy.into(), "--wal".into()]);
            server_args.push(path.display().to_string());
            path
        });
        Self {
            label,
            server_args,
            wal,
        }
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        if let Some(wal) = &self.wal {
            let _ = std::fs::remove_file(wal);
        }
    }
}

struct Report {
    total_ops: usize,
    elapsed: Duration,
    latencies_us: Vec<u32>,
}

impl Report {
    fn ops_per_sec(&self) -> f64 {
        self.total_ops as f64 / self.elapsed.as_secs_f64()
    }

    fn percentile(&self, p: f64) -> u32 {
        let i = ((self.latencies_us.len() - 1) as f64 * p).round() as usize;
        self.latencies_us[i]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!(
        "{} clients x {} ops, {:.0}% reads, {} keys, {}B values",
        args.clients,
        args.ops,
        args.read_ratio * 100.0,
        args.keys,
        args.value_size
    );

    let Some(compare) = args.compare else {
        preload(&args).await?;
        let report = run(&args).await?;
        println!("| ops/sec | p50 (us) | p99 (us) |");
        println!("|--------:|---------:|---------:|");
        println!(
            "| {:>7.0} | {:>8} | {:>8} |",
            report.ops_per_sec(),
            report.percentile(0.50),
            report.percentile(0.99)
        );
        return Ok(());
    };

    if let Compare::Replicas = compare {
        return compare_replicas(&args).await;
    }
    let configs = match compare {
        Compare::Stores => vec![
            Config::new("mutex", "mutex", None),
            Config::new("rwlock", "rwlock", None),
            Config::new("sharded", "sharded", None),
        ],
        Compare::Fsync => vec![
            Config::new("no WAL", "sharded", None),
            Config::new("never", "sharded", Some("never")),
            Config::new("every-sec", "sharded", Some("every-sec")),
            Config::new("always", "sharded", Some("always")),
        ],
        Compare::Replicas => unreachable!(),
    };

    println!(
        "| config    | ops/sec | p50 (us) | p99 (us) | server CPU (cores) | CPU us/op | RSS idle (MB) | RSS peak (MB) |"
    );
    println!(
        "|-----------|--------:|---------:|---------:|-------------------:|----------:|--------------:|--------------:|"
    );
    for (i, config) in configs.iter().enumerate() {
        let addr = format!("127.0.0.1:{}", 16380 + i);
        let server = Server::spawn(&addr, &config.server_args).await?;
        let args = Args {
            addr,
            ..args.clone()
        };

        let idle_rss = server.rss();
        preload(&args).await?;
        let sampler = server.sample_peak_rss();
        let cpu_before = server.cpu_ms();
        let report = run(&args).await?;
        let cpu_ms = server.cpu_ms() - cpu_before;
        let peak_rss = sampler.stop();

        println!(
            "| {:<9} | {:>7.0} | {:>8} | {:>8} | {:>18.2} | {:>9.1} | {:>13.1} | {:>13.1} |",
            config.label,
            report.ops_per_sec(),
            report.percentile(0.50),
            report.percentile(0.99),
            cpu_ms as f64 / report.elapsed.as_millis() as f64,
            cpu_ms as f64 * 1000.0 / report.total_ops as f64,
            mb(idle_rss),
            mb(peak_rss),
        );
    }
    Ok(())
}

/// Load the leader, then measure how long the replicas take to have exactly
/// the leader's data (same DIGEST) once the load stops. Replication is
/// asynchronous, so clients never wait for replicas: the cost shows up as
/// leader CPU (one stream per replica) and as catch-up time.
async fn compare_replicas(args: &Args) -> anyhow::Result<()> {
    println!(
        "| replicas | ops/sec | p50 (us) | p99 (us) | leader CPU (cores) | CPU us/op | catch-up (ms) |"
    );
    println!(
        "|---------:|--------:|---------:|---------:|-------------------:|----------:|--------------:|"
    );
    for (i, n) in [0usize, 1, 2].into_iter().enumerate() {
        let leader_addr = format!("127.0.0.1:{}", 16480 + i * 10);
        let store = ["--store".to_string(), "sharded".to_string()];
        let leader = Server::spawn(&leader_addr, &store).await?;
        let mut replicas = Vec::new();
        for r in 0..n {
            let addr = format!("127.0.0.1:{}", 16481 + i * 10 + r);
            let mut replica_args = store.to_vec();
            replica_args.extend(["--replica-of".to_string(), leader_addr.clone()]);
            replicas.push((addr.clone(), Server::spawn(&addr, &replica_args).await?));
        }
        let mut ctl = Conn::connect(&leader_addr).await?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ctl
            .request("ROLE")
            .await?
            .contains(&format!("replicas={n}"))
        {
            anyhow::ensure!(Instant::now() < deadline, "replicas did not connect");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let args = Args {
            addr: leader_addr.clone(),
            ..args.clone()
        };
        preload(&args).await?;
        let cpu_before = leader.cpu_ms();
        let report = run(&args).await?;
        let cpu_ms = leader.cpu_ms() - cpu_before;

        let caught_up = Instant::now();
        let digest = ctl.request("DIGEST").await?;
        for (addr, _) in &replicas {
            let mut conn = Conn::connect(addr).await?;
            while conn.request("DIGEST").await? != digest {
                anyhow::ensure!(
                    caught_up.elapsed() < Duration::from_secs(30),
                    "replica never caught up"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        let catch_up = if n == 0 {
            "-".to_string()
        } else {
            format!("{:.1}", caught_up.elapsed().as_secs_f64() * 1000.0)
        };

        println!(
            "| {:>8} | {:>7.0} | {:>8} | {:>8} | {:>18.2} | {:>9.1} | {:>13} |",
            n,
            report.ops_per_sec(),
            report.percentile(0.50),
            report.percentile(0.99),
            cpu_ms as f64 / report.elapsed.as_millis() as f64,
            cpu_ms as f64 * 1000.0 / report.total_ops as f64,
            catch_up,
        );
        drop(replicas);
        drop(leader);
    }
    Ok(())
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// An `rkv` server running as a child process, so its CPU/memory are measured
/// separately from the load generator's. Killed on drop.
struct Server {
    child: Child,
    pid: Pid,
}

impl Server {
    async fn spawn(addr: &str, server_args: &[String]) -> anyhow::Result<Self> {
        let exe =
            std::env::current_exe()?.with_file_name(format!("rkv{}", std::env::consts::EXE_SUFFIX));
        let child = Command::new(&exe)
            .args(["--addr", addr])
            .args(server_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| {
                anyhow::anyhow!(
                    "failed to start {}: {e} (run `cargo build --release` first)",
                    exe.display()
                )
            })?;
        let pid = Pid::from_u32(child.id());

        let deadline = Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(addr).await.is_err() {
            anyhow::ensure!(Instant::now() < deadline, "server on {addr} did not start");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(Self { child, pid })
    }

    fn refresh(&self) -> System {
        let mut sys = System::new();
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        sys
    }

    fn rss(&self) -> u64 {
        self.refresh().process(self.pid).map_or(0, |p| p.memory())
    }

    fn cpu_ms(&self) -> u64 {
        self.refresh()
            .process(self.pid)
            .map_or(0, |p| p.accumulated_cpu_time())
    }

    /// Poll RSS every 20ms in a background thread, keeping the maximum.
    fn sample_peak_rss(&self) -> PeakSampler {
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(0));
        let pid = self.pid;
        let handle = {
            let (stop, peak) = (stop.clone(), peak.clone());
            std::thread::spawn(move || {
                let mut sys = System::new();
                while !stop.load(Ordering::Relaxed) {
                    sys.refresh_processes_specifics(
                        ProcessesToUpdate::Some(&[pid]),
                        true,
                        ProcessRefreshKind::nothing().with_memory(),
                    );
                    if let Some(p) = sys.process(pid) {
                        peak.fetch_max(p.memory(), Ordering::Relaxed);
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            })
        };
        PeakSampler { stop, peak, handle }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct PeakSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    handle: std::thread::JoinHandle<()>,
}

impl PeakSampler {
    fn stop(self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.handle.join();
        self.peak.load(Ordering::Relaxed)
    }
}

/// Fill the key space so GETs hit real values.
async fn preload(args: &Args) -> anyhow::Result<()> {
    let mut conn = Conn::connect(&args.addr).await?;
    let value = "x".repeat(args.value_size);
    for k in 0..args.keys {
        conn.request(&format!("SET key:{k} {value}")).await?;
    }
    Ok(())
}

async fn run(args: &Args) -> anyhow::Result<Report> {
    let start = Instant::now();
    let tasks: Vec<_> = (0..args.clients)
        .map(|id| tokio::spawn(client(args.clone(), id as u64)))
        .collect();

    let mut latencies_us = Vec::with_capacity(args.clients * args.ops);
    for t in tasks {
        latencies_us.extend(t.await??);
    }
    let elapsed = start.elapsed();
    latencies_us.sort_unstable();

    Ok(Report {
        total_ops: latencies_us.len(),
        elapsed,
        latencies_us,
    })
}

async fn client(args: Args, id: u64) -> anyhow::Result<Vec<u32>> {
    let mut conn = Conn::connect(&args.addr).await?;
    let mut rng = Rng(id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let value = "v".repeat(args.value_size);
    let mut latencies = Vec::with_capacity(args.ops);

    for _ in 0..args.ops {
        let key = rng.next() % args.keys as u64;
        let line = if rng.unit() < args.read_ratio {
            format!("GET key:{key}")
        } else {
            format!("SET key:{key} {value}")
        };
        let t = Instant::now();
        conn.request(&line).await?;
        latencies.push(t.elapsed().as_micros() as u32);
    }
    Ok(latencies)
}

struct Conn {
    lines: tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl Conn {
    async fn connect(addr: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (r, w) = stream.into_split();
        Ok(Self {
            lines: BufReader::new(r).lines(),
            writer: w,
        })
    }

    async fn request(&mut self, line: &str) -> anyhow::Result<String> {
        let mut buf = Vec::with_capacity(line.len() + 1);
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
        self.writer.write_all(&buf).await?;
        self.lines
            .next_line()
            .await?
            .ok_or_else(|| anyhow::anyhow!("server closed connection"))
    }
}

/// xorshift64: tiny, deterministic, good enough for picking keys.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}
