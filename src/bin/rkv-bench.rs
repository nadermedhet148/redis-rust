//! Load generator: N concurrent clients, each doing M request/response ops.
//!
//!   cargo run --release --bin rkv-bench -- --addr 127.0.0.1:6380 -c 50 -n 20000

use std::time::{Duration, Instant};

use clap::Parser;
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
    preload(&args).await?;
    let report = run(&args).await?;

    println!(
        "{} clients x {} ops, {:.0}% reads, {} keys, {}B values",
        args.clients,
        args.ops,
        args.read_ratio * 100.0,
        args.keys,
        args.value_size
    );
    println!("| ops/sec | p50 (us) | p99 (us) |");
    println!("|--------:|---------:|---------:|");
    println!(
        "| {:>7.0} | {:>8} | {:>8} |",
        report.ops_per_sec(),
        report.percentile(0.50),
        report.percentile(0.99)
    );
    Ok(())
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
