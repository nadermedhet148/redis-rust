use std::path::PathBuf;

use clap::Parser;
use tokio::net::TcpListener;

use rkv::db::{Db, StoreKind};
use rkv::repl::{Node, backlog};
use rkv::wal::FsyncPolicy;

#[derive(Parser, Debug)]
#[command(name = "rkv", about = "A tiny Rust key-value store")]
struct Args {
    /// Address to listen on
    #[arg(long, default_value = "127.0.0.1:6380")]
    addr: String,
    /// Storage locking strategy
    #[arg(long, value_enum, default_value_t = StoreKind::Mutex)]
    store: StoreKind,
    /// Write-ahead log file. Without it, data lives only in memory.
    #[arg(long)]
    wal: Option<PathBuf>,
    /// When to fsync the WAL
    #[arg(long, value_enum, default_value_t = FsyncPolicy::EverySec)]
    fsync: FsyncPolicy,
    /// Start as a read-only replica of this leader (host:port)
    #[arg(long, value_name = "HOST:PORT")]
    replica_of: Option<String>,
    /// Replication backlog size in bytes (how far a replica can fall behind and
    /// still catch up without a full sync)
    #[arg(long, default_value_t = backlog::DEFAULT_CAPACITY)]
    repl_backlog: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    tracing::info!(
        store = ?args.store,
        wal = ?args.wal,
        fsync = ?args.fsync,
        replica_of = ?args.replica_of,
        "starting"
    );
    let db = match &args.wal {
        Some(path) => Db::open(args.store, path, args.fsync)?.0,
        None => Db::new(args.store),
    };
    let node = Node::with_backlog(db, args.repl_backlog);
    if let Some(leader) = args.replica_of {
        node.replicate_from(leader);
    }
    // Bind only after replay, so a client that can connect sees all recovered data.
    let listener = TcpListener::bind(&args.addr).await?;
    rkv::server::serve(listener, node).await
}
