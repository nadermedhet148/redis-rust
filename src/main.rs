use clap::Parser;
use tokio::net::TcpListener;

use rkv::db::Db;

#[derive(Parser, Debug)]
#[command(name = "rkv", about = "A tiny Rust key-value store")]
struct Args {
    /// Address to listen on
    #[arg(long, default_value = "127.0.0.1:6380")]
    addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let listener = TcpListener::bind(&args.addr).await?;
    rkv::server::run(listener, Db::new()).await
}
