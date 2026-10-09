use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::command::Command;
use crate::db::Db;
use crate::repl::{Node, leader};

/// Accept connections forever, one task per client. A standalone leader.
pub async fn run(listener: TcpListener, db: Db) -> anyhow::Result<()> {
    serve(listener, Node::new(db)).await
}

/// Like `run`, for a node the caller set up (a replica, a custom backlog size...).
pub async fn serve(listener: TcpListener, node: Arc<Node>) -> anyhow::Result<()> {
    info!(addr = %listener.local_addr()?, "listening");
    loop {
        let (socket, peer) = listener.accept().await?;
        let node = node.clone();
        tokio::spawn(async move {
            debug!(%peer, "client connected");
            if let Err(e) = handle_client(socket, peer, node).await {
                warn!(%peer, error = %e, "connection error");
            }
            debug!(%peer, "client disconnected");
        });
    }
}

async fn handle_client(socket: TcpStream, peer: SocketAddr, node: Arc<Node>) -> anyhow::Result<()> {
    // Small request/response messages: don't let Nagle hold replies back.
    socket.set_nodelay(true)?;
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(Command::Sync { replid, offset }) = Command::parse(&line) {
            // From here on this connection is a replication stream.
            return leader::serve(node, lines, writer, peer, replid, offset).await;
        }
        let mut reply = execute(&line, &node.db);
        reply.push(b'\n');
        writer.write_all(&reply).await?;
    }
    Ok(())
}

fn execute(line: &str, db: &Db) -> Vec<u8> {
    match Command::parse(line) {
        Ok(Command::Ping) => b"PONG".to_vec(),
        Ok(Command::Get { key }) => match db.get(&key) {
            Some(value) => value.to_vec(),
            None => b"(nil)".to_vec(),
        },
        Ok(Command::Set { key, value }) => match db.set(key, value) {
            Ok(()) => b"OK".to_vec(),
            Err(e) => storage_error(e),
        },
        Ok(Command::Del { key }) => match db.del(&key) {
            Ok(true) => b"1".to_vec(),
            Ok(false) => b"0".to_vec(),
            Err(e) => storage_error(e),
        },
        Ok(Command::DbSize) => db.len().to_string().into_bytes(),
        Ok(Command::Digest) => format!("{:016x}", db.digest()).into_bytes(),
        Ok(_) => b"ERR not implemented yet".to_vec(),
        Err(e) => format!("ERR {e}").into_bytes(),
    }
}

/// The write was not logged, so it was not applied either: tell the client.
fn storage_error(e: std::io::Error) -> Vec<u8> {
    error!(error = %e, "WAL write failed");
    format!("ERR storage: {e}").into_bytes()
}
