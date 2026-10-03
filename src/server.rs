use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::command::Command;
use crate::db::Db;

/// Accept connections forever, one task per client.
pub async fn run(listener: TcpListener, db: Db) -> anyhow::Result<()> {
    info!(addr = %listener.local_addr()?, "listening");
    loop {
        let (socket, peer) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            debug!(%peer, "client connected");
            if let Err(e) = handle_client(socket, db).await {
                warn!(%peer, error = %e, "connection error");
            }
            debug!(%peer, "client disconnected");
        });
    }
}

async fn handle_client(socket: TcpStream, db: Db) -> anyhow::Result<()> {
    // Small request/response messages: don't let Nagle hold replies back.
    socket.set_nodelay(true)?;
    let (reader, mut writer) = socket.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let mut reply = execute(&line, &db);
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
        Err(e) => format!("ERR {e}").into_bytes(),
    }
}

/// The write was not logged, so it was not applied either: tell the client.
fn storage_error(e: std::io::Error) -> Vec<u8> {
    error!(error = %e, "WAL write failed");
    format!("ERR storage: {e}").into_bytes()
}
