use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

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
        Ok(Command::Set { key, value }) => {
            db.set(key, value);
            b"OK".to_vec()
        }
        Ok(Command::Del { key }) => {
            if db.del(&key) {
                b"1".to_vec()
            } else {
                b"0".to_vec()
            }
        }
        Err(e) => format!("ERR {e}").into_bytes(),
    }
}
