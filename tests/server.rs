use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use rkv::db::Db;

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rkv::server::run(listener, Db::default()));
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

#[tokio::test]
async fn basic_commands() {
    let mut c = Client::connect(start_server().await).await;
    assert_eq!(c.send("PING").await, "PONG");
    assert_eq!(c.send("GET k").await, "(nil)");
    assert_eq!(c.send("SET k hello world").await, "OK");
    assert_eq!(c.send("GET k").await, "hello world");
    assert_eq!(c.send("DEL k").await, "1");
    assert_eq!(c.send("DEL k").await, "0");
    assert_eq!(c.send("NOPE").await, "ERR unknown command 'NOPE'");
}

#[tokio::test]
async fn clients_share_state() {
    let addr = start_server().await;
    let mut a = Client::connect(addr).await;
    let mut b = Client::connect(addr).await;
    assert_eq!(a.send("SET shared 42").await, "OK");
    assert_eq!(b.send("GET shared").await, "42");
}

#[tokio::test]
async fn many_concurrent_clients() {
    let addr = start_server().await;
    let tasks: Vec<_> = (0..20)
        .map(|i| {
            tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                for j in 0..50 {
                    let key = format!("c{i}-k{j}");
                    assert_eq!(c.send(&format!("SET {key} v{j}")).await, "OK");
                    assert_eq!(c.send(&format!("GET {key}")).await, format!("v{j}"));
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
}
