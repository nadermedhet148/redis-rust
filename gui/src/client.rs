//! Background connection to an rkv server.
//!
//! The UI thread never touches the socket: it sends `Request`s and drains
//! `Event`s once per frame, so a slow or dead server can't freeze the window.
//! The worker is strictly request/response over one connection, exactly like
//! `nc` would be.

use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use eframe::egui;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// fsync=always on a slow disk can take a while, but not this long.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

pub enum Request {
    Connect(String),
    Disconnect,
    /// One protocol line, without the trailing newline. The reply carries the same id.
    Send {
        id: u64,
        line: String,
    },
    /// `n` SETs then `n` GETs on `bench:<i>` keys, timed separately.
    Bench {
        n: usize,
        value_len: usize,
    },
}

pub enum Event {
    Connected(String),
    Disconnected {
        reason: Option<String>,
    },
    Reply {
        id: u64,
        reply: String,
        elapsed: Duration,
    },
    /// The request never got a reply. Afterwards there is no connection.
    Failed {
        id: u64,
        error: String,
    },
    BenchDone {
        n: usize,
        set: Duration,
        get: Duration,
    },
}

pub struct Client {
    tx: Sender<Request>,
    rx: Receiver<Event>,
}

impl Client {
    /// `ctx` is poked after every event so the UI repaints even when idle.
    pub fn spawn(ctx: egui::Context) -> Self {
        let (req_tx, req_rx) = mpsc::channel();
        let (ev_tx, ev_rx) = mpsc::channel();
        thread::Builder::new()
            .name("rkv-client".into())
            .spawn(move || worker(req_rx, ev_tx, ctx))
            .expect("spawn client thread");
        Self {
            tx: req_tx,
            rx: ev_rx,
        }
    }

    pub fn request(&self, req: Request) {
        // Only fails if the worker panicked; nothing useful to do then.
        let _ = self.tx.send(req);
    }

    pub fn poll(&self) -> impl Iterator<Item = Event> + '_ {
        self.rx.try_iter()
    }
}

struct Conn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Conn {
    fn open(addr: &str) -> io::Result<Conn> {
        let mut last_err =
            io::Error::new(io::ErrorKind::InvalidInput, "address resolved to nothing");
        for sa in addr.to_socket_addrs()? {
            match TcpStream::connect_timeout(&sa, CONNECT_TIMEOUT) {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    stream.set_read_timeout(Some(READ_TIMEOUT))?;
                    return Ok(Conn {
                        reader: BufReader::new(stream.try_clone()?),
                        writer: stream,
                    });
                }
                Err(e) => last_err = e,
            }
        }
        Err(last_err)
    }

    fn call(&mut self, line: &str) -> io::Result<String> {
        self.writer.write_all(format!("{line}\n").as_bytes())?;
        // Values are arbitrary bytes, so don't insist on UTF-8 like read_line would.
        let mut buf = Vec::new();
        if self.reader.read_until(b'\n', &mut buf)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "server closed the connection",
            ));
        }
        while matches!(buf.last(), Some(b'\n' | b'\r')) {
            buf.pop();
        }
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    fn bench(&mut self, n: usize, value_len: usize) -> io::Result<(Duration, Duration)> {
        let value = "x".repeat(value_len.max(1));
        let start = Instant::now();
        for i in 0..n {
            self.call(&format!("SET bench:{i} {value}"))?;
        }
        let set = start.elapsed();
        let start = Instant::now();
        for i in 0..n {
            self.call(&format!("GET bench:{i}"))?;
        }
        Ok((set, start.elapsed()))
    }
}

fn worker(rx: Receiver<Request>, tx: Sender<Event>, ctx: egui::Context) {
    let mut conn: Option<Conn> = None;
    // Ends when the UI drops its Client.
    for req in rx {
        let event = match req {
            Request::Connect(addr) => match Conn::open(&addr) {
                Ok(c) => {
                    conn = Some(c);
                    Event::Connected(addr)
                }
                Err(e) => {
                    conn = None;
                    Event::Disconnected {
                        reason: Some(format!("connect {addr}: {e}")),
                    }
                }
            },
            Request::Disconnect => {
                conn = None;
                Event::Disconnected { reason: None }
            }
            Request::Send { id, line } => match conn.as_mut() {
                None => Event::Failed {
                    id,
                    error: "not connected".into(),
                },
                Some(c) => {
                    let start = Instant::now();
                    match c.call(&line) {
                        Ok(reply) => Event::Reply {
                            id,
                            reply,
                            elapsed: start.elapsed(),
                        },
                        Err(e) => {
                            conn = None;
                            Event::Failed {
                                id,
                                error: e.to_string(),
                            }
                        }
                    }
                }
            },
            Request::Bench { n, value_len } => match conn.as_mut() {
                None => Event::Disconnected {
                    reason: Some("bench: not connected".into()),
                },
                Some(c) => match c.bench(n, value_len) {
                    Ok((set, get)) => Event::BenchDone { n, set, get },
                    Err(e) => {
                        conn = None;
                        Event::Disconnected {
                            reason: Some(format!("bench: {e}")),
                        }
                    }
                },
            },
        };
        if tx.send(event).is_err() {
            break;
        }
        ctx.request_repaint();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    /// Tiny stand-in server: answers each line from a script, then hangs up.
    fn fake_server(replies: &'static [&'static [u8]]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut writer = stream;
            for reply in replies {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                writer.write_all(reply).unwrap();
            }
        });
        addr
    }

    fn next(client: &Client) -> Event {
        client
            .rx
            .recv_timeout(Duration::from_secs(5))
            .expect("event")
    }

    #[test]
    fn request_reply_then_server_hangup() {
        let addr = fake_server(&[b"PONG\r\n", b"caf\xc3\xa9 \xff\n"]);
        let client = Client::spawn(egui::Context::default());

        client.request(Request::Connect(addr));
        assert!(matches!(next(&client), Event::Connected(_)));

        client.request(Request::Send {
            id: 1,
            line: "PING".into(),
        });
        assert!(matches!(next(&client), Event::Reply { id: 1, reply, .. } if reply == "PONG"));

        // Non-UTF-8 bytes in a value must not kill the connection.
        client.request(Request::Send {
            id: 2,
            line: "GET k".into(),
        });
        assert!(
            matches!(next(&client), Event::Reply { id: 2, reply, .. } if reply == "café \u{FFFD}")
        );

        client.request(Request::Send {
            id: 3,
            line: "GET k".into(),
        });
        assert!(matches!(next(&client), Event::Failed { id: 3, .. }));

        client.request(Request::Send {
            id: 4,
            line: "PING".into(),
        });
        assert!(
            matches!(next(&client), Event::Failed { id: 4, error } if error == "not connected")
        );
    }

    #[test]
    fn connect_refused_reports_reason() {
        // Bind then drop to get a port nobody is listening on.
        let addr = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string();
        let client = Client::spawn(egui::Context::default());
        client.request(Request::Connect(addr));
        assert!(matches!(
            next(&client),
            Event::Disconnected { reason: Some(_) }
        ));
    }
}
