use bytes::Bytes;
use thiserror::Error;

/// A single request from a client, parsed from one line of text.
///
/// Wire format (one command per line, nc-friendly):
///   PING
///   GET <key>
///   SET <key> <value...>   (value is the rest of the line, spaces allowed)
///   DEL <key>
///   DBSIZE | DIGEST | ROLE
///   REPLICAOF <host> <port> | REPLICAOF NO ONE
///   SYNC <replid|?> <offset|-1>   (sent by a replica; the connection then becomes a stream)
///   ACK <offset>                  (sent by a replica on that stream)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Get {
        key: String,
    },
    Set {
        key: String,
        value: Bytes,
    },
    Del {
        key: String,
    },
    /// Number of keys.
    DbSize,
    /// Order-independent hash of the whole data set: equal digests, equal data.
    Digest,
    /// Leader or replica, offsets, connected replicas.
    Role,
    /// `Some("host:port")`: become a replica of it. `None` (`REPLICAOF NO ONE`): become a leader.
    ReplicaOf {
        leader: Option<String>,
    },
    /// A replica asking for the stream. `None`/`None` (`SYNC ? -1`) means "I have nothing".
    Sync {
        replid: Option<String>,
        offset: Option<u64>,
    },
    /// A replica reporting how far it has applied the stream.
    Ack {
        offset: u64,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("empty command")]
    Empty,
    #[error("unknown command '{0}'")]
    Unknown(String),
    #[error("wrong number of arguments for '{0}'")]
    WrongArgs(&'static str),
    #[error("invalid argument for '{0}'")]
    BadArg(&'static str),
}

impl Command {
    pub fn parse(line: &str) -> Result<Command, ParseError> {
        let line = line.trim();
        let (name, rest) = match line.split_once(char::is_whitespace) {
            Some((name, rest)) => (name, rest.trim_start()),
            None => (line, ""),
        };
        if name.is_empty() {
            return Err(ParseError::Empty);
        }

        match name.to_ascii_uppercase().as_str() {
            "PING" => {
                if !rest.is_empty() {
                    return Err(ParseError::WrongArgs("PING"));
                }
                Ok(Command::Ping)
            }
            "GET" => Ok(Command::Get {
                key: single_key(rest, "GET")?,
            }),
            "DEL" => Ok(Command::Del {
                key: single_key(rest, "DEL")?,
            }),
            "SET" => {
                let (key, value) = rest
                    .split_once(char::is_whitespace)
                    .ok_or(ParseError::WrongArgs("SET"))?;
                Ok(Command::Set {
                    key: key.to_string(),
                    value: Bytes::copy_from_slice(value.trim_start().as_bytes()),
                })
            }
            "DBSIZE" => no_args(rest, "DBSIZE", Command::DbSize),
            "DIGEST" => no_args(rest, "DIGEST", Command::Digest),
            "ROLE" => no_args(rest, "ROLE", Command::Role),
            "REPLICAOF" => match tokens::<2>(rest, "REPLICAOF")? {
                [no, one] if no.eq_ignore_ascii_case("NO") && one.eq_ignore_ascii_case("ONE") => {
                    Ok(Command::ReplicaOf { leader: None })
                }
                [host, port] => {
                    port.parse::<u16>()
                        .map_err(|_| ParseError::BadArg("REPLICAOF"))?;
                    Ok(Command::ReplicaOf {
                        leader: Some(format!("{host}:{port}")),
                    })
                }
            },
            "SYNC" => {
                let [replid, offset] = tokens::<2>(rest, "SYNC")?;
                let replid = (replid != "?").then(|| replid.to_string());
                let offset = match offset {
                    "-1" => None,
                    n => Some(n.parse().map_err(|_| ParseError::BadArg("SYNC"))?),
                };
                Ok(Command::Sync { replid, offset })
            }
            "ACK" => {
                let [offset] = tokens::<1>(rest, "ACK")?;
                Ok(Command::Ack {
                    offset: offset.parse().map_err(|_| ParseError::BadArg("ACK"))?,
                })
            }
            _ => Err(ParseError::Unknown(name.to_string())),
        }
    }
}

fn no_args(rest: &str, cmd: &'static str, command: Command) -> Result<Command, ParseError> {
    if rest.is_empty() {
        Ok(command)
    } else {
        Err(ParseError::WrongArgs(cmd))
    }
}

/// Expects exactly `N` whitespace-separated tokens.
fn tokens<'a, const N: usize>(
    rest: &'a str,
    cmd: &'static str,
) -> Result<[&'a str; N], ParseError> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    parts.try_into().map_err(|_| ParseError::WrongArgs(cmd))
}

/// Expects exactly one whitespace-free token.
fn single_key(rest: &str, cmd: &'static str) -> Result<String, ParseError> {
    let mut parts = rest.split_whitespace();
    match (parts.next(), parts.next()) {
        (Some(key), None) => Ok(key.to_string()),
        _ => Err(ParseError::WrongArgs(cmd)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping() {
        assert_eq!(Command::parse("PING"), Ok(Command::Ping));
        assert_eq!(Command::parse("ping\r\n"), Ok(Command::Ping));
    }

    #[test]
    fn get() {
        assert_eq!(
            Command::parse("GET foo"),
            Ok(Command::Get { key: "foo".into() })
        );
    }

    #[test]
    fn set_value_keeps_spaces() {
        assert_eq!(
            Command::parse("SET greeting hello  world"),
            Ok(Command::Set {
                key: "greeting".into(),
                value: Bytes::from_static(b"hello  world"),
            })
        );
    }

    #[test]
    fn del_is_case_insensitive() {
        assert_eq!(
            Command::parse("dEl foo"),
            Ok(Command::Del { key: "foo".into() })
        );
    }

    #[test]
    fn extra_whitespace_between_tokens() {
        assert_eq!(
            Command::parse("  SET   k   v  "),
            Ok(Command::Set {
                key: "k".into(),
                value: Bytes::from_static(b"v")
            })
        );
    }

    #[test]
    fn errors() {
        assert_eq!(Command::parse(""), Err(ParseError::Empty));
        assert_eq!(Command::parse("   \r\n"), Err(ParseError::Empty));
        assert_eq!(
            Command::parse("FLY away"),
            Err(ParseError::Unknown("FLY".into()))
        );
        assert_eq!(Command::parse("GET"), Err(ParseError::WrongArgs("GET")));
        assert_eq!(Command::parse("GET a b"), Err(ParseError::WrongArgs("GET")));
        assert_eq!(Command::parse("SET k"), Err(ParseError::WrongArgs("SET")));
        assert_eq!(Command::parse("DEL"), Err(ParseError::WrongArgs("DEL")));
        assert_eq!(Command::parse("PING x"), Err(ParseError::WrongArgs("PING")));
    }

    #[test]
    fn no_arg_commands() {
        assert_eq!(Command::parse("dbsize"), Ok(Command::DbSize));
        assert_eq!(Command::parse("DIGEST"), Ok(Command::Digest));
        assert_eq!(Command::parse("Role"), Ok(Command::Role));
        assert_eq!(Command::parse("ROLE x"), Err(ParseError::WrongArgs("ROLE")));
    }

    #[test]
    fn replicaof() {
        assert_eq!(
            Command::parse("REPLICAOF 127.0.0.1 6380"),
            Ok(Command::ReplicaOf {
                leader: Some("127.0.0.1:6380".into())
            })
        );
        assert_eq!(
            Command::parse("replicaof no one"),
            Ok(Command::ReplicaOf { leader: None })
        );
        assert_eq!(
            Command::parse("REPLICAOF host notaport"),
            Err(ParseError::BadArg("REPLICAOF"))
        );
        assert_eq!(
            Command::parse("REPLICAOF host"),
            Err(ParseError::WrongArgs("REPLICAOF"))
        );
    }

    #[test]
    fn sync_and_ack() {
        assert_eq!(
            Command::parse("SYNC ? -1"),
            Ok(Command::Sync {
                replid: None,
                offset: None
            })
        );
        assert_eq!(
            Command::parse("SYNC ab12 4096"),
            Ok(Command::Sync {
                replid: Some("ab12".into()),
                offset: Some(4096)
            })
        );
        assert_eq!(
            Command::parse("SYNC ab12 x"),
            Err(ParseError::BadArg("SYNC"))
        );
        assert_eq!(Command::parse("ACK 42"), Ok(Command::Ack { offset: 42 }));
        assert_eq!(Command::parse("ACK -3"), Err(ParseError::BadArg("ACK")));
        assert_eq!(Command::parse("ACK"), Err(ParseError::WrongArgs("ACK")));
    }
}
