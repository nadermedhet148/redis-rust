use bytes::Bytes;
use thiserror::Error;

/// A single request from a client, parsed from one line of text.
///
/// Wire format (one command per line, nc-friendly):
///   PING
///   GET <key>
///   SET <key> <value...>   (value is the rest of the line, spaces allowed)
///   DEL <key>
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Get { key: String },
    Set { key: String, value: Bytes },
    Del { key: String },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("empty command")]
    Empty,
    #[error("unknown command '{0}'")]
    Unknown(String),
    #[error("wrong number of arguments for '{0}'")]
    WrongArgs(&'static str),
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
            _ => Err(ParseError::Unknown(name.to_string())),
        }
    }
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
}
