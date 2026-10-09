//! What goes over a replication connection.
//!
//! ```text
//! replica → leader   SYNC <replid|?> <offset|-1>\n
//! leader  → replica  FULLSYNC <replid> <offset> <consistent_at> <keys>\n
//!                      then <keys> SET records (the snapshot), then the stream from <offset>
//!                or  CONTINUE <replid>\n
//!                      then the stream from the offset the replica asked for
//! leader  → replica  stream: WAL records, plus a heartbeat frame every second
//! replica → leader   ACK <offset>\n   every second
//! ```
//!
//! Records use the WAL format (`wal::encode_*`). A heartbeat is an 8-byte
//! header with length 0 and CRC 0: no real record is that short (a payload has
//! at least an op byte and a key length), so it can't be confused with data.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::wal::{self, HEADER_LEN, Record};

pub const HEARTBEAT: [u8; HEADER_LEN] = [0; HEADER_LEN];

pub enum Frame {
    /// A record and its encoded size (how far it moves the stream offset).
    Record(Record, u64),
    Heartbeat,
}

/// Read one frame. Corruption is an `InvalidData` error: the connection is
/// dropped and the replica reconnects.
pub async fn read_frame(r: &mut (impl AsyncRead + Unpin)) -> io::Result<Frame> {
    let mut header = [0u8; HEADER_LEN];
    r.read_exact(&mut header).await?;
    if header == HEARTBEAT {
        return Ok(Frame::Heartbeat);
    }
    let (len, crc) = wal::parse_header(&header).ok_or_else(|| invalid("record too large"))?;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    let record = wal::decode_payload(payload, crc).ok_or_else(|| invalid("bad record"))?;
    Ok(Frame::Record(record, (HEADER_LEN + len) as u64))
}

/// The leader's answer to `SYNC`.
#[derive(Debug, PartialEq, Eq)]
pub enum SyncReply {
    /// Drop everything, load `keys` snapshot records, then apply the stream from
    /// `offset`. The data is a consistent copy of the leader once the replica
    /// has applied up to `consistent_at`.
    FullSync {
        replid: String,
        offset: u64,
        consistent_at: u64,
        keys: u64,
    },
    /// Keep your data; the stream continues from the offset you sent.
    Continue { replid: String },
}

impl SyncReply {
    pub fn parse(line: &str) -> io::Result<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        let num = |s: &str| s.parse::<u64>().map_err(|_| invalid(line));
        match parts.as_slice() {
            ["FULLSYNC", replid, offset, consistent_at, keys] => Ok(SyncReply::FullSync {
                replid: replid.to_string(),
                offset: num(offset)?,
                consistent_at: num(consistent_at)?,
                keys: num(keys)?,
            }),
            ["CONTINUE", replid] => Ok(SyncReply::Continue {
                replid: replid.to_string(),
            }),
            _ => Err(invalid(&format!("unexpected reply to SYNC: {line}"))),
        }
    }

    pub fn to_line(&self) -> String {
        match self {
            SyncReply::FullSync {
                replid,
                offset,
                consistent_at,
                keys,
            } => format!("FULLSYNC {replid} {offset} {consistent_at} {keys}\n"),
            SyncReply::Continue { replid } => format!("CONTINUE {replid}\n"),
        }
    }
}

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn sync_reply_roundtrip() {
        for reply in [
            SyncReply::FullSync {
                replid: "abc".into(),
                offset: 10,
                consistent_at: 25,
                keys: 3,
            },
            SyncReply::Continue {
                replid: "abc".into(),
            },
        ] {
            let line = reply.to_line();
            assert_eq!(SyncReply::parse(line.trim_end()).unwrap(), reply);
        }
        assert!(SyncReply::parse("ERR nope").is_err());
    }

    #[tokio::test]
    async fn frames_records_and_heartbeats() {
        let mut bytes = wal::encode_set("k", b"v");
        bytes.extend_from_slice(&HEARTBEAT);
        bytes.extend(wal::encode_del("k"));
        let mut r = bytes.as_slice();

        let Frame::Record(rec, size) = read_frame(&mut r).await.unwrap() else {
            panic!("expected a record")
        };
        assert_eq!(
            rec,
            Record::Set {
                key: "k".into(),
                value: Bytes::from("v")
            }
        );
        assert_eq!(size, wal::encode_set("k", b"v").len() as u64);
        assert!(matches!(
            read_frame(&mut r).await.unwrap(),
            Frame::Heartbeat
        ));
        assert!(matches!(
            read_frame(&mut r).await.unwrap(),
            Frame::Record(Record::Del { .. }, _)
        ));
        assert!(read_frame(&mut r).await.is_err()); // EOF
    }

    #[tokio::test]
    async fn corrupted_record_is_an_error() {
        let mut bytes = wal::encode_set("key", b"value");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        let err = read_frame(&mut bytes.as_slice()).await.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
