//! The replication backlog: the last N bytes of the write stream, in memory.
//!
//! Every SET/DEL the leader applies is appended here as an encoded WAL record.
//! The stream has a byte **offset** that only ever grows: a record pushed when
//! the stream is 100 bytes long occupies offsets 100..100+len. A replica
//! remembers the offset it has applied up to; when it reconnects, the leader
//! can send just the bytes after it, as long as they are still in the buffer
//! (partial resync). If they were already evicted, the replica needs a full sync.

use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::watch;

/// Default capacity: 1 MiB of encoded records.
pub const DEFAULT_CAPACITY: usize = 1024 * 1024;

pub struct Backlog {
    ring: Mutex<Ring>,
    /// Current end offset; sender tasks wait on it instead of polling.
    end: watch::Sender<u64>,
}

struct Ring {
    bytes: VecDeque<u8>,
    /// Stream offset of `bytes[0]`.
    start: u64,
    capacity: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReadError {
    /// The requested offset was evicted; the oldest one still held is `oldest`.
    Gone { oldest: u64 },
    /// The requested offset is past the end of the stream (`end`).
    Future { end: u64 },
}

impl Backlog {
    pub fn new(capacity: usize) -> Self {
        Self {
            ring: Mutex::new(Ring {
                bytes: VecDeque::new(),
                start: 0,
                capacity,
            }),
            end: watch::Sender::new(0),
        }
    }

    /// Append one encoded record; returns the new end offset.
    ///
    /// The oldest bytes are evicted to stay within capacity, but the newest
    /// record is always kept whole, even if it alone is bigger than the capacity.
    pub fn push(&self, record: &[u8]) -> u64 {
        let mut ring = self.ring.lock().unwrap();
        ring.bytes.extend(record);
        let keep = ring.capacity.max(record.len());
        let excess = ring.bytes.len().saturating_sub(keep);
        ring.bytes.drain(..excess);
        ring.start += excess as u64;
        let end = ring.start + ring.bytes.len() as u64;
        // Published under the ring lock, so watchers never see the end go backwards.
        self.end.send_replace(end);
        end
    }

    pub fn end(&self) -> u64 {
        *self.end.borrow()
    }

    /// Up to `max` bytes starting at `offset`. Empty if `offset` is the end.
    pub fn read_from(&self, offset: u64, max: usize) -> Result<Vec<u8>, ReadError> {
        let ring = self.ring.lock().unwrap();
        let end = ring.start + ring.bytes.len() as u64;
        if offset < ring.start {
            return Err(ReadError::Gone { oldest: ring.start });
        }
        if offset > end {
            return Err(ReadError::Future { end });
        }
        let from = (offset - ring.start) as usize;
        let to = ring.bytes.len().min(from + max);
        Ok(ring.bytes.range(from..to).copied().collect())
    }

    /// Can a replica that has applied up to `offset` continue from here?
    pub fn contains(&self, offset: u64) -> bool {
        self.read_from(offset, 0).is_ok()
    }

    /// Wakes up whenever the end offset moves.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.end.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_grow_by_record_size() {
        let b = Backlog::new(100);
        assert_eq!(b.end(), 0);
        assert_eq!(b.push(b"abc"), 3);
        assert_eq!(b.push(b"defg"), 7);
        assert_eq!(b.read_from(0, 100).unwrap(), b"abcdefg");
        assert_eq!(b.read_from(3, 100).unwrap(), b"defg");
        assert_eq!(b.read_from(3, 2).unwrap(), b"de");
        assert_eq!(b.read_from(7, 100).unwrap(), b"");
    }

    #[test]
    fn evicts_oldest_bytes_but_offsets_keep_growing() {
        let b = Backlog::new(5);
        b.push(b"abc");
        b.push(b"def"); // 6 bytes > 5: "a" is evicted
        assert_eq!(b.read_from(0, 10), Err(ReadError::Gone { oldest: 1 }));
        assert_eq!(b.read_from(3, 10).unwrap(), b"def");
        assert!(b.contains(1));
        assert!(!b.contains(0));
    }

    #[test]
    fn future_offset_is_an_error() {
        let b = Backlog::new(10);
        b.push(b"abc");
        assert_eq!(b.read_from(4, 10), Err(ReadError::Future { end: 3 }));
    }

    #[test]
    fn a_record_bigger_than_capacity_is_kept_whole() {
        let b = Backlog::new(4);
        b.push(b"ab");
        assert_eq!(b.push(b"0123456789"), 12);
        assert_eq!(b.read_from(2, 100).unwrap(), b"0123456789");
        assert_eq!(b.read_from(0, 100), Err(ReadError::Gone { oldest: 2 }));
    }

    #[tokio::test]
    async fn subscribers_see_the_end_move() {
        let b = Backlog::new(100);
        let mut rx = b.subscribe();
        b.push(b"xyz");
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 3);
    }
}
