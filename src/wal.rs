//! Write-ahead log: every SET/DEL is appended here before it is applied in memory,
//! and replayed on startup.
//!
//! On-disk format, all integers little-endian:
//!
//! ```text
//! record  = len: u32 | crc: u32 | payload[len]
//! payload = op: u8   | key_len: u32 | key[key_len] | value[..]
//! ```
//!
//! `crc` is CRC-32 of `payload`. On replay, the first record that is cut short
//! or fails its CRC marks the end of the valid log; the file is truncated there.
//!
//! Each record goes to the OS in a single `write` call, so a record the server
//! acknowledged survives the *process* dying (kill -9) under every policy.
//! `FsyncPolicy` only decides what survives the *machine* dying (power loss).

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;
use tracing::{info, warn};

const HEADER_LEN: usize = 8;
const OP_SET: u8 = 1;
const OP_DEL: u8 = 2;
/// Upper bound on one record, so a corrupted length can't make replay allocate gigabytes.
const MAX_RECORD_LEN: u32 = 64 * 1024 * 1024;

/// When the log is fsynced to the disk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum FsyncPolicy {
    /// After every record, before the client gets "OK". Survives power loss; slowest.
    Always,
    /// From a background thread once per second. Power loss can lose up to ~1s of writes.
    #[default]
    EverySec,
    /// Never; the OS flushes when it wants. Power loss can lose much more.
    Never,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Set { key: String, value: Bytes },
    Del { key: String },
}

pub struct Wal {
    shared: Arc<Shared>,
}

struct Shared {
    file: Mutex<File>,
    policy: FsyncPolicy,
    /// Written since the last fsync (used by `EverySec`).
    dirty: AtomicBool,
}

/// What `Wal::open` found in an existing log.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReplayStats {
    pub records: usize,
    /// Bytes cut off the end because they were an incomplete or corrupt record.
    pub truncated_bytes: u64,
}

impl Wal {
    /// Open (or create) the log at `path`, call `apply` for every valid record in
    /// order, cut off any invalid tail, and return the log ready for appends.
    pub fn open(
        path: impl AsRef<Path>,
        policy: FsyncPolicy,
        mut apply: impl FnMut(Record),
    ) -> io::Result<(Wal, ReplayStats)> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let mut stats = ReplayStats::default();
        let valid_len = {
            let mut reader = BufReader::new(&mut file);
            let mut offset = 0u64;
            loop {
                match read_record(&mut reader)? {
                    Some((record, size)) => {
                        apply(record);
                        stats.records += 1;
                        offset += size;
                    }
                    None => break offset,
                }
            }
        };

        if valid_len < file_len {
            stats.truncated_bytes = file_len - valid_len;
            warn!(
                path = %path.display(),
                valid_len,
                truncated = stats.truncated_bytes,
                "WAL has an incomplete or corrupt tail; truncating"
            );
            file.set_len(valid_len)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        info!(path = %path.display(), records = stats.records, ?policy, "WAL replayed");

        let shared = Arc::new(Shared {
            file: Mutex::new(file),
            policy,
            dirty: AtomicBool::new(false),
        });
        if policy == FsyncPolicy::EverySec {
            spawn_syncer(Arc::downgrade(&shared));
        }
        Ok((Wal { shared }, stats))
    }

    pub fn append_set(&self, key: &str, value: &[u8]) -> io::Result<()> {
        self.append(&encode(OP_SET, key, value))
    }

    pub fn append_del(&self, key: &str) -> io::Result<()> {
        self.append(&encode(OP_DEL, key, &[]))
    }

    fn append(&self, record: &[u8]) -> io::Result<()> {
        let mut file = self.shared.file.lock().unwrap();
        // One write call per record: once it returns, the record is in the OS
        // page cache and survives this process crashing.
        file.write_all(record)?;
        match self.shared.policy {
            FsyncPolicy::Always => file.sync_data()?,
            FsyncPolicy::EverySec => self.shared.dirty.store(true, Ordering::Release),
            FsyncPolicy::Never => {}
        }
        Ok(())
    }
}

/// Background fsync for `EverySec`. Holds only a Weak, so it exits once the Wal is dropped.
fn spawn_syncer(shared: Weak<Shared>) {
    std::thread::Builder::new()
        .name("wal-fsync".into())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let Some(shared) = shared.upgrade() else {
                    return;
                };
                if shared.dirty.swap(false, Ordering::AcqRel) {
                    let file = shared.file.lock().unwrap();
                    if let Err(e) = file.sync_data() {
                        warn!(error = %e, "WAL fsync failed");
                        shared.dirty.store(true, Ordering::Release);
                    }
                }
            }
        })
        .expect("failed to spawn wal-fsync thread");
}

fn encode(op: u8, key: &str, value: &[u8]) -> Vec<u8> {
    let payload_len = 1 + 4 + key.len() + value.len();
    let mut buf = Vec::with_capacity(HEADER_LEN + payload_len);
    buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
    buf.extend_from_slice(&[0; 4]); // crc, filled in below
    buf.push(op);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf.extend_from_slice(value);
    let crc = crc32fast::hash(&buf[HEADER_LEN..]);
    buf[4..8].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Read one record. `Ok(None)` means "end of the valid log": a clean EOF, a
/// record cut short, a CRC mismatch, or a payload that doesn't decode.
/// `Err` is only for real I/O errors.
fn read_record(r: &mut impl Read) -> io::Result<Option<(Record, u64)>> {
    let mut header = [0u8; HEADER_LEN];
    if !read_full(r, &mut header)? {
        return Ok(None);
    }
    let len = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
    if len > MAX_RECORD_LEN {
        return Ok(None);
    }
    let mut payload = vec![0u8; len as usize];
    if !read_full(r, &mut payload)? || crc32fast::hash(&payload) != crc {
        return Ok(None);
    }
    Ok(decode(payload).map(|rec| (rec, (HEADER_LEN + len as usize) as u64)))
}

/// Like `read_exact`, but a short read returns `Ok(false)` instead of an error.
fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<bool> {
    match r.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

fn decode(payload: Vec<u8>) -> Option<Record> {
    let (&op, rest) = payload.split_first()?;
    let key_len = u32::from_le_bytes(rest.get(0..4)?.try_into().ok()?) as usize;
    let key = std::str::from_utf8(rest.get(4..4 + key_len)?)
        .ok()?
        .to_string();
    let value = rest.get(4 + key_len..)?;
    match op {
        OP_SET => Some(Record::Set {
            key,
            value: Bytes::copy_from_slice(value),
        }),
        OP_DEL if value.is_empty() => Some(Record::Del { key }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replay(path: &Path) -> (Vec<Record>, ReplayStats) {
        let mut out = Vec::new();
        let (_wal, stats) = Wal::open(path, FsyncPolicy::Never, |r| out.push(r)).unwrap();
        (out, stats)
    }

    fn set(k: &str, v: &str) -> Record {
        Record::Set {
            key: k.into(),
            value: Bytes::copy_from_slice(v.as_bytes()),
        }
    }

    fn write_sample(path: &Path) -> Vec<Record> {
        let (wal, _) = Wal::open(path, FsyncPolicy::Always, |_| {}).unwrap();
        wal.append_set("a", b"1").unwrap();
        wal.append_set("b", b"hello world").unwrap();
        wal.append_del("a").unwrap();
        wal.append_set("c", b"").unwrap();
        vec![
            set("a", "1"),
            set("b", "hello world"),
            Record::Del { key: "a".into() },
            set("c", ""),
        ]
    }

    #[test]
    fn empty_log() {
        let dir = tempfile::tempdir().unwrap();
        let (records, stats) = replay(&dir.path().join("wal"));
        assert!(records.is_empty());
        assert_eq!(stats, ReplayStats::default());
    }

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let expected = write_sample(&path);
        let (records, stats) = replay(&path);
        assert_eq!(records, expected);
        assert_eq!(stats.truncated_bytes, 0);
    }

    #[test]
    fn appends_after_reopen_continue_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let mut expected = write_sample(&path);
        {
            let (wal, _) = Wal::open(&path, FsyncPolicy::Never, |_| {}).unwrap();
            wal.append_set("d", b"4").unwrap();
        }
        expected.push(set("d", "4"));
        assert_eq!(replay(&path).0, expected);
    }

    /// Simulate a crash in the middle of writing the last record: cut the file at
    /// every possible byte inside it. Replay must return exactly the records
    /// before it and truncate the partial one away.
    #[test]
    fn torn_last_record_at_every_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let expected = write_sample(&path);
        let full = std::fs::read(&path).unwrap();
        let last_len = encode(OP_SET, "c", b"").len();
        let last_start = full.len() - last_len;

        for cut in last_start..full.len() {
            std::fs::write(&path, &full[..cut]).unwrap();
            let (records, stats) = replay(&path);
            assert_eq!(records, expected[..3], "cut at byte {cut}");
            assert_eq!(stats.truncated_bytes, (cut - last_start) as u64);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), last_start as u64);
        }
    }

    /// Flip each byte of the last record (header and payload): the CRC (or the
    /// length check) must reject it.
    #[test]
    fn corrupted_last_record_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let expected = write_sample(&path);
        let full = std::fs::read(&path).unwrap();
        let last_start = full.len() - encode(OP_SET, "c", b"").len();

        for i in last_start..full.len() {
            let mut bad = full.clone();
            bad[i] ^= 0xFF;
            std::fs::write(&path, &bad).unwrap();
            let (records, _) = replay(&path);
            assert_eq!(records, expected[..3], "flipped byte {i}");
        }
    }

    #[test]
    fn garbage_length_does_not_allocate_huge_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let expected = write_sample(&path);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xFF; 16]).unwrap(); // len = 4 GiB - 1
        drop(file);
        let (records, stats) = replay(&path);
        assert_eq!(records, expected);
        assert_eq!(stats.truncated_bytes, 16);
    }

    #[test]
    fn writes_after_truncation_are_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal");
        let mut expected = write_sample(&path);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[7, 0, 0, 0, 1, 2]).unwrap(); // half a header
        drop(file);
        {
            let (wal, _) = Wal::open(&path, FsyncPolicy::Never, |_| {}).unwrap();
            wal.append_set("after", b"crash").unwrap();
        }
        expected.push(set("after", "crash"));
        let (records, stats) = replay(&path);
        assert_eq!(records, expected);
        assert_eq!(stats.truncated_bytes, 0);
    }
}
