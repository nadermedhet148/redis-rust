//! Backups over the replication protocol. The leader needs no backup code:
//! `rkv-backup` connects like a brand-new replica (`SYNC ? -1`) and writes what
//! it receives to a file in the WAL format. So a backup file *is* a WAL, and
//! restoring is just starting a server on (a copy of) it:
//!
//!   rkv-backup snapshot --from 127.0.0.1:6380 --out backup.rkv
//!   rkv-backup follow   --from 127.0.0.1:6380 --out live.rkv   # keeps appending
//!   rkv-backup verify backup.rkv
//!   cp backup.rkv restored.wal && rkv --wal restored.wal
//!
//! A snapshot is consistent: it contains the leader's fuzzy snapshot *plus*
//! the stream up to `consistent_at`, so restoring it gives the exact state the
//! leader had at one moment (see docs/REPLICATION.md).

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::time::timeout;

use rkv::db::{Db, StoreKind};
use rkv::repl::replica::LEADER_TIMEOUT;
use rkv::repl::stream::{Frame, SyncReply, read_frame};
use rkv::wal;

#[derive(Parser)]
#[command(
    name = "rkv-backup",
    about = "Back up an rkv server, or check a backup file"
)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Write a consistent point-in-time copy of the server to a file, then exit
    Snapshot {
        /// Server to back up (leader or replica)
        #[arg(long, value_name = "HOST:PORT")]
        from: String,
        #[arg(long)]
        out: PathBuf,
    },
    /// Like snapshot, then keep appending every new write until stopped
    Follow {
        #[arg(long, value_name = "HOST:PORT")]
        from: String,
        #[arg(long)]
        out: PathBuf,
    },
    /// Read a backup (or any WAL) file and print its record count, key count and digest
    Verify { file: PathBuf },
}

#[tokio::main]
async fn main() -> ExitCode {
    let result = match Args::parse().cmd {
        Cmd::Snapshot { from, out } => snapshot(&from, &out).await.map(|_| ExitCode::SUCCESS),
        Cmd::Follow { from, out } => follow(&from, &out).await.map(|()| ExitCode::SUCCESS),
        Cmd::Verify { file } => verify(&file),
    };
    result.unwrap_or_else(|e| {
        eprintln!("error: {e:#}");
        ExitCode::FAILURE
    })
}

/// An open replication stream, positioned right after the consistent point.
struct Stream {
    reader: AsyncBufReader<OwnedReadHalf>,
    /// Keeps the connection open (we never send ACKs; the leader doesn't need them).
    _writer: tokio::net::tcp::OwnedWriteHalf,
    offset: u64,
}

/// Write the snapshot to `<out>.tmp`, fsync it, rename it to `out` (atomic:
/// `out` is either the old file or the complete new one, never half-written).
async fn snapshot(from: &str, out: &Path) -> anyhow::Result<Stream> {
    let started = Instant::now();
    let stream = TcpStream::connect(from)
        .await
        .with_context(|| format!("connecting to {from}"))?;
    stream.set_nodelay(true)?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = AsyncBufReader::new(reader);
    writer.write_all(b"SYNC ? -1\n").await?;

    let mut line = String::new();
    timeout(LEADER_TIMEOUT, reader.read_line(&mut line)).await??;
    let SyncReply::FullSync {
        replid,
        offset,
        consistent_at,
        keys,
    } = SyncReply::parse(line.trim_end())?
    else {
        anyhow::bail!("expected FULLSYNC, got {line:?}");
    };

    let mut tmp = out.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let mut file = BufWriter::new(File::create(&tmp).with_context(|| tmp.display().to_string())?);
    let mut records = 0u64;
    for _ in 0..keys {
        let Frame::Record(record, _) = timeout(LEADER_TIMEOUT, read_frame(&mut reader)).await??
        else {
            anyhow::bail!("heartbeat inside the snapshot");
        };
        file.write_all(&wal::encode_record(&record))?;
        records += 1;
    }
    // The snapshot alone is fuzzy; it is consistent once the stream has been
    // applied up to `consistent_at`.
    let mut pos = offset;
    while pos < consistent_at {
        if let Frame::Record(record, size) =
            timeout(LEADER_TIMEOUT, read_frame(&mut reader)).await??
        {
            file.write_all(&wal::encode_record(&record))?;
            records += 1;
            pos += size;
        }
    }
    let file = file.into_inner()?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, out)?;

    let (db, _) = load(out)?;
    println!(
        "{}: {} keys ({records} records), leader replid={replid} offset={pos}, digest={:016x}, {:.1?}",
        out.display(),
        db.len(),
        db.digest(),
        started.elapsed()
    );
    Ok(Stream {
        reader,
        _writer: writer,
        offset: pos,
    })
}

/// Snapshot, then append every new record to the same file, fsyncing once a
/// second. If the connection drops, the file is still a valid backup up to the
/// last complete record: stop with an error and let the operator restart it.
async fn follow(from: &str, out: &Path) -> anyhow::Result<()> {
    let mut stream = snapshot(from, out).await?;
    let mut file = OpenOptions::new().append(true).open(out)?;
    println!("following {from}; Ctrl-C to stop");
    let mut last_sync = Instant::now();
    let mut dirty = false;
    loop {
        let frame = match timeout(LEADER_TIMEOUT, read_frame(&mut stream.reader)).await {
            Ok(Ok(frame)) => frame,
            Ok(Err(e)) => return Err(lost(out, stream.offset, e.into())),
            Err(e) => return Err(lost(out, stream.offset, e.into())),
        };
        if let Frame::Record(record, size) = frame {
            // One write per record, like the WAL: a crash leaves at most a torn tail.
            file.write_all(&wal::encode_record(&record))?;
            stream.offset += size;
            dirty = true;
        }
        if dirty && last_sync.elapsed() >= Duration::from_secs(1) {
            file.sync_data()?;
            dirty = false;
            last_sync = Instant::now();
        }
    }
}

fn lost(out: &Path, offset: u64, e: anyhow::Error) -> anyhow::Error {
    e.context(format!(
        "connection lost at offset {offset}; {} is valid up to there",
        out.display()
    ))
}

/// Exit 0 if the file is clean, 2 if it ends in an incomplete or corrupt
/// record (a restore will cut that tail off, like after a crash).
fn verify(path: &Path) -> anyhow::Result<ExitCode> {
    let (db, stats) = load(path)?;
    println!(
        "{}: {} records, {} keys, digest={:016x}, valid_bytes={}, torn_bytes={}",
        path.display(),
        stats.records,
        db.len(),
        db.digest(),
        stats.valid_bytes,
        stats.file_bytes - stats.valid_bytes
    );
    if stats.valid_bytes < stats.file_bytes {
        eprintln!("warning: invalid tail; a restore will truncate it");
        return Ok(ExitCode::from(2));
    }
    Ok(ExitCode::SUCCESS)
}

struct FileStats {
    records: u64,
    valid_bytes: u64,
    file_bytes: u64,
}

/// Replay a WAL-format file into memory without modifying it.
fn load(path: &Path) -> anyhow::Result<(Db, FileStats)> {
    let file = File::open(path).with_context(|| path.display().to_string())?;
    let file_bytes = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let db = Db::new(StoreKind::Sharded);
    let mut stats = FileStats {
        records: 0,
        valid_bytes: 0,
        file_bytes,
    };
    while let Some((record, size)) = wal::read_record(&mut reader)? {
        db.apply(record)?;
        stats.records += 1;
        stats.valid_bytes += size;
    }
    Ok((db, stats))
}
