# rkv — a key-value store in Rust, built step by step

`rkv` is a small Redis-like key-value server written in Rust. It is built one
episode at a time for a video series: it starts as a `HashMap` behind a TCP
socket and grows into a replicated, Raft-based store. Every step shown on
screen is its own commit, and every episode is a git tag, so you can check out
any point in the series and run it.

```text
$ cargo run --release -- --wal data.wal
$ nc 127.0.0.1 6380
SET greeting hello world
OK
GET greeting
hello world
```

## Episodes

| Tag | What gets built | Done when | Status |
|-----|-----------------|-----------|:------:|
| `ep01` | TCP server, `Command` enum (`GET`/`SET`/`DEL`/`PING`), `Arc<Mutex<HashMap>>` | several `nc` clients work at once; the parser has unit tests | ✅ |
| `ep02` | benchmark tool (N clients × M ops), then `RwLock`, then a hash-sharded store | one table compares all 3 versions | ✅ |
| `ep03` | write-ahead log `[len][crc][op][key][value]`, fsync policy, replay on startup | `kill -9` and the data comes back; the CRC catches a half-written record | ✅ |
| `ep04` | `Storage` trait, memtable → SSTable flush, tombstones, compaction | reads go memtable → SSTables newest first; disk usage drops after compaction | ⏳ |
| `ep05` | leader ships WAL entries with sequence numbers; followers apply them | demo of replication lag and a stale read from a follower | ⏳ |
| `ep06` | Raft roles, terms, randomized election timeout, `RequestVote` | kill the leader, a new one is elected in < 2 s | ⏳ |
| `ep07` | `AppendEntries`, commit index, storage engine as the state machine | a chaos script kills random nodes and no acknowledged write is lost | ⏳ |

```sh
git checkout ep02     # jump to the end of any episode
git log --oneline     # one commit per step shown in the video
```

## Quick start

Needs a recent stable Rust (developed on 1.96).

```sh
cargo build --release

# in-memory only
target/release/rkv

# durable: write-ahead log, fsync once per second
target/release/rkv --wal data.wal

# all options
target/release/rkv --help
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--addr` | `127.0.0.1:6380` | address to listen on |
| `--store` | `mutex` | `mutex` \| `rwlock` \| `sharded`: how the in-memory map is locked |
| `--wal` | *(none)* | WAL file; without it nothing survives a restart |
| `--fsync` | `every-sec` | `always` \| `every-sec` \| `never`: when the WAL is flushed to disk |

Set `RUST_LOG=debug` for per-connection logs.

### Talking to it

The protocol is one text command per line, so any TCP tool works:

| Command | Reply |
|---------|-------|
| `PING` | `PONG` |
| `SET <key> <value...>` | `OK` (the value is the rest of the line, spaces allowed) |
| `GET <key>` | the value, or `(nil)` |
| `DEL <key>` | `1` if the key existed, else `0` |

On Linux/macOS use `nc 127.0.0.1 6380`. On Windows use `ncat` (from Nmap), WSL,
or Git Bash's `/dev/tcp` (see `scripts/crash-demo.sh` for an example).

### Desktop GUI

`gui/` is a separate crate (egui), so the server keeps its small dependency list.
It talks to a running server over the same protocol:

```sh
cargo run --release                                   # start a server
cargo run --release --manifest-path gui/Cargo.toml    # then the GUI
```

On Windows, `.\run.ps1` does both: it builds, starts the server, opens the GUI,
and stops the server when you close the window. It takes the server's options
(`-Addr`, `-Store`, `-Wal`, `-Fsync`), and `-NoGui` runs the server alone.

The GUI has GET/SET/DEL buttons, a raw command console with per-request latency,
a table of the keys the window has touched (the server has no `KEYS` command),
and a sequential round-trip benchmark.

## Tests and benchmarks

```sh
cargo test --release                        # 33 tests: protocol, WAL, concurrency, memory, CPU, crash
scripts/test-report.sh                      # all tests + benchmarks -> results/<tag>.md
scripts/crash-demo.sh                       # ep03 demo: kill -9, restart, data is back

cargo build --release
target/release/rkv-bench --compare stores   # mutex vs rwlock vs sharded over TCP
target/release/rkv-bench --compare fsync    # cost of each fsync policy
cargo bench --bench store                   # lock scaling in memory (criterion)
```

Some numbers from `ep03` (Core Ultra 9 275HX, loopback; full reports in [`results/`](results/)):

- **~140k ops/s over TCP** with 50 clients, p99 under 1 ms.
- **In memory, the sharded store reaches ~86M ops/s on 16 threads**, versus ~5M for a single mutex.
- **`--fsync always` costs ~9× in throughput**; the WAL with `every-sec` costs ~5–10%.
- **81 bytes of overhead per key**, 0 bytes leaked, ~0% CPU when idle.
- **0 acknowledged writes lost** across `kill -9` crashes under every fsync policy.

## Documentation

- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**: how the system works. It covers the request lifecycle, the three store designs, the WAL format, write ordering, crash recovery, and the concurrency model.
- **[docs/PERFORMANCE.md](docs/PERFORMANCE.md)**: how it's measured. It covers the benchmarks, what every test asserts and why, and what the measurements turned up.
- **[results/](results/)**: the saved test and benchmark output for each episode.

## Repository layout

```text
src/            server, protocol, storage, WAL (see docs/ARCHITECTURE.md §7)
src/bin/        rkv-bench load generator
benches/        criterion in-memory benchmark
tests/          integration tests: server, concurrency, memory, cpu, crash
scripts/        crash demo, test report
gui/            desktop client (separate crate)
docs/           architecture and performance write-ups
results/        saved test/benchmark reports per episode
```
