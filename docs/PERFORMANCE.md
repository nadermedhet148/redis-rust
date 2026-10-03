# rkv — Performance & Safety

This document covers how rkv is measured and tested for **throughput, CPU, memory,
concurrency safety, and durability**, and what the numbers mean.
For how the system itself works, see [ARCHITECTURE.md](ARCHITECTURE.md).

The raw output for each episode, with every test result and benchmark table, is
saved in [`results/`](../results/) (`results/ep03.md`, ...) by
`scripts/test-report.sh`. This document quotes from those runs.

Everything here is reproducible from the repo:

```sh
scripts/test-report.sh                                # all tests + benchmarks -> results/<tag>.md
cargo test --release                                  # all safety / memory / CPU / crash tests
cargo test --release -- --nocapture                   # same, printing the measured numbers
cargo build --release && target/release/rkv-bench --compare stores   # TCP benchmark table
cargo bench --bench store                             # in-memory lock benchmark (criterion)
```

**Test machine:** Intel Core Ultra 9 275HX (24 cores: 8 P + 16 E, no SMT), 32 GB RAM,
Windows 11, Rust 1.96.1, release build. Loopback TCP.

---

## 1. The three store versions (ep02)

| `--store` | Structure | Reads | Writes |
|-----------|-----------|-------|--------|
| `mutex`   | `Arc<Mutex<HashMap>>` | exclusive | exclusive |
| `rwlock`  | `Arc<RwLock<HashMap>>` | shared | exclusive |
| `sharded` | 64 × `RwLock<HashMap>`, shard = `hash(key) & 63` | shared per shard | exclusive per shard |

All three live behind the same `Db` handle (`src/db.rs`), so every test and
benchmark runs against all three.

---

## 2. Throughput & latency

### 2a. Over TCP — `rkv-bench --compare stores`

`rkv-bench --compare stores` starts a separate `rkv` **process** per store (so server
CPU/memory are measured without the load generator), preloads the key space,
then runs N clients × M request/response ops.

50 clients × 20,000 ops, 80% GET / 20% SET, 10k keys, 64 B values:

| store   | ops/sec | p50 (µs) | p99 (µs) | server CPU (cores) | CPU µs/op | RSS idle (MB) | RSS peak (MB) |
|---------|--------:|---------:|---------:|-------------------:|----------:|--------------:|--------------:|
| mutex   |  137238 |      328 |      853 |               4.37 |      31.9 |           5.8 |          11.0 |
| rwlock  |  132219 |      336 |      923 |               4.54 |      34.4 |           5.8 |           9.6 |
| sharded |  132935 |      335 |      903 |               4.71 |      35.4 |           5.9 |           9.9 |

Same at 50% reads (mutex 138k, rwlock 133k, sharded 130k ops/s).

**Takeaway: over TCP the lock doesn't matter.** The server spends ~32 µs of CPU
per request, almost all of it in socket syscalls and the tokio reactor. A
`HashMap` op under a lock is ~50 ns, so it's <0.2% of the work. The differences
in the table are run-to-run noise. To see the locks, take the network away (2b).

### 2b. In memory — `cargo bench --bench store`

T OS threads call `Db::get`/`Db::set` directly (80% reads, 10k keys, 64 B values).
Millions of ops/sec, median of criterion's estimate:

| threads | mutex | rwlock | sharded | sharded vs mutex |
|--------:|------:|-------:|--------:|-----------------:|
|       1 |  15.3 |   15.1 |    11.4 | 0.7× |
|       4 |   5.0 |    4.2 |    26.8 | 5.3× |
|       8 |   5.0 |    2.9 |    53.3 | 10.7× |
|      16 |   4.0 |    4.0 |  **86.5** | **21.8×** |
|      24 |   4.9 |    3.5 |    67.8 | 13.9× |

What this shows:

- **Mutex collapses from 1 → 4 threads** (15 → 5 M/s) and never recovers: all
  threads serialize on one cache line.
- **RwLock is *not* better than Mutex here; at 8 threads it's worse.** Every
  reader still does an atomic write to the lock's reader count, so the lock's
  cache line bounces between cores just like a mutex. On top of that, 20%
  writers must wait for all readers to drain. RwLock only wins when reads are
  long and writes are rare.
- **Sharding scales almost linearly up to the P-core count.** Threads usually hit
  different shards, so the cache lines they touch rarely overlap.
- **Single-thread sharded is ~25% slower**: it hashes the key twice (once to
  pick the shard, once inside the shard's `HashMap`). That's the price of
  sharding with no contention to win back.
- **16 → 24 threads drops**: the extra 8 threads land on E-cores, which are
  slower, and criterion waits for the slowest thread.

---

## 3. CPU usage — `tests/cpu.rs`

Measured as this process's CPU time (`sysinfo`), in its own test binary so no
other test's work is counted.

| Test | Asserts | Measured (release) |
|------|---------|--------------------|
| `idle_server_with_open_connections_uses_no_cpu` | 200 open idle connections, 2 s window: ≤ 100 ms CPU (5% of a core) | **0 ms** |
| `cpu_time_per_request_is_bounded` | 16 clients × 2,000 mixed ops: < 500 µs CPU/request (client + server) | **25.9 µs** |

The idle test catches busy loops and polling timers: an idle tokio server must
block in the OS (IOCP / epoll), not spin. The thresholds are loose so the tests
also pass on a debug build and on slower machines; they're there to catch
regressions, and the real numbers are in the "Measured" column.

---

## 4. Memory — `tests/memory.rs`

A **counting global allocator** records every `alloc`/`dealloc`/`realloc`, which
gives exact live heap bytes, not noisy RSS. It keeps two counters:

- **per-thread** (`local()`): used by the single-threaded `Db` tests. A global
  counter picked up noise from the test harness spawning *other* test threads
  mid-measurement (we saw ~9 KB of fake "leaks" before switching).
- **process-wide** (`live()`): used by the server test, whose work happens on
  tokio worker threads; it allows a small tolerance.

| Test | Asserts | Measured |
|------|---------|----------|
| `per_key_overhead_is_bounded` | 100k keys × 100 B values: overhead beyond key+value bytes ≤ 128 B/key | **81 B/key** (all 3 stores); 10.9 MB payload → 19.0 MB heap |
| `dropping_the_store_frees_everything` | after the last `Db` clone is dropped, heap returns to baseline **exactly** | **0 B** leaked |
| `del_frees_keys_and_values` | DEL on every key frees ≥ all key + value bytes | 11.5 MB freed; 7.5 MB table capacity retained |
| `overwriting_does_not_grow_memory` | 100 rounds of overwriting 1,000 keys: **0 B** growth | **0 B** |
| `connections_release_their_memory` | 500 connections ≤ 32 KB each; after close, heap back within 64 KB of baseline | **9.7 KB/conn**; 10–40 KB residue |

Where the 81 B/key comes from: a `HashMap<String, Bytes>` slot is 56 B
(`String` 24 + `Bytes` 32) + 1 control byte. hashbrown keeps the table ≤ 7/8
full and power-of-two sized, so 100k keys get 131,072 slots → ~7.5 MB ≈ 75 B/key.
The remaining ~6 B/key is key capacity beyond length: `format!("key:{i}")` grows
its `String` to 16 B for a ~10 B key, and the test counts only the length as payload.

### Things these tests surfaced

1. **`HashMap` never shrinks on `remove`.** After deleting all 100k keys, 7.5 MB
   of empty table remains. Not a leak (it's freed on drop and reused by later
   inserts), but a long-running server that grows then deletes will hold its
   peak table size. Fix if it matters: `shrink_to_fit` when `len` drops well
   below `capacity`.
2. **An overwrite can allocate.** hashbrown's `insert` calls `reserve(1)`
   *before* checking whether the key already exists, so a map that is exactly
   full resizes even when the insert only replaces a value. With 64 shards, a
   few usually end up exactly full: the first overwrite round grew the sharded
   store by ~5–8 KB, and every round after that by 0 B. The test does one
   warm-up overwrite round before taking its baseline.

---

## 5. Concurrency safety — `tests/concurrency.rs`

Every test runs against **all three stores**. Server tests use multi-threaded
tokio runtimes and fail on a 60 s timeout, so a deadlock fails the test instead
of hanging it.

| Test | What it proves |
|------|----------------|
| `no_lost_writes_across_threads` | 24 threads × 5,000 SETs on disjoint keys; every write is readable afterwards. |
| `no_torn_values_on_contended_keys` | Half the threads overwrite 8 hot keys with 1 KB values whose bytes are all the writer's id; the other half read continuously for 500 ms. Any value that mixes bytes from two writes fails the test. |
| `read_your_writes_and_deletes_under_contention` | Each thread does SET→GET→DEL→GET→DEL on keys sharing shards with every other thread: own writes are visible, DEL returns true once, then false. |
| `db_clones_share_one_store` | `Db::clone()` shares state; `Db::new()` doesn't. |
| `server_100_clients_read_your_writes` | 100 TCP clients × 200 SET/GET pairs; each sees its own latest write. |
| `server_contended_key_never_returns_torn_value` | 20 TCP clients race SET/GET on one 512 B key; every GET is a complete value. |
| `abrupt_disconnects_do_not_affect_other_clients` | 50 clients send half-lines, invalid UTF-8 or nothing, then drop; a live client's data and connection are unaffected. |

Why these are the right properties for this design: values are `Bytes`
(immutable, refcounted), and every map access happens entirely under a lock, so
a reader can only ever get a pointer to a complete value. These tests check
that this holds in practice, and they'll keep checking when the storage engine
changes (LSM in ep04). With the WAL (ep03), the same tests still pass: they
use in-memory `Db`s, and section 6 covers the WAL itself.

**Not covered yet** (worth adding as the design gets more complex):
- **loom** model checking: exhaustively explores thread interleavings. More
  valuable from ep04 on (memtable flush racing reads) than for a single `RwLock`.
- **Linearizability checking** (e.g. a Porcupine/Knossos-style history checker)
  for ep05–07, where replication makes stale reads possible by design.

---

## 6. Durability — WAL (ep03)

Run with `--wal <path>`. Every SET/DEL is appended to the log before it is
applied in memory, and the log is replayed on startup.

```text
record  = len: u32 | crc: u32 | payload[len]          (little-endian)
payload = op: u8   | key_len: u32 | key | value        op: 1 = SET, 2 = DEL
```

Design decisions, and why:

- **One `write()` syscall per record, no userspace buffer.** Once `write`
  returns, the record is in the OS page cache, so it survives the *process*
  being killed (`kill -9`, panic, OOM-kill) under **every** fsync policy. fsync
  only matters if the *machine* dies (power loss, kernel panic).
- **The append happens while holding the key's lock** (the map lock, or the
  shard lock for `sharded`). Two writes to the same key reach the log in the
  same order they reach memory, so replay rebuilds exactly the state clients
  saw. Writes to different shards can be logged in any order, which is fine
  because they don't affect each other.
- **Log first, then apply.** If the append fails, memory isn't touched and the
  client gets `ERR storage: ...`, so the server never acknowledges a write it
  didn't log.
- **Replay stops at the first bad record** (cut short, CRC mismatch, length >
  64 MiB, or undecodable) and **truncates the file there**. Otherwise new
  records would be appended after garbage and never be read.
- **The server binds its port only after replay finishes.** A client that can
  connect sees all recovered data.

### fsync policies — `rkv-bench --compare fsync`

Sharded store, 50 clients × 2,000 ops, 80% GET / 20% SET:

| `--fsync` | ops/sec | p50 (µs) | p99 (µs) | what a power loss can lose |
|-----------|--------:|---------:|---------:|----------------------------|
| (no WAL)    |  141863 |      313 |      903 | everything |
| `never`     |  127708 |      343 |     1034 | whatever the OS hadn't flushed yet (Linux default: up to ~30 s) |
| `every-sec` |  125483 |      344 |     1083 | up to ~1 s of acknowledged writes |
| `always`    |   17635 |     1959 |    11056 | nothing that was acknowledged |

- **The WAL itself is cheap**: ~10% for `never` / `every-sec`, i.e. one extra
  `write` syscall per SET.
- **`always` is 8× slower, and reads get slower too** (p50 0.3 → 2 ms, even
  though 80% of ops are GETs). Two reasons, both visible in the code:
  1. All SETs serialize on the WAL's mutex, and each one holds it through a
     ~0.3 ms `fsync`, so throughput is capped at roughly 1 / fsync-latency SETs
     per second.
  2. The SET also holds its **shard lock** while it waits for the fsync, so
     GETs on that shard wait for the disk too.

  The standard fix is **group commit**: many writers append, one fsync covers
  all of them, and everyone gets acknowledged together. Not implemented yet.

### Crash tests — `tests/crash.rs`

These run the real `rkv` binary as a child process and kill it with
`Child::kill` (SIGKILL on Unix, TerminateProcess on Windows: no cleanup).

| Test | Asserts | Measured |
|------|---------|----------|
| `kill9_mid_write_loses_no_acknowledged_write` | For each policy: 8 clients write `k{c}:{i}` in order, the server is killed 300 ms in and restarted on the same WAL. Every write that got `OK` is present; nothing beyond the single in-flight write per client exists. | always: ~1,000 acked writes, every-sec / never: ~20,000; **all recovered** |
| `deletes_and_overwrites_survive_restart` | SET, overwrite, DEL, values with spaces: replay gives the final state, for each policy. | ✓ |
| `torn_record_at_tail_is_discarded_on_restart` | Append half a record to the WAL after a kill. The server starts, the file is truncated back to the last good record, all 100 earlier keys are intact, and new writes after recovery survive another kill. | ✓ |

Unit tests in `src/wal.rs` go further on the format: they **cut the last record
at every byte offset** and **flip every byte of it**; replay must return exactly
the records before it every time. They also check that a garbage length
(4 GiB) doesn't allocate, and that appends after a truncation are readable.

**What these tests can't show:** surviving *power loss*. A killed process
doesn't drop the OS page cache, so all three policies pass the kill -9 test.
Testing fsync for real needs a VM you can hard-reset, or a fault-injecting
filesystem (e.g. dm-flakey / LazyFS). That's the difference between the
policies; the benchmark above shows what each one costs.

**Known limitation:** a corrupt record in the *middle* of the log (bit rot, not
a crash) is treated like a torn tail: everything after it is dropped, with a
`WARN` in the log. A stricter design would refuse to start and ask an operator.

**Demo for the video:** `scripts/crash-demo.sh [always|every-sec|never]` writes
keys, kills the server with `kill -9`, restarts it, then appends a torn record
by hand and shows replay truncating it.

---

## 7. Running it yourself

```sh
# everything, saved to results/<git describe>.md
scripts/test-report.sh
CRITERION=1 scripts/test-report.sh      # also the in-memory criterion bench (~15 min)

# everything, with numbers, to the terminal
cargo test --release -- --nocapture

# only one area
cargo test --release --test concurrency
cargo test --release --test memory -- --nocapture
cargo test --release --test cpu -- --nocapture
cargo test --release --test crash -- --nocapture
cargo test --release --lib wal                           # WAL format unit tests

# benchmarks
cargo build --release
target/release/rkv-bench --compare stores                # 50 clients x 20k ops, 80% reads
target/release/rkv-bench --compare fsync -n 2000         # WAL fsync policies
target/release/rkv-bench --compare stores -c 100 -n 5000 --read-ratio 0.5
target/release/rkv-bench --addr 127.0.0.1:6380           # against a server you started
cargo bench --bench store                                # criterion; HTML in target/criterion/
```

Benchmark numbers vary run to run by roughly ±5% over TCP and more on the
in-memory benchmark at high thread counts. Compare stores within one run, not
across runs or machines.
