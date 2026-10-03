//! In-memory store benchmark: no network, just T threads hammering `Db`.
//! This isolates lock contention, which the TCP benchmark hides behind syscalls.
//!
//!   cargo bench --bench store

use std::hint::black_box;
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rkv::db::{Db, StoreKind};

const KEYS: u64 = 10_000;
const READ_RATIO: f64 = 0.8;

fn mixed_workload(c: &mut Criterion) {
    let max_threads = thread::available_parallelism().map_or(8, |n| n.get());
    let mut threads: Vec<usize> = vec![1, 4, 8, 16];
    threads.retain(|&t| t < max_threads);
    threads.push(max_threads);

    let mut group = c.benchmark_group("store_80r_20w");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(1));

    for kind in [StoreKind::Mutex, StoreKind::Rwlock, StoreKind::Sharded] {
        let db = Db::new(kind);
        let keys: Vec<String> = (0..KEYS).map(|k| format!("key:{k}")).collect();
        for k in &keys {
            db.set(k.clone(), Bytes::from_static(&[b'x'; 64])).unwrap();
        }

        for &t in &threads {
            group.bench_with_input(BenchmarkId::new(format!("{kind:?}"), t), &t, |b, &t| {
                b.iter_custom(|iters| run(&db, &keys, t, iters))
            });
        }
    }
    group.finish();
}

/// Split `iters` ops across `threads` threads; return wall time for all of them.
fn run(db: &Db, keys: &[String], threads: usize, iters: u64) -> Duration {
    let per_thread = iters.div_ceil(threads as u64);
    let value = Bytes::from_static(&[b'v'; 64]);
    let start = Instant::now();
    thread::scope(|s| {
        for id in 0..threads as u64 {
            let value = value.clone();
            s.spawn(move || {
                let mut x = (id + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                for _ in 0..per_thread {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let key = &keys[(x % KEYS) as usize];
                    if ((x >> 11) as f64 / (1u64 << 53) as f64) < READ_RATIO {
                        black_box(db.get(key));
                    } else {
                        db.set(key.clone(), value.clone()).unwrap();
                    }
                }
            });
        }
    });
    start.elapsed()
}

criterion_group!(benches, mixed_workload);
criterion_main!(benches);
