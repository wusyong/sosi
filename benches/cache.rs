use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};

use sosi::cache::Cache;

const THREADS: usize = 16;
const CAPACITY: usize = 1024 * 1024 * 1024;
const NUM_SHARDS_BITS: u8 = 6;
const VALUE_BYTES: usize = 8 * 1024;
const MAX_KEY: usize = CAPACITY / VALUE_BYTES * 4;

const HUNDREDTH_U64: u64 = u64::MAX / 100;
const LOOKUP_INSERT_THRESHOLD: u64 = HUNDREDTH_U64 * 87;
const INSERT_THRESHOLD: u64 = LOOKUP_INSERT_THRESHOLD + HUNDREDTH_U64 * 2;
const LOOKUP_THRESHOLD: u64 = INSERT_THRESHOLD + HUNDREDTH_U64 * 10;
const ERASE_THRESHOLD: u64 = LOOKUP_THRESHOLD + HUNDREDTH_U64 * 1;

fn random_op(cache: &Cache<Vec<u8>>) {
    let rnd: u64 = thread_rng().gen();
    let key = thread_rng().gen_range(0..MAX_KEY);
    match rnd {
        _ if rnd < LOOKUP_INSERT_THRESHOLD => {
            let mut result = None;
            if let Some(val) = cache.lookup(key).value() {
                // Do something
                let mut hasher = DefaultHasher::new();
                val.hash(&mut hasher);
                result = Some(hasher.finish());
            }
            if result.is_none() {
                cache.insert(key, vec![thread_rng().gen(); VALUE_BYTES], VALUE_BYTES);
            }
        }
        _ if rnd < INSERT_THRESHOLD => {
            cache.insert(key, vec![thread_rng().gen(); VALUE_BYTES], VALUE_BYTES);
        }
        _ if rnd < LOOKUP_THRESHOLD => {
            if let Some(val) = cache.lookup(key).value() {
                // Do something
                let mut hasher = DefaultHasher::new();
                val.hash(&mut hasher);
                let _ = hasher.finish();
            }
        }
        _ if rnd < ERASE_THRESHOLD => {
            cache.erase(key);
        }
        _ => unreachable!(),
    }
}

fn random_op_singlethread(c: &mut Criterion) {
    let cache = Cache::new(CAPACITY)
        .num_shard_bits(NUM_SHARDS_BITS)
        .unwrap()
        .build();
    c.bench_function("random operations to cache", |b| {
        b.iter(|| {
            random_op(&cache);
        })
    });
}

fn random_op_multithread(c: &mut Criterion) {
    c.bench_function("random operations to cache in multithread.", |b| {
        b.iter(|| {
            let mut handles = vec![];
            let main_cache = Arc::new(
                Cache::new(CAPACITY)
                    .num_shard_bits(NUM_SHARDS_BITS)
                    .unwrap()
                    .build(),
            );
            //Each sample perform MAX_KEY operations in each thread.
            for _ in 0..THREADS {
                let cache = Arc::clone(&main_cache);
                let handle = thread::spawn(move || {
                    for _ in 0..MAX_KEY {
                        random_op(&cache);
                    }
                });
                handles.push(handle);
            }

            handles.into_iter().for_each(|h| {
                h.join().unwrap();
            });
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = random_op_singlethread, random_op_multithread
}
criterion_main!(benches);
