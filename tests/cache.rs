use sosi::cache::Cache;

#[test]
fn default_shard_bits() {
    let cache: Cache<usize> = Cache::new(16 * 1024 * 1024).build();
    assert_eq!(cache.num_shard_bits, 5);

    let cache: Cache<usize> = Cache::new(511 * 1024).build();
    assert_eq!(cache.num_shard_bits, 0);

    let cache: Cache<usize> = Cache::new(1024 * 1024 * 1024).build();
    assert_eq!(cache.num_shard_bits, 6);
}

#[test]
fn usage() {
    let capacity = 100000;
    let cache = Cache::new(capacity).num_shard_bits(8).unwrap().build();
    assert_eq!(cache.get_usage(), 0);

    let mut usage = 0;
    let val = "abcde";
    let kv_size = 8 + 5;
    for i in 0..100 {
        cache.insert(i, val, kv_size);
        usage += kv_size;
    }
    assert_eq!(usage, cache.get_usage());

    // Overload the cache
    for i in 0..capacity / 10 {
        cache.insert(i, val, kv_size);
    }

    let lower = (capacity as f32 * 0.95) as usize;
    assert!((lower..capacity).contains(&cache.get_usage()));
}

#[test]
fn hit_and_miss() {
    let cache = Cache::new(2).build();

    cache.insert(100, 101, 1);
    assert_eq!(cache.lookup(100).value(), Some(&mut 101));
    assert_eq!(cache.lookup(200).value(), None);
    assert_eq!(cache.lookup(300).value(), None);

    cache.insert(200, 201, 1);
    assert_eq!(cache.lookup(100).value(), Some(&mut 101));
    assert_eq!(cache.lookup(200).value(), Some(&mut 201));
    assert_eq!(cache.lookup(300).value(), None);

    cache.insert(100, 102, 1);
    // Lookup 200 first because we don't want to reorder the cache after assert.
    assert_eq!(cache.lookup(200).value(), Some(&mut 201));
    assert_eq!(cache.lookup(100).value(), Some(&mut 102));
    assert_eq!(cache.lookup(300).value(), None);

    cache.insert(300, 301, 1);
    assert_eq!(cache.lookup(100).value(), Some(&mut 102));
    assert_eq!(cache.lookup(200).value(), None);
    assert_eq!(cache.lookup(300).value(), Some(&mut 301));
}

#[test]
fn erase() {
    let cache = Cache::new(2).build();

    let erased = cache.erase(200);
    assert_eq!(erased, None);

    cache.insert(100, 101, 1);
    cache.insert(200, 201, 1);
    let erased = cache.erase(100);
    assert_eq!(cache.lookup(100).value(), None);
    assert_eq!(erased, Some(101));
    assert_eq!(cache.lookup(200).value(), Some(&mut 201));

    let erased = cache.erase(100);
    assert_eq!(erased, None);
}
