use sosi::cache::LRUCache;

/// Convenience function for test assertions
fn items<T>(cache: &LRUCache<T>) -> Vec<usize>
where
    T: Clone,
{
    cache.iter().map(|(x, _)| x.clone()).collect()
}

#[test]
fn empty() {
    let mut cache: LRUCache<u8> = LRUCache::new(4);
    assert_eq!(cache.len(), 0);
    assert_eq!(items(&mut cache), []);
}

#[test]
fn insert() {
    let mut cache = LRUCache::new(4);
    cache.insert(1, "a", 1);
    assert_eq!(cache.len(), 1);
    cache.insert(2, "b", 1);
    assert_eq!(cache.len(), 2);
    cache.insert(3, "c", 1);
    assert_eq!(cache.len(), 3);
    cache.insert(4, "d", 1);
    assert_eq!(cache.len(), 4);
    assert_eq!(
        items(&cache),
        [4, 3, 2, 1],
        "Ordered from most- to least-recent."
    );
    cache.insert(5, "e", 1);
    assert_eq!(cache.len(), 4);
    assert_eq!(
        items(&mut cache),
        [5, 4, 3, 2],
        "Least-recently-used item cleared."
    );

    cache.insert(6, "f", 4);
    assert_eq!(items(&mut cache), [6], "Least-recently-used item cleared.");
}

#[test]
fn lookup() {
    let mut cache = LRUCache::new(4);
    cache.insert(1, 100, 1);
    cache.insert(2, 200, 1);
    cache.insert(3, 300, 1);
    cache.insert(4, 400, 1);

    let result = cache.lookup(5);
    assert_eq!(result, None, "Cache miss.");
    assert_eq!(items(&mut cache), [4, 3, 2, 1], "Order not changed.");

    // Cache hit
    let result = cache.lookup(3);
    assert_eq!(result, Some(&mut 300), "Cache hit.");
    assert_eq!(
        items(&mut cache),
        [3, 4, 2, 1],
        "Matching item moved to front."
    );
}

#[test]
fn erase() {
    let mut cache = LRUCache::new(4);
    cache.insert(1, 100, 1);
    cache.erase(1);
    assert_eq!(items(&mut cache), []);

    cache.insert(1, 100, 1);
    cache.insert(2, 200, 1);
    cache.insert(3, 300, 1);
    cache.insert(4, 400, 1);
    assert_eq!(items(&mut cache), [4, 3, 2, 1]);
    cache.erase(3);
    assert_eq!(items(&mut cache), [4, 2, 1]);
}

#[test]
fn remove_lru() {
    let mut cache = LRUCache::new(4);

    cache.insert(1, 100, 1);
    cache.insert(2, 200, 1);
    cache.insert(3, 300, 1);
    cache.insert(4, 400, 1);
    cache.remove_lru();
    assert_eq!(
        items(&mut cache),
        [4, 3, 2],
        "Least-recently-used item cleared."
    );
}
