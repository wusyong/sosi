use std::collections::{hash_map::Entry as MapEntry, HashMap};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use fnv::{FnvBuildHasher, FnvHasher};

use crate::Error;

/// A LRU cache implementation that contains several cache shards based on given number of shard bits.
/// If it's not provided, the cache will create the shards based on the size of capacity. The least
/// recent used entry in each shard would be evicted if the shard capacity is full.
///
/// Since this is a cache for specific usage, the key is defined as `usize`, and the size of each
/// entry is left to users to define. When inserting an entry, it also requires a charge argument
/// which usually would be the size of the entry value.
#[derive(Debug)]
pub struct Cache<T> {
    shards: Vec<LRUCache<T>>,
    pub capacity: usize, // TODO make struct field private and provide get method instead. Same as LRUCache.
    pub num_shard_bits: u8,
}

impl<T> Cache<T> {
    /// Construct a new `Cache`.
    pub fn new(capacity: usize) -> CacheBuilder<T> {
        CacheBuilder::new(capacity)
    }

    /// Insert a key-value pair into the cache. Unlike normal cache, it also requires a `charge`
    /// argument which usually serves as the size of the pair. The remaining capacity of the cache
    /// would be `capacity - charge` after the insertion.
    pub fn insert(&mut self, key: usize, val: T, charge: usize) -> Option<T> {
        let idx = self.get_shard(key);
        self.shards[idx].insert(key, val, charge)
    }

    /// Lookup an entry in the cache with given key and turn mutable reference of its value if present.
    /// This will also update the entry to most recent used entry of the shard cache.
    pub fn lookup(&mut self, key: usize) -> Option<&mut T> {
        let idx = self.get_shard(key);
        self.shards[idx].lookup(key)
    }

    /// Remove a entry from the cache and return the value.
    pub fn erase(&mut self, key: usize) -> Option<T> {
        let idx = self.get_shard(key);
        self.shards[idx].erase(key)
    }

    /// Get total usage of the cache.
    pub fn get_usage(&self) -> usize {
        self.shards.iter().fold(0, |acc, shard| acc + shard.usage)
    }

    fn get_shard(&self, key: usize) -> usize {
        let mut hash = FnvHasher::default();
        key.hash(&mut hash);
        match self.num_shard_bits {
            0 => 0,
            n => (hash.finish() >> (64 - n)) as usize,
        }
    }
}

/// Builder of the `Cache`.
#[derive(Debug)]
pub struct CacheBuilder<T> {
    capacity: usize,
    num_shard_bits: Option<u8>,
    phantom: PhantomData<T>,
}

impl<T> CacheBuilder<T> {
    /// Construct a new `CacheBuilder`.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            num_shard_bits: None,
            phantom: PhantomData,
        }
    }

    /// Cache is sharded into 2^num_shard_bits shards by hash of key.
    pub fn num_shard_bits(mut self, num_shard_bits: u8) -> Result<Self, Error> {
        if num_shard_bits >= 20 {
            return Err(Error::TooManyShards);
        }
        self.num_shard_bits = Some(num_shard_bits);
        Ok(self)
    }

    /// Consume the instance and build the `Cache`. If num_shard_bits isn't provided, it will
    /// determine the shard number based on capacity with size of 512KB for each shard.
    pub fn build(self) -> Cache<T> {
        let num_shard_bits = self
            .num_shard_bits
            .unwrap_or(get_default_cache_shard_bits(self.capacity));
        let num_shards = 1 << num_shard_bits;
        let per_shard = (self.capacity + (num_shards - 1)) / num_shards;
        let mut shards = Vec::new();
        for _ in 0..num_shards {
            shards.push(LRUCache::new(per_shard));
        }
        Cache {
            shards,
            capacity: self.capacity,
            num_shard_bits,
        }
    }
}

fn get_default_cache_shard_bits(capacity: usize) -> u8 {
    let mut num_shard_bits = 0;
    let min_shard_size = 512 * 1024;
    let mut num_shards = capacity / min_shard_size;
    num_shards >>= 1;
    while num_shards > 0 {
        num_shard_bits += 1;
        if num_shard_bits >= 6 {
            return num_shard_bits;
        }
        num_shards >>= 1;
    }
    num_shard_bits
}

/// A LRU cache builds on top of the HashMap from standard library.
///
/// `LRUCache` uses `std::collections::HashMap` for storage. It provides `O(1)` performance on
/// `insert`, `get`, `remove_lru` and many other APIs.
///
/// All entries are linked inlined within the `LRUCache` without raw pointer manipulation, so it is
/// complete memory safe and doesn't suffer any undefined behavior. A linked list is used to record
/// the cache order, so the items themselves do not need to be moved when the order changes.
/// (This is important for speed if the items are large.)
#[derive(Debug, Clone)]
pub struct LRUCache<T> {
    /// The most-recently-used entry is at index `head`. The entries form a linked list, linked to
    /// each other by key within the `entries` map.
    entries: HashMap<usize, Entry<T>, FnvBuildHasher>,
    /// Index of the first entry. If the cache is empty, ignore this field.
    head: usize,
    /// Index of the last entry. If the cache is empty, ignore this field.
    tail: usize,
    /// Capacity of the cache.
    pub capacity: usize,
    /// Current memory space that has been allocated.
    pub usage: usize,
}

/// An entry in an LRUCache.
#[derive(Debug, Clone)]
struct Entry<T> {
    val: T,
    /// Index of the previous entry. If this entry is the head, ignore this field.
    prev: usize,
    /// Index of the next entry. If this entry is the tail, ignore this field.
    next: usize,
    /// The size of the entry, but it's free to be measured as any metric.
    charge: usize,
}

impl<T> LRUCache<T> {
    /// Create a new LRU cache that can hold `capacity` of memory.
    pub fn new(capacity: usize) -> Self {
        LRUCache {
            entries: HashMap::default(),
            head: 0,
            tail: 0,
            capacity,
            usage: 0,
        }
    }

    /// Lookup an entry in the list with given key and turn its value if present. This will move
    /// the entry to the head of the cache.
    pub fn lookup(&mut self, key: usize) -> Option<&mut T> {
        if self.entries.contains_key(&key) {
            self.touch_index(key);
        }
        self.entries.get_mut(&key).map(|e| &mut e.val)
    }

    /// Insert a given key in the cache. Return old value if the key is present.
    ///
    /// This item becomes the front (most-recently-used) item in the cache.  If the cache is full,
    /// the back (least-recently-used) item will be removed.
    pub fn insert(&mut self, key: usize, val: T, charge: usize) -> Option<T> {
        self.usage += charge;
        while self.usage > self.capacity {
            self.remove_lru();
        }

        let (old, old_charge) = match self.entries.entry(key) {
            MapEntry::Occupied(mut e) => {
                let old_val = e.insert(Entry {
                    val,
                    prev: e.get().prev,
                    next: e.get().next,
                    charge,
                });
                (Some(old_val.val), old_val.charge)
            }
            MapEntry::Vacant(e) => {
                e.insert(Entry {
                    val,
                    prev: 0,
                    next: 0,
                    charge,
                });
                (None, 0)
            }
        };
        self.push_front(key);

        self.usage += old_charge;
        while self.usage > self.capacity {
            self.remove_lru();
        }
        old
    }

    /// Remove a entry from the cache.
    pub fn erase(&mut self, key: usize) -> Option<T> {
        if self.entries.contains_key(&key) {
            self.evict(key);
            self.entries.remove(&key).map(|old_entry| {
                self.usage -= old_entry.charge;
                old_entry.val
            })
        } else {
            None
        }
    }

    /// Remove the last entry from the cache.
    pub fn remove_lru(&mut self) -> Option<(usize, T)> {
        self.entries.remove(&self.tail).map(|old_tail| {
            let old_key = self.tail;
            let new_tail = old_tail.prev;
            self.tail = new_tail;
            self.usage -= old_tail.charge;
            (old_key, old_tail.val)
        })
    }

    /// Iterate over the contents of this cache.
    pub fn iter(&self) -> Iter<T> {
        Iter {
            pos: self.head,
            done: self.entries.is_empty(),
            cache: self,
        }
    }

    /// Returns the number of elements in the cache.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Touch a given entry, putting it first in the list.
    #[inline]
    fn touch_index(&mut self, idx: usize) {
        if idx != self.head {
            self.evict(idx);
            self.push_front(idx);
        }
    }

    /// Evict an entry from the linked list.
    /// Note this doesn't remove the entry from the cache.
    fn evict(&mut self, i: usize) {
        let evicted = self.entries.get(&i).expect("Invalid entry access");
        let prev = evicted.prev;
        let next = evicted.next;

        if i == self.head {
            self.head = next;
        } else {
            self.entries
                .get_mut(&prev)
                .expect("Invalid entry access")
                .next = next;
        }

        if i == self.tail {
            self.tail = prev;
        } else {
            self.entries
                .get_mut(&next)
                .expect("Invalid entry access")
                .prev = prev;
        }
    }

    /// Insert a new entry at the head of the list.
    fn push_front(&mut self, i: usize) {
        if self.entries.len() == 1 {
            self.tail = i;
        } else {
            self.entries.get_mut(&i).expect("Invalid entry access").next = self.head;
            self.entries
                .get_mut(&self.head)
                .expect("Invalid entry access")
                .prev = i;
        }
        self.head = i;
    }
}

/// Mutable iterator over values in an LRUCache, from most-recently-used to least-recently-used.
#[derive(Debug)]
pub struct Iter<'a, T> {
    cache: &'a LRUCache<T>,
    pos: usize,
    done: bool,
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: 'a,
{
    type Item = (usize, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        // Use a raw pointer because the compiler doesn't know that subsequent calls can't alias.
        //let entry = unsafe { &mut *(&mut self.cache.entries[self.pos as usize] as *mut Entry<T>) };
        let (key, entry) = self
            .cache
            .entries
            .get_key_value(&self.pos)
            .expect("Invalid entry access");

        if self.pos == self.cache.tail {
            self.done = true;
        }
        self.pos = entry.next;

        Some((*key, &entry.val))
    }
}
