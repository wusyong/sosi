use std::cmp::{min, Ordering};
use std::iter::Iterator;
use std::mem::size_of;

use integer_encoding::{VarInt, VarIntWriter};

use crate::bloom::BloomFilterPolicy;
use crate::Error;

const U32_SIZE: usize = size_of::<u32>();

pub type BlockContents = Vec<u8>;

/// A Data `Block` is an immutable ordered list of key/value entries followed by a list of restart point offsets,
/// terminated by a fixed u32 num_restarts. Each Block entry shares key prefix with its preceding
/// key until a restart point reached. A block should contains at least one restart point.
/// First restart point are always zero.
///
/// For more information, see the [module level](crate::table) documentation.
#[derive(Debug, Clone)]
pub struct Block {
    /// Underlying block contents.
    data: Vec<u8>,
    /// Offset of restart array (list of fixed u32).
    restarts: usize,
    /// Number of entries in restart array.
    num_restarts: usize,
}

impl Block {
    // TODO add another vesion with checks?
    /// Create a Black from raw bytes.
    pub fn new(content: BlockContents) -> Result<Self, Error> {
        let len = content.len();
        if len < U32_SIZE {
            return Err(Error::InvalidBlock);
        }

        let idx = len - U32_SIZE;
        let bytes = [
            content[idx],
            content[idx + 1],
            content[idx + 2],
            content[idx + 3],
        ];
        let num_restarts = u32::from_le_bytes(bytes) as usize;
        let max_restarts = (len - U32_SIZE) / U32_SIZE;

        if num_restarts as usize <= max_restarts {
            return Ok(Self {
                data: content,
                num_restarts,
                restarts: len - (1 + num_restarts) * U32_SIZE,
            });
        }
        Err(Error::InvalidBlock)
    }

    /// Returns an iterator over Block.
    pub fn iter(&self) -> BlockIter {
        BlockIter {
            block: &self,
            current: 0,
            prev: 0,
            restart_index: 0,
            key: Vec::new(),
            value: &[],
        }
    }
}

pub struct BlockBuilder {
    block_restart_interval: usize,
    /// Destination buffer
    buffer: Vec<u8>,
    /// Restart points
    restarts: Vec<u32>,
    /// Number of entries emitted since restart
    counter: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(block_restart_interval: usize) -> Self {
        assert!(block_restart_interval > 0);
        Self {
            block_restart_interval,
            buffer: vec![],
            restarts: vec![0],
            counter: 0,
            last_key: vec![],
        }
    }

    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    pub fn size_estimate(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + U32_SIZE
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if !self.buffer.is_empty() && key.cmp(&self.last_key) != Ordering::Greater {
            return Err(Error::InvalidKey);
        }

        let mut shared = 0;
        if self.counter < self.block_restart_interval {
            // See how much sharing to do with previous string
            let min_len = min(self.last_key.len(), key.len());
            while shared < min_len && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            // Restart compression
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
        }
        let non_shared = key.len() - shared;

        // Add |SHARED|NON_SHARED|VALUE_LEN| to buffer
        self.buffer
            .write_varint(shared)
            .expect("Fail to write SHARED");
        self.buffer
            .write_varint(non_shared)
            .expect("Fail to write NON_SHARED");
        self.buffer
            .write_varint(value.len())
            .expect("Fail to write VALUE_LEN");
        // Add key delta to buffer followed by value
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(value);
        // Update state
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;

        Ok(())
    }

    pub fn finish(mut self) -> BlockContents {
        self.buffer.reserve(self.restarts.len() * 4 + 4);

        // Add RESTARTS
        let restarts_len = self.restarts.len() as u32;
        for r in self.restarts {
            self.buffer.extend_from_slice(&r.to_le_bytes());
        }
        // Add N_RESTARTS
        self.buffer.extend_from_slice(&restarts_len.to_le_bytes());

        self.buffer
    }
}

/// Block iterator is created by [`iter`](Block::iter) method on [`Block`](Block).
/// It's an double-ended iterator which also provides more methods to seek certain entry.
pub struct BlockIter<'a> {
    block: &'a Block,
    /// Offset of current entry in `data`.
    current: usize,
    prev: usize,
    /// Index of restart block in which current_offset falls.
    restart_index: usize,
    key: Vec<u8>,
    value: &'a [u8],
}

impl<'a> BlockIter<'a> {
    /// Returns the key of current entry.
    /// Usually called after `next` method if needed.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Seek the first entry in block with key>= target and returns its value.
    pub fn seek(&mut self, target: &[u8]) -> Option<&[u8]> {
        let data = &self.block.data;
        let mut left = 0;
        let mut right = self.block.num_restarts - 1;
        // Binary search in restart array to find the last restart point with key < target
        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.get_restart_point(mid);

            let mut i = 0;
            let (shared, len) =
                usize::decode_var(&data[self.current..]).expect("SHARED data is corrupted.");
            i += len;
            let (non_shared, len) = usize::decode_var(&data[self.current + i..])
                .expect("NON_SHARED data is corrupted.");
            i += len;
            let (_, len) =
                usize::decode_var(&data[self.current + i..]).expect("VALUE_LEN data is corrupted.");
            i += len;

            let key_offset = region_offset + i;
            let key_len = shared + non_shared;
            let key = &data[key_offset..key_offset + key_len];
            if let Ordering::Less = key.cmp(target) {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        // Linear search (within restart block) for first key >= target
        self.seek_to_restart_point(left);
        loop {
            if !self.valid() {
                return Some(self.value);
            }

            if let Ordering::Less = self.key.as_slice().cmp(target) {
                self.parse_entry();
            } else {
                return Some(self.value);
            }
        }
    }

    /// Seek to first entry in the block.
    pub fn seek_to_first(&mut self) -> &[u8] {
        self.seek_to_restart_point(0);
        self.value
    }

    /// Seek to last entry in the block.
    pub fn seek_to_last(&mut self) -> &[u8] {
        self.seek_to_restart_point(self.block.num_restarts - 1);
        while self.valid() {
            self.parse_entry();
        }
        self.value
    }

    #[inline]
    fn valid(&self) -> bool {
        self.current < self.block.restarts
    }

    /// Returns key and value from the current entry position.
    /// This will also move self.current to the beginning of the next entry.
    #[inline]
    fn parse_entry(&mut self) {
        let mut i = 0;
        let data = &self.block.data;
        let (shared, len) =
            usize::decode_var(&data[self.current..]).expect("SHARED data is corrupted.");
        i += len;
        let (non_shared, len) =
            usize::decode_var(&data[self.current + i..]).expect("NON_SHARED data is corrupted.");
        i += len;
        let (value_len, len) =
            usize::decode_var(&data[self.current + i..]).expect("VALUE_LEN data is corrupted.");
        i += len;
        // TODO add restart_offset check

        // Parse the key
        let key_offset = self.current + i;
        let key_len = shared + non_shared;
        self.key.resize(key_len, 0);
        let data = &self.block.data[key_offset..key_offset + non_shared];
        for i in shared..key_len {
            self.key[i] = data[i - shared];
        }

        // Parse the value
        let value_offset = self.current + i + non_shared;
        self.value = &self.block.data[value_offset..value_offset + value_len];

        // Move current offset to the next entry
        self.prev = self.current;
        self.current = value_offset + value_len;
    }

    #[inline]
    fn get_restart_point(&self, index: usize) -> usize {
        let offset = self.block.restarts + index * U32_SIZE;
        let data = &self.block.data;
        u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize
    }

    #[inline]
    fn seek_to_restart_point(&mut self, index: usize) {
        self.key.clear();
        self.restart_index = index;
        self.current = self.get_restart_point(index);
        self.parse_entry();
    }
}

/// Returns the value of next entry when calling `next`.
impl<'a> Iterator for BlockIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if !self.valid() {
            return None;
        }

        // Decode next entry
        self.parse_entry();

        // Update restart_index
        while self.restart_index + 1 < self.block.num_restarts
            && self.get_restart_point(self.restart_index + 1) < self.current
        {
            self.restart_index += 1;
        }

        Some(self.value)
    }
}

/// Returns the value of previous entry when calling `next_back`
/// TODO just name it prev
impl<'a> DoubleEndedIterator for BlockIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let original = self.prev;
        // Find the restart point that is before current offset.
        while self.get_restart_point(self.restart_index) >= original {
            if self.restart_index == 0 {
                // No more entry.
                return None;
            }
            self.restart_index -= 1;
        }

        self.seek_to_restart_point(self.restart_index);

        // Parse the entry until it parses the original block entry.
        while self.current < original {
            self.parse_entry();
        }

        Some(self.value)
    }
}

const DEFAULT_BITS_PER_KEY: usize = 10;
const FILTER_BASE_LOG: usize = 11;

pub struct FilterBlockBuilder<'a> {
    policy: BloomFilterPolicy,
    keys: Vec<&'a [u8]>,
    /// Filter block data
    data: Vec<u8>,
    /// Offset of every filter data
    filter_offsets: Vec<usize>,
}

impl<'a> FilterBlockBuilder<'a> {
    pub fn new() -> Self {
        FilterBlockBuilder {
            policy: BloomFilterPolicy::new(DEFAULT_BITS_PER_KEY),
            keys: Vec::new(),
            data: Vec::new(),
            filter_offsets: Vec::new(),
        }
    }

    pub fn add_key(&mut self, key: &'a [u8]) {
        self.keys.push(key);
    }

    /// Create filter data for the data block with given `block_offset`
    pub fn start_block(&mut self, block_offset: usize) {
        // Filter index handles data range from [i*FILTER_BASE..(i+1)*FILTER_BASE]
        let filter_index = block_offset as usize >> FILTER_BASE_LOG;
        assert!(filter_index >= self.filter_offsets.len());

        while filter_index > self.filter_offsets.len() {
            self.generate_filter();
        }
    }

    /// Append filter block trailer and return the filter block in bytes.
    pub fn finish(mut self) -> Vec<u8> {
        // Consume remaining keys
        if !self.keys.is_empty() {
            self.generate_filter();
        }

        // Append array of per-filter offsets
        let array_offset = self.data.len() as u32;
        for offset in self.filter_offsets {
            self.data.extend_from_slice(&(offset as u32).to_le_bytes());
        }

        // Append filter trailer offset
        self.data
            .extend_from_slice(&(array_offset as u32).to_le_bytes());

        // Append base lg
        self.data.push(FILTER_BASE_LOG as u8);

        self.data
    }

    /// Convert `keys` to encoded filter vector
    fn generate_filter(&mut self) {
        self.filter_offsets.push(self.data.len());
        if self.keys.is_empty() {
            return;
        }

        let filter = self.policy.create_filter(&self.keys);
        self.data.extend_from_slice(&filter);
        self.keys.clear();
    }
}

#[derive(Debug, Clone)]
pub struct FilterBlockReader {
    policy: BloomFilterPolicy,
    data: Vec<u8>,
    /// Beginning of the offset array
    offset: usize,
    /// Number of entries in offset array
    num: usize,
    base_lg: u8,
}

impl FilterBlockReader {
    pub fn new(data: Vec<u8>) -> Self {
        let n = data.len();
        // 1 byte for base_lg_ and 4 for start of offset array
        assert!(n >= 5);
        let base_lg = data[n - 1];
        let offset =
            u32::from_le_bytes([data[n - 5], data[n - 4], data[n - 3], data[n - 2]]) as usize;
        assert!(offset <= n - 5);
        let num = (n - 5 - offset) / 4;

        Self {
            policy: BloomFilterPolicy::new(DEFAULT_BITS_PER_KEY),
            data,
            offset,
            num,
            base_lg,
        }
    }

    pub fn key_may_match(&self, block_offset: usize, key: &[u8]) -> bool {
        let index = block_offset >> self.base_lg;
        if index < self.num {
            let offset = self.offset + index * 4;
            let start = u32::from_le_bytes([
                self.data[offset],
                self.data[offset + 1],
                self.data[offset + 2],
                self.data[offset + 3],
            ]) as usize;
            let end = u32::from_le_bytes([
                self.data[offset + 4],
                self.data[offset + 5],
                self.data[offset + 6],
                self.data[offset + 7],
            ]) as usize;

            assert!(start <= end);
            assert!(end <= self.offset);

            return self.policy.key_may_match(key, &self.data[start..end]);
        }

        true // Errors are treated as potential matches.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Block tests

    const BLOCK_SAMPLE: [&[u8]; 6] = [
        b"key111",
        b"key112",
        b"key123",
        b"prefix_key1",
        b"prefix_key2",
        b"prefix_key3",
    ];

    #[test]
    fn block_builder() {
        let mut builder = BlockBuilder::new(3);
        for i in BLOCK_SAMPLE.iter() {
            builder.add(i, i).unwrap();
            assert!(builder.counter <= 3);
            assert!(&builder.last_key == i);
        }

        assert_eq!(103, builder.size_estimate());

        let block = Block::new(builder.finish()).unwrap();
        assert_eq!(block.data.len(), 103);
        assert_eq!(block.restarts, 91);
    }

    #[test]
    fn builder_with_inconsist_key() {
        let mut builder = BlockBuilder::new(3);
        for i in BLOCK_SAMPLE.iter() {
            builder.add(i, i).unwrap();
            assert!(builder.counter <= 3);
            assert!(&builder.last_key == i);
        }
        let builder = builder.add(b"key0", b"key0");
        assert!(builder.is_err());
    }

    #[test]
    fn corrupted_block() {
        let block = Block::new(vec![0, 0, 0]);
        assert!(block.is_err());

        let mut data = vec![];
        let restart_offset = vec![0u32, 10, 20];
        for i in restart_offset.into_iter() {
            data.extend_from_slice(&i.to_le_bytes());
        }
        // Append wrong NUM_RESTARTS
        data.extend_from_slice(&4u32.to_le_bytes());
        let block = Block::new(data);
        assert!(block.is_err());
    }

    #[test]
    fn empty_block() {
        let block = Block::new(BlockBuilder::new(2).finish()).unwrap();
        let len = block.data.len();
        let num_restarts = block.num_restarts;
        let restarts = &block.data[block.restarts..len - 4];
        assert_eq!(num_restarts, 1);
        assert_eq!(restarts.len() / 4, 1);
        assert_eq!(restarts, &[0, 0, 0, 0]);
        let mut iter = block.iter();
        assert!(iter.next().is_none());
    }

    #[test]
    fn empty_key_entry() {
        let mut block = BlockBuilder::new(2);
        block.add(b"", b"value").unwrap();
        let block = Block::new(block.finish()).unwrap();
        let mut iter = block.iter();
        assert_eq!(iter.seek(b""), Some(b"value".as_ref()));
        assert_eq!(iter.key(), b"");
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn block_iter_seek() {
        let mut builder = BlockBuilder::new(3);
        for (idx, key) in BLOCK_SAMPLE.iter().enumerate() {
            builder.add(key, [idx as u8].as_ref()).unwrap();
        }
        let block = Block::new(builder.finish()).unwrap();
        let mut iter = block.iter();

        assert_eq!(iter.seek_to_last(), [5].as_ref());
        assert_eq!(iter.key(), b"prefix_key3");

        assert_eq!(iter.next(), None);
        // The current entry is still the last entry.
        assert_eq!(iter.key(), b"prefix_key3");

        assert_eq!(iter.next_back(), Some([4].as_ref()));
        assert_eq!(iter.key(), b"prefix_key2");

        assert_eq!(iter.seek_to_first(), [0].as_ref());
        assert_eq!(iter.key(), b"key111");

        assert_eq!(iter.next_back(), None);
        // The current entry is still the first entry.
        assert_eq!(iter.key(), b"key111");

        assert_eq!(iter.seek(b"key112"), Some([1].as_ref()));
        assert_eq!(iter.key(), b"key112");

        assert_eq!(iter.next_back(), Some([0].as_ref()));
        assert_eq!(iter.key(), b"key111");

        assert_eq!(iter.seek(b"key123"), Some([2].as_ref()));
        assert_eq!(iter.key(), b"key123");

        assert_eq!(iter.next(), Some([3].as_ref()));
        assert_eq!(iter.key(), b"prefix_key1");
    }

    // Filter block tests

    #[test]
    fn empty_filter_builder() {
        let builder = FilterBlockBuilder::new().finish();
        assert_eq!(vec![0, 0, 0, 0, FILTER_BASE_LOG as u8], builder);

        let reader = FilterBlockReader::new(builder);
        assert!(reader.key_may_match(0, "foo".as_bytes()));
        assert!(reader.key_may_match(10000, "foo".as_bytes()));
    }

    #[test]
    fn single_chunk_filter() {
        let mut builder = FilterBlockBuilder::new();
        builder.start_block(100);
        builder.add_key("foo".as_bytes());
        builder.add_key("bar".as_bytes());
        builder.add_key("box".as_bytes());
        builder.start_block(200);
        builder.add_key("box".as_bytes());
        builder.start_block(300);
        builder.add_key("hello".as_bytes());
        let block = FilterBlockReader::new(builder.finish());
        assert!(block.key_may_match(100, "foo".as_bytes()));
        assert!(block.key_may_match(100, "bar".as_bytes()));
        assert!(block.key_may_match(100, "box".as_bytes()));
        assert!(block.key_may_match(100, "hello".as_bytes()));
        assert!(block.key_may_match(100, "foo".as_bytes()));
        assert!(!block.key_may_match(100, "missing".as_bytes())); // No match
        assert!(!block.key_may_match(100, "other".as_bytes()));
    }

    #[test]
    fn multiple_chunk_filter() {
        let mut builder = FilterBlockBuilder::new();
        // first filter
        builder.start_block(0);
        builder.add_key("foo".as_bytes());
        builder.start_block(2000);
        builder.add_key("bar".as_bytes());
        // second filter
        builder.start_block(3100);
        builder.add_key("box".as_bytes());
        // third filter is empty
        // last filter
        builder.start_block(9000);
        builder.add_key("box".as_bytes());
        builder.add_key("hello".as_bytes());

        let block = FilterBlockReader::new(builder.finish());
        // check first filter
        assert_eq!(block.key_may_match(0, "foo".as_bytes()), true);
        assert_eq!(block.key_may_match(2000, "bar".as_bytes()), true);
        assert_eq!(block.key_may_match(0, "box".as_bytes()), false);
        assert_eq!(block.key_may_match(0, "hello".as_bytes()), false);
        // check second filter
        assert_eq!(block.key_may_match(3100, "foo".as_bytes()), false);
        assert_eq!(block.key_may_match(3100, "bar".as_bytes()), false);
        assert_eq!(block.key_may_match(3100, "box".as_bytes()), true);
        assert_eq!(block.key_may_match(3100, "hello".as_bytes()), false);
        // check third filter
        assert_eq!(block.key_may_match(4100, "foo".as_bytes()), false);
        assert_eq!(block.key_may_match(4100, "bar".as_bytes()), false);
        assert_eq!(block.key_may_match(4100, "box".as_bytes()), false);
        assert_eq!(block.key_may_match(4100, "hello".as_bytes()), false);
        // check last filter
        assert_eq!(block.key_may_match(9000, "foo".as_bytes()), false);
        assert_eq!(block.key_may_match(9000, "bar".as_bytes()), false);
        assert_eq!(block.key_may_match(9000, "box".as_bytes()), true);
        assert_eq!(block.key_may_match(9000, "hello".as_bytes()), true);
    }
}
