use std::cmp::{min, Ordering};
use std::iter::Iterator;
use std::mem::size_of;

use integer_encoding::{VarInt, VarIntWriter};

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
