use std::iter::Iterator;
use std::mem::size_of;

use integer_encoding::VarInt;

use crate::Error;

const U32_SIZE: usize = size_of::<u32>();

/// A `Block` is an immutable ordered list of key/value ENTRIES followed by a list of RESTARTS,
/// terminated by a fixed u32 NUM_RESTARTS. Each Block entry shares key prefix with its preceding
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
    // TODO Use block builder instead.
    pub fn new(content: Vec<u8>) -> Result<Self, Error> {
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
        let max_restarts = (len - U32_SIZE) / 1;

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
            restart_index: 0,
            key: Vec::new(),
            value: &[],
        }
    }
}

/// Block iterator is created by [`iter`](Block::iter) method on [`Block`](Block).
/// It's an double-ended iterator which also provides more methods to seek certain entry.
pub struct BlockIter<'a> {
    block: &'a Block,
    /// Offset of current entry in `data`.
    current: usize,
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

    // TODO Seek

    #[inline]
    fn valid(&self) -> bool {
        self.current < self.block.restarts
    }

    /// Returns key and value from the current entry position.
    /// This will also move self.current to the beginning of the next entry.
    #[inline]
    fn parse_entry(&mut self) {
        let mut i = 0;
        let (shared, len) =
            usize::decode_var(&self.block.data[self.current..]).expect("SHARED data is corrupted.");
        i += len;
        let (non_shared, len) = usize::decode_var(&self.block.data[self.current + i..])
            .expect("NON_SHARED data is corrupted.");
        i += len;
        let (value_len, len) = usize::decode_var(&self.block.data[self.current + i..])
            .expect("VALUE_LEN data is corrupted.");
        i += len;

        // Parse the key
        let key_len = shared + non_shared;
        self.key.resize(key_len, 0);
        let data = &self.block.data[key_len..key_len + non_shared];
        for i in shared..key_len {
            self.key[i] = data[i - shared];
        }

        // Parse the value
        let value_offset = self.current + i + non_shared;
        self.value = &self.block.data[value_offset..value_offset + value_len];

        // Move cuurent offset to the next entry
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
    }
}

/// Returns the value of next entry when calling `next`.
impl<'a> Iterator for BlockIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if !self.valid() {
            self.current = self.block.restarts;
            self.restart_index = self.block.num_restarts;
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
impl<'a> DoubleEndedIterator for BlockIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let original = self.current;
        // Find the restart point that is before current offset.
        while self.get_restart_point(self.restart_index) >= original {
            if self.restart_index == 0 {
                // No more entry. Set to the first entry.
                self.current = 0;
                self.restart_index = 0;
                return None;
            }
            self.restart_index -= 1;
        }

        self.seek_to_restart_point(self.restart_index);

        // Parse the entry until it parses the original block entry.
        while self.current <= original {
            self.parse_entry();
        }

        Some(self.value)
    }
}
