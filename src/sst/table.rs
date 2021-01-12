use std::cmp::Ordering;
use std::io::Write;
use std::mem::swap;

use crc32fast::Hasher;
use integer_encoding::{VarInt, VarIntWriter};

use crate::sst::BlockBuilder;
use crate::Error;

/// Builder to construct new SST Table and save to .sst file. It groups entries into blocks,
/// calculacalculating checksum, and bloom filters.
/// Data blocks, metaindex block, and index block are built by `Block` module.
pub struct TableBuilder<T: Write> {
    file: T,
    /// Length of written data
    offset: usize,
    /// Number of key/value entry in the file
    num_entries: usize,
    /// Size of each data block
    block_size: usize,
    /// Interval of block restart point
    block_restart_interval: usize,
    /// Previous added key
    last_key: Vec<u8>,

    data_block: BlockBuilder,
    index_block: BlockBuilder,
}

impl<T: Write> TableBuilder<T> {
    /// Create a new TableBuilder.
    pub fn new(file: T, block_restart_interval: usize, block_size: usize) -> Self {
        Self {
            file,
            offset: 0,
            num_entries: 0,
            block_size,
            block_restart_interval,
            last_key: Vec::new(),
            data_block: BlockBuilder::new(block_restart_interval),
            index_block: BlockBuilder::new(1),
        }
    }

    /// Add a key/value pair to the table. The key must be greate than the previous one.
    /// If the data block reaches the limit, it will write and flush to the file. It also add a
    /// entry index to the index block.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if !self.last_key.is_empty() {
            return Err(Error::InvalidKey);
        }

        // TODO write filter block

        self.num_entries += 1;
        self.data_block.add(key, value)?;

        if self.data_block.size_estimate() > self.block_size {
            self.write_data_block(key)?;
        }

        Ok(())
    }

    /// Write the data block to the file and add a index entry to the index block where `key` is
    /// the first key of the next data block.
    fn write_data_block(&mut self, key: &[u8]) -> Result<(), Error> {
        // Find shortest Seperator
        let separator = find_separator(self.data_block.last_key(), key);
        self.last_key = Vec::from(self.data_block.last_key());

        let mut data_block = BlockBuilder::new(self.block_restart_interval);
        swap(&mut self.data_block, &mut data_block);
        let block_contents = data_block.finish();

        let handle = self.write_block(block_contents)?;
        let handle_encoding = handle.encode();

        self.index_block.add(&separator, &handle_encoding)?;
        // TODO filter block
        Ok(())
    }

    /// Perform compression, append the checksum, write the block to the file, and update the current offset.
    fn write_block(&mut self, data: Vec<u8>) -> Result<BlockHandle, Error> {
        const MASK_DELTA: u32 = 0xa282ead8;
        const COMPRESS_LEN: usize = 1;
        const CHECKSUM_LEN: usize = 4;
        // TODO compression
        let mut hasher = Hasher::new();
        hasher.update(&data);
        hasher.update(&[0]);
        let crc = hasher.finalize();
        let maskcrc = ((crc >> 15) | (crc << 17)).wrapping_add(MASK_DELTA);

        self.file.write(&data)?;
        self.file.write(&[0])?;
        self.file.write(&maskcrc.to_le_bytes())?;

        let handle = BlockHandle::new(self.offset, data.len());
        self.offset += data.len() + COMPRESS_LEN + CHECKSUM_LEN;

        Ok(handle)
    }

    /// Finish building the table and close the file.
    pub fn finish(mut self) -> Result<usize, Error> {
        // Write the rest data block
        if self.data_block.last_key().is_empty() {
            let key = find_successor(self.data_block.last_key());
            self.write_data_block(&key)?;
        }

        // Create metaindex block
        let metaindex_block = BlockBuilder::new(self.block_restart_interval);

        // TODO filter block

        // Write metaindex block
        let metaindex = metaindex_block.finish();
        let metaindex_handle = self.write_block(metaindex)?;

        // Write index block
        let mut index_block = BlockBuilder::new(1);
        swap(&mut index_block, &mut self.index_block);
        let index_block = index_block.finish();
        let index_handle = self.write_block(index_block)?;

        // Write footer
        let footer = Footer::new(metaindex_handle, index_handle);
        let buffer = footer.encode();

        self.offset += self.file.write(&buffer)?;
        self.file.flush()?;

        Ok(self.offset)
    }
}

/// BlockHandle is a pointer to the extent of a file that stores a data
/// block or a meta block.
#[derive(Debug, Clone)]
struct BlockHandle {
    offset: usize,
    size: usize,
}

impl BlockHandle {
    fn new(offset: usize, size: usize) -> Self {
        Self { offset, size }
    }

    /// Create varint encoded offset and size
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode_to(&mut v);
        v
    }

    /// Append varint encoded offset and size to `dst`
    fn encode_to(&self, to: &mut Vec<u8>) {
        to.write_varint(self.offset)
            .expect("Fail to write block handle offset.");
        to.write_varint(self.size)
            .expect("Fail to write block handle size.");
    }

    /// Decode a BlockHandle from bytes. Return the handle and the size read from bytes.
    fn decode(from: &[u8]) -> (Self, usize) {
        let (offset, offlen) = usize::decode_var(from).expect("Handle offset is corrupted.");
        let (size, sizelen) =
            usize::decode_var(&from[offlen..]).expect("Handle size is corrupted.");

        (Self { offset, size }, offlen + sizelen)
    }
}

const FOOTER_LENGTH: usize = 45;
const FOOTER_MAGIC: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];

#[derive(Debug)]
struct Footer {
    metaindex: BlockHandle,
    index: BlockHandle,
}

impl Footer {
    fn new(metaindex: BlockHandle, index: BlockHandle) -> Self {
        Self { metaindex, index }
    }

    /// Decode a Footer from bytes. Return the handle and the size read from bytes.
    fn decode(from: &[u8]) -> (Self, usize) {
        assert!(from.len() == FOOTER_LENGTH);
        assert_eq!(&from[FOOTER_LENGTH - 8..], &FOOTER_MAGIC);
        let (metaindex, i) = BlockHandle::decode(from);
        let (index, j) = BlockHandle::decode(&from[i..]);

        (Self { metaindex, index }, i + j)
    }

    /// Encode footer and return the bytes.
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.metaindex.encode_to(&mut v);
        self.index.encode_to(&mut v);

        v.resize(FOOTER_LENGTH - 8, 0);
        v.extend_from_slice(&FOOTER_MAGIC);
        v
    }
}

// TODO Maybe move to utils
/// Find the separator which is greater than `a` but shorter than `b`. It will assign to a's
/// reference.
fn find_separator(a: &[u8], b: &[u8]) -> Vec<u8> {
    // Find length of common prefix
    let min = a.len().min(b.len());
    let mut diff_index = 0;
    while diff_index < min && a[diff_index] == b[diff_index] {
        diff_index += 1;
    }

    if diff_index < min {
        let diff_byte = a[diff_index];
        if diff_byte < 0xff && diff_byte + 1 < b[diff_index] {
            let mut separator = vec![0; diff_index + 1];
            separator[0..=diff_index].copy_from_slice(&a[0..=diff_index]);
            separator[diff_index] += 1;
            assert_eq!(separator.as_slice().cmp(b), Ordering::Less);
            return separator;
        }
    }

    a.to_vec()
}

/// Find the successor key that is one bit greater than provided bytes. If it's maximum key, return
/// itself.
fn find_successor(a: &[u8]) -> Vec<u8> {
    let mut result = a.to_vec();
    // Find first byte that can be incremented.
    for i in 0..a.len() {
        if a[i] != 0xff {
            result[i] += 1;
            result.resize(i + 1, 0);
            return result;
        }
    }
    result
}
