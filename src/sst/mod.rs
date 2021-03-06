//! SST Table is a sorted map from strings to strings. Tables are immutable and persistent.
//!
//! It can be safely accessed from multiple threads without external synchronization.
//! A Table contains one or more data blocks, an optional filter block, a metaindex
//! block, an index block and a table footer. **All fixed-length integers are little-endian.**
//! Table data structure looks like this:
//!
//! ```text
//!                                              + optional
//!                                             /
//! +--------------+-----+--------------+------+-------+-----------------+-------------+--------+
//! | data block 1 | ... | data block n | filter block | metaindex block | index block | footer |
//! +--------------+-----+--------------+--------------+-----------------+-------------+--------+
//! ```
//!
//! ## Block Trailer
//!
//! Each block is followed by a 5 bytes trailer contains compression type and checksum.
//! The checksum is a CRC-32 computed using Castagnoli's polynomial. Compression type also included in the checksum.
//!
//! ```text
//! +---------------------------+-------------------+
//! | compression type (1-byte) | checksum (4-byte) |
//! +---------------------------+-------------------+
//! ```
//!
//! # Data Block
//!
//! A Data `Block` is an immutable ordered list of key/value entries followed by a list of restart point offsets,
//! terminated by a fixed u32 num_restarts. Each Block entry shares key prefix with its preceding
//! key until a restart point reached. A block should contains at least one restart point. First restart point are always zero.
//! Block data structure looks like this:
//!
//! ```text
//!   + restart point                 + restart point (depends on restart interval)
//!  /                               /
//! +---------------+---------------+-----+---------------+------------------+---------------+
//! | block entry 1 | block entry 2 | ... | block entry n | restarts trailer | block trailer |
//! +---------------+---------------+-----+---------------+------------------+---------------+
//! ```
//!
//! ## Block (key/value) entry
//!
//! ```text
//!               +---- key len ----+
//!              /                   \
//!     +-------+---------+-----------+---------+--------------------+-----+-------+
//!     | shared (varint) | not shared (varint) | value len (varint) | key | value |
//!     +-----------------+---------------------+--------------------+-----+-------+
//! ```
//!
//! ## Restarts trailer
//!
//! ```text
//!   +-- 4-bytes --+
//!  /               \
//! +-----------------+------+-----------------+------------------------------+
//! | restart point 1 | .... | restart point n | restart points len (4-bytes) |
//! +-----------------+------+-----------------+------------------------------+
//! ```
//!
//! Here's an example of how each entry shares key prefix with previous key. Say we have a data
//! block with `restart_interval = 2`. If we have three key/value as follow:
//!
//! - Entry one  : key="deck", value="v1"
//! - Entry two  : key="dock", value="v2"
//! - Entry three: key="duck", value="v3"
//!
//! The entries will be encoded as follow:
//!
//! ```text
//!   + restart point (offset=0)                      + restart point (offset=16)
//!  /                                               /
//! +---+---+---+--------+------+---+---+---+-------+------+---+---+---+--------+------+
//! | 0 | 4 | 2 | "deck" | "v1" | 1 | 3 | 2 | "ock" | "v2" | 0 | 4 | 2 | "duck" | "v3" |
//! +---+---+---+--------+------+---+---+---+-------+------+---+---+---+--------+------+
//!  \                           \                   \
//!   + entry one                 + entry two         + entry three
//!
//! The restart trailer will contains two restart points:
//!
//! +------------+-----------+--------+
//! |     0      |    16     |   2    |
//! +------------+-----------+---+----+
//!  \                      /     \
//!   +-- restart points --+       + restart points length
//! ```
//!
//! # Filter block
//!
//! Filter block is an optional block contains one or more filter data generated by a filter generator
//! and a filter block trailer. The trailer contains filter data offset, trailer offset, and 1-byte
//! lg(base). Filter block data structure looks like this:
//!
//! ```text
//!   + offset 1            + offset n      + trailer offset
//!  /                     /               /
//! +---------------+-----+---------------+---------+
//! | filter data 1 | ... | filter data n | trailer |
//! +---------------+-----+---------------+---------+
//! ```
//!
//! ## Filter trailer
//!
//! ```text
//!   +- 4-bytes -+
//!  /             \
//! +---------------+------+---------------+--------------------------+-------------------+
//! | data 1 offset | .... | data n offset | trailer offset (4-bytes) | lg(base) (1-byte) |
//! +---------------+------+---------------+--------------------------+-------------------+
//! ```
//!
//! # Metaindex block
//!
//! Metaindex block is a special block used to keep parameters of the table, usch as filter block
//! name and its block handle.
//!
//! ```text
//! +-------------+---------------------+
//! |     key     |        value        |
//! +-------------+---------------------+
//! | filter name | filter block handle |
//! +-------------+---------------------+
//! ```
//!
//! # Index block
//!
//! Index block is a special block used to keep record of data blocks offset and length.
//! The block restart interval of index block is one. The key used by index block are shortest
//! separator between the last key and the next key. If it's the last block, then it's the shortest
//! successor of the last key. The value of the index block is the block handle of the data block
//! which contains its offset and size.
//!
//! ```text
//! +---------------+--------------+
//! |      key      |    value     |
//! +---------------+--------------+
//! | separator key | block handle |
//! +---------------+--------------+
//! ```
//!
//! # Table footer
//!
//! Footer contains the BlockHandle of the metaindex and index blocks as well as a magic number.
//!
//! ```text
//!   +------------------- 40-bytes ----------------------+
//!  /                                                     \
//! +------------------------+--------------------+---------+-----------------+
//! | metaindex block handle | index block handle | padding | magic (8-bytes) |
//! +------------------------+--------------------+---------+-----------------+
//! ```

mod block;
mod table;

pub use block::{Block, BlockBuilder, BlockIter, FilterBlockBuilder, FilterBlockReader};
pub use table::TableBuilder;
