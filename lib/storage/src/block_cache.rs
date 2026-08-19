use bytes::Bytes;
use object_store::path::Path as OSPath;
use quick_cache::sync::Cache;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Key identifying a block in a file by path and block index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub path: OSPath,
    pub block_index: u64,
}

/// A block cache that stores fixed-size byte chunks for Parquet file reads.
#[derive(Clone)]
pub struct BlockCache {
    block_size: u64,
    cache: Arc<Cache<BlockKey, Bytes>>,
}

impl Debug for BlockCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCache")
            .field("block_size", &self.block_size)
            .field("num_cached_blocks", &self.cache.len())
            .finish()
    }
}

impl BlockCache {
    /// Creates a new [`BlockCache`] with the specified block size and maximum number of blocks.
    pub fn new(block_size: usize, num_blocks: usize) -> Self {
        Self {
            block_size: block_size as u64,
            cache: Arc::new(Cache::new(num_blocks)),
        }
    }

    /// Returns the block size in bytes.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// Gets a cached block if present.
    pub fn get(&self, path: &OSPath, block_index: u64) -> Option<Bytes> {
        self.cache.get(&BlockKey {
            path: path.clone(),
            block_index,
        })
    }

    /// Inserts a block into the cache.
    pub fn insert(&self, path: OSPath, block_index: u64, bytes: Bytes) {
        self.cache.insert(BlockKey { path, block_index }, bytes);
    }
}
