use crate::block_cache::BlockCache;
use bytes::Bytes;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::{
    CachedParquetFileReaderFactory, ParquetFileReaderFactory,
};
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

/// Fetches [`ParquetMetaData`] through the [`FileMetadataCache`] of a session.
///
/// On a cache miss, the metadata is fetched from the object store and inserted into the cache.
pub struct ParquetMetadataFetcher {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<FileMetadataCache>,
    metadata_size_hint: Option<usize>,
}

/// Returns the maximum number of files that should be processed concurrently (e.g., when fetching
/// their metadata). This is DataFusion's `meta_fetch_concurrency`, which is also used by its
/// `ListingTable`.
pub fn max_concurrent_files(state: &SessionState) -> usize {
    state
        .config_options()
        .execution
        .meta_fetch_concurrency
        .get()
}

impl ParquetMetadataFetcher {
    /// Creates a new [`ParquetMetadataFetcher`] that uses the file metadata cache and the metadata
    /// size hint of the `state`.
    pub fn new(state: &SessionState, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            metadata_cache: state.runtime_env().cache_manager.get_file_metadata_cache(),
            metadata_size_hint: state.table_options().parquet.global.metadata_size_hint,
        }
    }

    /// Obtains the [`ParquetMetaData`] of a file.
    ///
    /// With [`PageIndexPolicy::Skip`], only the footer is fetched. The returned metadata may still
    /// contain the page index if it is already cached. With [`PageIndexPolicy::Optional`], the
    /// page index is loaded (if present in the file). If the cache only contains the footer, only
    /// the page index is fetched and the cache entry is updated.
    pub async fn fetch(
        &self,
        object_meta: &ObjectMeta,
        page_index_policy: PageIndexPolicy,
    ) -> DFResult<Arc<ParquetMetaData>> {
        DFParquetMetadata::new(self.store.as_ref(), object_meta)
            .with_file_metadata_cache(Some(Arc::clone(&self.metadata_cache)))
            .with_metadata_size_hint(self.metadata_size_hint)
            .with_page_index_policy(Some(page_index_policy))
            .fetch_metadata()
            .await
    }

    /// Obtains the footers (i.e., without the page index) of the given files, processing up to
    /// `max_concurrent_files` files concurrently. The footers are returned in the order of
    /// `object_metas`.
    pub async fn fetch_footers(
        &self,
        object_metas: &[ObjectMeta],
        max_concurrent_files: usize,
    ) -> DFResult<Vec<Arc<ParquetMetaData>>> {
        futures::stream::iter(object_metas.iter().cloned())
            .map(|object_meta| async move {
                self.fetch(&object_meta, PageIndexPolicy::Skip).await
            })
            .buffered(max_concurrent_files)
            .try_collect()
            .await
    }
}

/// An [`AsyncFileReader`] that serves the metadata from the inner reader (which uses DataFusion's
/// metadata cache), but routes the byte reads through an optional [`BlockCache`].
///
/// As Bloom filters are read with regular byte reads, they are also cached in the block cache.
struct BlockCachedReader {
    inner: Box<dyn AsyncFileReader + Send>,
    path: object_store::path::Path,
    file_size: u64,
    block_cache: Option<Arc<BlockCache>>,
}

impl AsyncFileReader for BlockCachedReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        if let Some(block_cache) = &self.block_cache {
            if range.start >= range.end {
                return Box::pin(async move { Ok(Bytes::new()) });
            }
            let block_size = block_cache.block_size();
            let start_block = range.start / block_size;
            let end_block = range.end.saturating_sub(1) / block_size;

            if start_block == end_block {
                let block_start = start_block * block_size;
                let block_end = (block_start + block_size).min(self.file_size);
                let slice_start = (range.start - block_start) as usize;
                let slice_end = (range.end - block_start) as usize;

                if let Some(block) = block_cache.get(&self.path, start_block) {
                    let slice = block.slice(slice_start..slice_end.min(block.len()));
                    return Box::pin(async move { Ok(slice) });
                }

                let path = self.path.clone();
                let block_cache = Arc::clone(block_cache);
                let inner_fut = self.inner.get_bytes(block_start..block_end);

                return Box::pin(async move {
                    let block = inner_fut.await?;
                    block_cache.insert(path, start_block, block.clone());
                    let slice = block.slice(slice_start..slice_end.min(block.len()));
                    Ok(slice)
                });
            }

            // Ranges spanning multiple blocks are handled by the multi-range implementation.
            let fut = self.get_byte_ranges(vec![range]);
            return Box::pin(async move {
                let mut bytes = fut.await?;
                Ok(bytes.pop().expect("One range requested"))
            });
        }

        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        let Some(block_cache) = &self.block_cache else {
            return self.inner.get_byte_ranges(ranges);
        };

        let block_size = block_cache.block_size();
        // The blocks required for the ranges are held locally, as other readers may evict them
        // from the cache while this request is in flight.
        let mut blocks: HashMap<u64, Bytes> = HashMap::new();
        let mut missing_blocks: HashMap<u64, Range<u64>> = HashMap::new();

        for range in &ranges {
            if range.start >= range.end {
                continue;
            }
            let start_block = range.start / block_size;
            let end_block = range.end.saturating_sub(1) / block_size;

            for block_idx in start_block..=end_block {
                if blocks.contains_key(&block_idx)
                    || missing_blocks.contains_key(&block_idx)
                {
                    continue;
                }
                match block_cache.get(&self.path, block_idx) {
                    Some(block) => {
                        blocks.insert(block_idx, block);
                    }
                    None => {
                        let block_start = block_idx * block_size;
                        let block_end = (block_start + block_size).min(self.file_size);
                        missing_blocks.insert(block_idx, block_start..block_end);
                    }
                }
            }
        }

        let assemble = move |blocks: &HashMap<u64, Bytes>| {
            ranges
                .iter()
                .map(|range| {
                    if range.start >= range.end {
                        Bytes::new()
                    } else {
                        assemble_range_from_blocks(blocks, range, block_size)
                    }
                })
                .collect::<Vec<_>>()
        };

        if missing_blocks.is_empty() {
            let bytes = assemble(&blocks);
            return Box::pin(async move { Ok(bytes) });
        }

        let block_cache = Arc::clone(block_cache);
        let path = self.path.clone();
        let block_indices: Vec<u64> = missing_blocks.keys().copied().collect();
        let fetch_ranges: Vec<Range<u64>> = missing_blocks.values().cloned().collect();
        let fut = self.inner.get_byte_ranges(fetch_ranges);

        Box::pin(async move {
            let fetched = fut.await?;
            for (block_idx, bytes) in block_indices.into_iter().zip(fetched) {
                block_cache.insert(path.clone(), block_idx, bytes.clone());
                blocks.insert(block_idx, bytes);
            }
            Ok(assemble(&blocks))
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
        self.inner.get_metadata(options)
    }
}

/// Assembles the given range from the given blocks. All blocks covering the range must be
/// present.
fn assemble_range_from_blocks(
    blocks: &HashMap<u64, Bytes>,
    range: &Range<u64>,
    block_size: u64,
) -> Bytes {
    let start_block = range.start / block_size;
    let end_block = range.end.saturating_sub(1) / block_size;

    if start_block == end_block {
        let block_start = start_block * block_size;
        let block = &blocks[&start_block];
        let slice_start = (range.start - block_start) as usize;
        let slice_end = (range.end - block_start) as usize;
        block.slice(slice_start..slice_end.min(block.len()))
    } else {
        let mut combined = Vec::with_capacity((range.end - range.start) as usize);
        for block_idx in start_block..=end_block {
            let block = &blocks[&block_idx];
            let block_start = block_idx * block_size;
            let slice_start = range.start.saturating_sub(block_start) as usize;
            let slice_end = if block_start + block_size > range.end {
                (range.end - block_start) as usize
            } else {
                block.len()
            };
            combined.extend_from_slice(&block[slice_start..slice_end.min(block.len())]);
        }
        Bytes::from(combined)
    }
}

/// A [`ParquetFileReaderFactory`] that uses DataFusion's [`CachedParquetFileReaderFactory`] (i.e.,
/// the footer and page index are served from the [`FileMetadataCache`]) and additionally caches
/// byte reads in an optional [`BlockCache`].
#[derive(Debug)]
pub struct BlockCachedReaderFactory {
    inner_factory: CachedParquetFileReaderFactory,
    block_cache: Option<Arc<BlockCache>>,
}

impl BlockCachedReaderFactory {
    /// Creates a new [`BlockCachedReaderFactory`].
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<FileMetadataCache>,
        block_cache: Option<Arc<BlockCache>>,
    ) -> Self {
        Self {
            inner_factory: CachedParquetFileReaderFactory::new(store, metadata_cache),
            block_cache,
        }
    }
}

impl ParquetFileReaderFactory for BlockCachedReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DFResult<Box<dyn AsyncFileReader + Send>> {
        let path = file.object_meta.location.clone();
        let file_size = file.object_meta.size;
        let inner = self.inner_factory.create_reader(
            partition_index,
            file,
            metadata_size_hint,
            metrics,
        )?;

        if self.block_cache.is_none() {
            return Ok(inner);
        }

        Ok(Box::new(BlockCachedReader {
            inner,
            path,
            file_size,
            block_cache: self.block_cache.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serves byte ranges from an in-memory buffer.
    struct InMemoryReader(Bytes);

    impl AsyncFileReader for InMemoryReader {
        fn get_bytes(
            &mut self,
            range: Range<u64>,
        ) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
            let bytes = self.0.slice(range.start as usize..range.end as usize);
            Box::pin(async move { Ok(bytes) })
        }

        fn get_metadata<'a>(
            &'a mut self,
            _options: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn block_cached_reader_survives_eviction() {
        let data = Bytes::from((0..=255u8).collect::<Vec<_>>());
        // A cache that holds a single block of 16 bytes, so blocks are evicted while a request
        // that spans multiple blocks is assembled.
        let mut reader = BlockCachedReader {
            inner: Box::new(InMemoryReader(data.clone())),
            path: object_store::path::Path::from("test.parquet"),
            file_size: data.len() as u64,
            block_cache: Some(Arc::new(BlockCache::new(16, 1))),
        };

        let ranges = vec![3..70, 100..101, 120..200, 10..20];
        for _ in 0..2 {
            let result = reader.get_byte_ranges(ranges.clone()).await.unwrap();
            for (range, bytes) in ranges.iter().zip(result) {
                assert_eq!(bytes, data.slice(range.start as usize..range.end as usize));
            }
            let bytes = reader.get_bytes(5..60).await.unwrap();
            assert_eq!(bytes, data.slice(5..60));
        }
    }
}
