use bytes::Bytes;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::metadata::PageIndexPolicy;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use futures::future::BoxFuture;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

type PreloadedMetadataCacheMap =
    HashMap<object_store::path::Path, (Arc<ParquetMetaData>, ObjectMeta)>;

/// Contains a list of preloaded parquet metadata for a given path.
#[derive(Debug, Clone, Default)]
pub struct PreloadedParquetMetadata {
    cache: Arc<RwLock<PreloadedMetadataCacheMap>>,
}

impl PreloadedParquetMetadata {
    /// Creates a new [`PreloadedParquetMetadata`].
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Obtains the metadata for the given URL.
    pub fn get(
        &self,
        path: &object_store::path::Path,
    ) -> Option<(Arc<ParquetMetaData>, ObjectMeta)> {
        let cache = self.cache.read().expect("poisoned lock");
        cache.get(path).cloned()
    }

    /// Inserts the metadata for a given URL.
    pub fn insert(
        &self,
        path: object_store::path::Path,
        value: (Arc<ParquetMetaData>, ObjectMeta),
    ) {
        let mut cache = self.cache.write().expect("poisoned lock");
        cache.insert(path, value);
    }
}

type PreloadedBloomFiltersList = Vec<(Range<u64>, Bytes)>;
type PreloadedBloomFiltersMap =
    HashMap<object_store::path::Path, Arc<PreloadedBloomFiltersList>>;

/// Contains a list of preloaded bloom filters for given URLs.
#[derive(Debug, Clone, Default)]
pub struct PreloadedBloomFilters {
    cache: Arc<RwLock<PreloadedBloomFiltersMap>>,
    hit_counter: Arc<AtomicUsize>,
}

impl PreloadedBloomFilters {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            hit_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get(
        &self,
        path: &object_store::path::Path,
        range: &Range<u64>,
    ) -> Option<Bytes> {
        let cache = self.cache.read().expect("poisoned lock");
        if let Some(filters) = cache.get(path) {
            let match_opt = filters
                .iter()
                .find(|(r, _)| r == range)
                .map(|(_, b)| b.clone());
            if match_opt.is_some() {
                self.hit_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            match_opt
        } else {
            None
        }
    }

    pub fn get_all(
        &self,
        path: &object_store::path::Path,
    ) -> Option<Arc<PreloadedBloomFiltersList>> {
        let cache = self.cache.read().expect("poisoned lock");
        cache.get(path).cloned()
    }

    pub fn insert(
        &self,
        path: object_store::path::Path,
        filters: PreloadedBloomFiltersList,
    ) {
        let mut cache = self.cache.write().expect("poisoned lock");
        cache.insert(path, Arc::new(filters));
    }

    pub fn insert_arc(
        &self,
        path: object_store::path::Path,
        filters: Arc<PreloadedBloomFiltersList>,
    ) {
        let mut cache = self.cache.write().expect("poisoned lock");
        cache.insert(path, filters);
    }

    pub fn hit_count(&self) -> usize {
        self.hit_counter.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of bloom filters in the cache.
    pub fn len(&self) -> usize {
        let cache = self.cache.read().expect("poisoned lock");
        cache.values().map(|v| v.len()).sum()
    }
}

/// Shared helper function to load parquet metadata and bloom filters
pub async fn load_parquet_metadata_and_bloom_filters(
    object_store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    object_meta: ObjectMeta,
) -> DFResult<(Arc<ParquetMetaData>, PreloadedBloomFiltersList)> {
    let reader = ParquetObjectReader::new(Arc::clone(&object_store), path.clone())
        .with_file_size(object_meta.size);
    let options =
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder =
        ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;

    let parquet_meta = Arc::clone(builder.metadata());
    let mut bloom_filter_ranges = Vec::new();
    for rg in parquet_meta.row_groups() {
        for col in rg.columns() {
            if let Some(offset) = col.bloom_filter_offset() {
                if let Some(length) = col.bloom_filter_length() {
                    bloom_filter_ranges
                        .push(offset as u64..(offset as u64 + length as u64));
                }
            }
        }
    }

    let bloom_filter_bytes = if bloom_filter_ranges.is_empty() {
        Vec::new()
    } else {
        object_store
            .get_ranges(&path, &bloom_filter_ranges)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    };

    let filters = bloom_filter_ranges
        .into_iter()
        .zip(bloom_filter_bytes)
        .collect();

    Ok((parquet_meta, filters))
}

use crate::block_cache::BlockCache;

/// A custom [`AsyncFileReader`] that serves ParquetMetaData from memory, but delegates actual byte
/// reading to the underlying storage reader, with optional block caching.
struct PreloadedMetadataReader {
    inner: Box<dyn AsyncFileReader + Send>,
    path: object_store::path::Path,
    file_size: u64,
    metadata: Arc<ParquetMetaData>,
    bloom_filter_cache: PreloadedBloomFilters,
    block_cache: Option<Arc<BlockCache>>,
}

impl AsyncFileReader for PreloadedMetadataReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        if let Some(bytes) = self.bloom_filter_cache.get(&self.path, &range) {
            return Box::pin(async move { Ok(bytes) });
        }

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
        }

        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        let mut results: Vec<Option<Bytes>> = vec![None; ranges.len()];
        let mut uncached_indices = Vec::new();

        for (idx, range) in ranges.iter().enumerate() {
            if let Some(bytes) = self.bloom_filter_cache.get(&self.path, range) {
                results[idx] = Some(bytes);
            } else {
                uncached_indices.push(idx);
            }
        }

        if uncached_indices.is_empty() {
            let bytes = results.into_iter().map(Option::unwrap).collect();
            return Box::pin(async move { Ok(bytes) });
        }

        if let Some(block_cache) = &self.block_cache {
            let block_size = block_cache.block_size();
            let mut missing_blocks: HashMap<u64, Range<u64>> = HashMap::new();

            for &idx in &uncached_indices {
                let range = &ranges[idx];
                if range.start >= range.end {
                    continue;
                }
                let start_block = range.start / block_size;
                let end_block = range.end.saturating_sub(1) / block_size;

                for block_idx in start_block..=end_block {
                    if block_cache.get(&self.path, block_idx).is_none() {
                        let block_start = block_idx * block_size;
                        let block_end = (block_start + block_size).min(self.file_size);
                        missing_blocks
                            .entry(block_idx)
                            .or_insert(block_start..block_end);
                    }
                }
            }

            let block_cache = Arc::clone(block_cache);
            let path = self.path.clone();
            let file_size = self.file_size;

            if missing_blocks.is_empty() {
                for idx in uncached_indices {
                    let range = &ranges[idx];
                    if range.start >= range.end {
                        results[idx] = Some(Bytes::new());
                        continue;
                    }
                    let bytes = assemble_range_from_cache(
                        &block_cache,
                        &path,
                        range,
                        block_size,
                        file_size,
                    );
                    results[idx] = Some(bytes);
                }
                let bytes = results.into_iter().map(Option::unwrap).collect();
                return Box::pin(async move { Ok(bytes) });
            }

            let block_indices: Vec<u64> = missing_blocks.keys().copied().collect();
            let fetch_ranges: Vec<Range<u64>> =
                missing_blocks.values().cloned().collect();
            let fut = self.inner.get_byte_ranges(fetch_ranges);

            return Box::pin(async move {
                let fetched = fut.await?;
                for (block_idx, bytes) in block_indices.into_iter().zip(fetched) {
                    block_cache.insert(path.clone(), block_idx, bytes);
                }

                for idx in uncached_indices {
                    let range = &ranges[idx];
                    if range.start >= range.end {
                        results[idx] = Some(Bytes::new());
                        continue;
                    }
                    let bytes = assemble_range_from_cache(
                        &block_cache,
                        &path,
                        range,
                        block_size,
                        file_size,
                    );
                    results[idx] = Some(bytes);
                }

                Ok(results.into_iter().map(Option::unwrap).collect())
            });
        }

        let uncached_ranges: Vec<Range<u64>> = uncached_indices
            .iter()
            .map(|&idx| ranges[idx].clone())
            .collect();
        let fut = self.inner.get_byte_ranges(uncached_ranges);
        Box::pin(async move {
            let fetched = fut.await?;
            let mut fetched_iter = fetched.into_iter();
            for idx in uncached_indices {
                results[idx] = Some(fetched_iter.next().expect("Fetched count mismatch"));
            }
            let bytes = results.into_iter().map(Option::unwrap).collect();
            Ok(bytes)
        })
    }

    fn get_metadata(
        &mut self,
        _options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, Result<Arc<ParquetMetaData>, ParquetError>> {
        let meta = Arc::clone(&self.metadata);
        Box::pin(async move { Ok(meta) })
    }
}

fn assemble_range_from_cache(
    block_cache: &BlockCache,
    path: &object_store::path::Path,
    range: &Range<u64>,
    block_size: u64,
    _file_size: u64,
) -> Bytes {
    let start_block = range.start / block_size;
    let end_block = range.end.saturating_sub(1) / block_size;

    if start_block == end_block {
        let block_start = start_block * block_size;
        let block = block_cache
            .get(path, start_block)
            .expect("Block must be in cache");
        let slice_start = (range.start - block_start) as usize;
        let slice_end = (range.end - block_start) as usize;
        block.slice(slice_start..slice_end.min(block.len()))
    } else {
        let mut combined = Vec::with_capacity((range.end - range.start) as usize);
        for block_idx in start_block..=end_block {
            let block = block_cache
                .get(path, block_idx)
                .expect("Block must be in cache");
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

/// A factory that verifies the file path and injects the preloaded metadata.
#[derive(Debug, Clone)]
pub struct PreLoadedMetadataReaderFactory {
    inner_factory: Arc<dyn ParquetFileReaderFactory>,
    cache: PreloadedParquetMetadata,
    bloom_filter_cache: PreloadedBloomFilters,
    block_cache: Option<Arc<BlockCache>>,
}

impl PreLoadedMetadataReaderFactory {
    pub fn new(
        inner_factory: Arc<dyn ParquetFileReaderFactory>,
        cache: PreloadedParquetMetadata,
        bloom_filter_cache: PreloadedBloomFilters,
        block_cache: Option<Arc<BlockCache>>,
    ) -> Self {
        Self {
            inner_factory,
            cache,
            bloom_filter_cache,
            block_cache,
        }
    }
}

impl ParquetFileReaderFactory for PreLoadedMetadataReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DFResult<Box<dyn AsyncFileReader + Send>> {
        let preloaded_meta = self
            .cache
            .get(&file.object_meta.location)
            .map(|(meta, _)| meta);
        let preloaded_parquet_meta = match preloaded_meta {
            Some(meta) => meta,
            None => {
                return Err(DataFusionError::Execution(format!(
                    "Pre-loaded metadata reader did not find file '{}' in cache",
                    file.object_meta.location
                )));
            }
        };

        let inner_reader = self.inner_factory.create_reader(
            partition_index,
            file.clone(),
            metadata_size_hint,
            metrics,
        )?;

        Ok(Box::new(PreloadedMetadataReader {
            inner: inner_reader,
            path: file.object_meta.location.clone(),
            file_size: file.object_meta.size,
            metadata: preloaded_parquet_meta,
            bloom_filter_cache: self.bloom_filter_cache.clone(),
            block_cache: self.block_cache.clone(),
        }))
    }
}
