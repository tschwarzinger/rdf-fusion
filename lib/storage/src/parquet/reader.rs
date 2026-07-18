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
use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

pub type PreloadedMetadataCacheMap =
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

pub type PreloadedBloomFiltersList = Vec<(Range<u64>, Bytes)>;
pub type PreloadedBloomFiltersMap =
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

    pub fn len(&self) -> usize {
        let cache = self.cache.read().expect("poisoned lock");
        cache.values().map(|v| v.len()).sum()
    }
}

/// Shared helper function to load parquet metadata and bloom filters
pub async fn load_parquet_metadata_and_bloom_filters(
    object_store: Arc<dyn object_store::ObjectStore>,
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

/// A custom [`AsyncFileReader`] that serves ParquetMetaData from memory, but delegates actual byte
/// reading to the underlying storage reader.
pub struct PreloadedMetadataReader {
    inner: Box<dyn AsyncFileReader + Send>,
    path: object_store::path::Path,
    metadata: Arc<ParquetMetaData>,
    bloom_filter_cache: PreloadedBloomFilters,
}

impl AsyncFileReader for PreloadedMetadataReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        if let Some(bytes) = self.bloom_filter_cache.get(&self.path, &range) {
            return Box::pin(async move { Ok(bytes) });
        }
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        let mut uncached_ranges = Vec::new();
        let mut uncached_indices = Vec::new();
        let mut results = vec![None; ranges.len()];

        for (idx, range) in ranges.into_iter().enumerate() {
            if let Some(bytes) = self.bloom_filter_cache.get(&self.path, &range) {
                results[idx] = Some(bytes);
            } else {
                uncached_ranges.push(range);
                uncached_indices.push(idx);
            }
        }

        if uncached_ranges.is_empty() {
            let bytes = results.into_iter().map(Option::unwrap).collect();
            return Box::pin(async move { Ok(bytes) });
        }

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

/// A factory that verifies the file path and injects the preloaded metadata.
#[derive(Debug, Clone)]
pub struct PreLoadedMetadataReaderFactory {
    inner_factory: Arc<dyn ParquetFileReaderFactory>,
    cache: PreloadedParquetMetadata,
    bloom_filter_cache: PreloadedBloomFilters,
}

impl PreLoadedMetadataReaderFactory {
    pub fn new(
        inner_factory: Arc<dyn ParquetFileReaderFactory>,
        cache: PreloadedParquetMetadata,
        bloom_filter_cache: PreloadedBloomFilters,
    ) -> Self {
        Self {
            inner_factory,
            cache,
            bloom_filter_cache,
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
            metadata: preloaded_parquet_meta,
            bloom_filter_cache: self.bloom_filter_cache.clone(),
        }))
    }
}
