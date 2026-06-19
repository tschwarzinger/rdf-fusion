use bytes::Bytes;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use futures::future::BoxFuture;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

/// A cache for pre-downloaded Bloom filters.
#[derive(Debug, Clone, Default)]
pub struct BloomFilterCache {
    filters: Arc<Vec<(Range<u64>, Bytes)>>,
    hit_counter: Arc<AtomicUsize>,
}

impl BloomFilterCache {
    pub fn new(filters: Vec<(Range<u64>, Bytes)>) -> Self {
        Self {
            filters: Arc::new(filters),
            hit_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get(&self, range: &Range<u64>) -> Option<Bytes> {
        let match_opt = self
            .filters
            .iter()
            .find(|(r, _)| r == range)
            .map(|(_, b)| b.clone());
        if match_opt.is_some() {
            self.hit_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        match_opt
    }

    pub fn hit_count(&self) -> usize {
        self.hit_counter.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.filters.len()
    }
}

/// A custom [`AsyncFileReader`] that serves ParquetMetaData from memory, but delegates actual byte
/// reading to the underlying storage reader.
pub struct PreloadedMetadataReader {
    inner: Box<dyn AsyncFileReader + Send>,
    metadata: Arc<ParquetMetaData>,
    bloom_filter_cache: BloomFilterCache,
}

impl AsyncFileReader for PreloadedMetadataReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        if let Some(bytes) = self.bloom_filter_cache.get(&range) {
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
            if let Some(bytes) = self.bloom_filter_cache.get(&range) {
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
    expected_path: String,
    cached_parquet_meta: Arc<ParquetMetaData>,
    bloom_filter_cache: BloomFilterCache,
}

impl PreLoadedMetadataReaderFactory {
    pub fn new(
        inner_factory: Arc<dyn ParquetFileReaderFactory>,
        expected_path: String,
        cached_parquet_meta: Arc<ParquetMetaData>,
        bloom_filter_cache: BloomFilterCache,
    ) -> Self {
        Self {
            inner_factory,
            expected_path,
            cached_parquet_meta,
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
        let requested_path = file.object_meta.location.as_ref();

        if requested_path != self.expected_path {
            return Err(DataFusionError::Execution(format!(
                "Pre-loaded metadata reader reader expected file '{}', but was asked to open '{}'",
                self.expected_path, requested_path
            )));
        }

        let inner_reader = self.inner_factory.create_reader(
            partition_index,
            file,
            metadata_size_hint,
            metrics,
        )?;

        Ok(Box::new(PreloadedMetadataReader {
            inner: inner_reader,
            metadata: Arc::clone(&self.cached_parquet_meta),
            bloom_filter_cache: self.bloom_filter_cache.clone(),
        }))
    }
}
