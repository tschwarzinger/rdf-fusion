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

/// A cache for pre-downloaded Bloom filters.
#[derive(Debug, Clone, Default)]
pub struct BloomFilterCache {
    filters: Arc<Vec<(Range<u64>, Bytes)>>,
}

impl BloomFilterCache {
    pub fn new(filters: Vec<(Range<u64>, Bytes)>) -> Self {
        Self {
            filters: Arc::new(filters),
        }
    }

    pub fn get(&self, range: &Range<u64>) -> Option<Bytes> {
        self.filters
            .iter()
            .find(|(r, _)| r == range)
            .map(|(_, b)| b.clone())
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
        let all_cached = ranges
            .iter()
            .all(|r| self.bloom_filter_cache.get(r).is_some());
        if all_cached {
            let bytes = ranges
                .iter()
                .map(|r| self.bloom_filter_cache.get(r).unwrap())
                .collect();
            return Box::pin(async move { Ok(bytes) });
        }
        self.inner.get_byte_ranges(ranges)
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

        // Error out if the file name doesn't match
        if requested_path != self.expected_path {
            return Err(DataFusionError::Execution(format!(
                "Pre-loaded metadata reader reader expected file '{}', but was asked to open '{}'",
                self.expected_path, requested_path
            )));
        }

        // Create the actual underlying reader (e.g., ObjectStore reader)
        let inner_reader = self.inner_factory.create_reader(
            partition_index,
            file,
            metadata_size_hint,
            metrics,
        )?;

        // Wrap it to intercept metadata requests
        Ok(Box::new(PreloadedMetadataReader {
            inner: inner_reader,
            metadata: Arc::clone(&self.cached_parquet_meta),
            bloom_filter_cache: self.bloom_filter_cache.clone(),
        }))
    }
}
