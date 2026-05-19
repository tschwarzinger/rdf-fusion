use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::object_store::{self, ObjectStore, PutOptions, PutPayload, PutResult};
use datafusion::physical_plan::metrics::Count;
use futures::StreamExt;
use futures::stream::BoxStream;
use moka::future::Cache;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, RenameOptions,
};
use rdf_fusion_common::DFResult;
use std::ops::Range;
use std::sync::Arc;
use url::Url;

/// Metrics for a [`CachingObjectStore`].
pub struct CacheMetrics {
    pub get_opts_hits: Arc<Count>,
    pub get_opts_misses: Arc<Count>,
    pub get_ranges_hits: Arc<Count>,
    pub get_ranges_misses: Arc<Count>,
    pub eviction_count: Arc<Count>,
    pub data_cache_size: u64,
    pub data_cache_weight: u64,
}

impl CacheMetrics {
    pub fn new() -> Self {
        Self {
            get_opts_hits: Arc::new(Count::new()),
            get_opts_misses: Arc::new(Count::new()),
            get_ranges_hits: Arc::new(Count::new()),
            get_ranges_misses: Arc::new(Count::new()),
            eviction_count: Arc::new(Count::new()),
            data_cache_size: 0,
            data_cache_weight: 0,
        }
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A caching decorator for an [`ObjectStore`] supporting exact range caching.
#[derive(Debug)]
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    // Caches metadata and attributes per file path
    meta_cache: Cache<Path, (ObjectMeta, Attributes)>,
    // Caches content bytes
    data_cache: Cache<(Path, Option<Range<u64>>), bytes::Bytes>,

    // Metrics
    get_opts_hits: Arc<Count>,
    get_opts_misses: Arc<Count>,
    get_ranges_hits: Arc<Count>,
    get_ranges_misses: Arc<Count>,
}

#[async_trait::async_trait]
impl ObjectStore for CachingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.meta_cache.invalidate(location).await;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.meta_cache.invalidate(location).await;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let is_cacheable = options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.version.is_none();

        if !is_cacheable {
            return self.inner.get_opts(location, options).await;
        }

        let cacheable_range = match &options.range {
            None => None,
            Some(GetRange::Bounded(r)) => Some(r.start..r.end),
            _ => return self.inner.get_opts(location, options).await,
        };

        let cache_key = (location.clone(), cacheable_range.clone());

        let cached_data = self.data_cache.get(&cache_key).await;
        let cached_meta = self.meta_cache.get(location).await;

        if let (Some(bytes), Some((meta, attributes))) = (cached_data, cached_meta) {
            self.get_opts_hits.add(1);
            let result_range = cacheable_range.unwrap_or(0..meta.size);
            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    futures::stream::once(async move { Ok(bytes) }).boxed(),
                ),
                meta,
                range: result_range,
                attributes,
            });
        }

        // Cache Miss: Fetch from inner store
        self.get_opts_misses.add(1);
        let result = self.inner.get_opts(location, options.clone()).await?;

        let meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let result_range = result.range.clone();

        self.meta_cache
            .insert(location.clone(), (meta.clone(), attributes.clone()))
            .await;

        let bytes = result.bytes().await?;
        self.data_cache.insert(cache_key, bytes.clone()).await;

        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(bytes) }).boxed(),
            ),
            meta,
            range: result_range,
            attributes,
        })
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        let mut results: Vec<Option<bytes::Bytes>> = vec![None; ranges.len()];
        let mut missing_ranges = Vec::new();
        let mut missing_indices = Vec::new();

        // Check cache for each requested range
        for (i, range) in ranges.iter().enumerate() {
            let key = (location.clone(), Some(range.clone()));
            if let Some(cached_bytes) = self.data_cache.get(&key).await {
                self.get_ranges_hits.add(1);
                results[i] = Some(cached_bytes);
            } else {
                self.get_ranges_misses.add(1);
                missing_ranges.push(range.clone());
                missing_indices.push(i);
            }
        }

        // Fetch missing ranges from the underlying store
        if !missing_ranges.is_empty() {
            let fetched_bytes = self.inner.get_ranges(location, &missing_ranges).await?;

            for (fetched, (original_index, range)) in fetched_bytes
                .into_iter()
                .zip(missing_indices.into_iter().zip(missing_ranges))
            {
                let key = (location.clone(), Some(range));
                self.data_cache.insert(key, fetched.clone()).await;

                results[original_index] = Some(fetched);
            }
        }

        Ok(results.into_iter().map(|b| b.unwrap()).collect())
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

impl std::fmt::Display for CachingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachingObjectStore({})", self.inner)
    }
}

/// A registry that decorates stores with a [`CachingObjectStore`].
#[derive(Debug)]
pub struct CachingObjectStoreRegistry {
    inner: Arc<dyn ObjectStoreRegistry>,
    meta_cache: Cache<Path, (ObjectMeta, Attributes)>,
    data_cache: Cache<(Path, Option<Range<u64>>), bytes::Bytes>,

    // Metrics
    get_opts_hits: Arc<Count>,
    get_opts_misses: Arc<Count>,
    get_ranges_hits: Arc<Count>,
    get_ranges_misses: Arc<Count>,

    // Statistics
    eviction_count: Arc<Count>,
}

impl CachingObjectStoreRegistry {
    pub fn new(inner: Arc<dyn ObjectStoreRegistry>, cache_size_bytes: u64) -> Self {
        let meta_cache = Cache::builder().max_capacity(10_000).build();

        let eviction_count = Arc::new(Count::new());
        let eviction_count_clone = Arc::clone(&eviction_count);

        let data_cache = Cache::builder()
            .max_capacity(cache_size_bytes)
            .weigher(|_k, v: &bytes::Bytes| {
                // Safely clamp to u32::MAX to prevent overflow panics on files > 4GB
                let len = v.len() as u64;
                len.try_into().unwrap_or(u32::MAX)
            })
            .eviction_listener(move |_k, _v, _cause| {
                eviction_count_clone.add(1);
            })
            .build();

        Self {
            inner,
            meta_cache,
            data_cache,
            get_opts_hits: Arc::new(Count::new()),
            get_opts_misses: Arc::new(Count::new()),
            get_ranges_hits: Arc::new(Count::new()),
            get_ranges_misses: Arc::new(Count::new()),
            eviction_count,
        }
    }

    /// Returns the aggregated metrics for all caching stores in this registry.
    pub fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            get_opts_hits: Arc::clone(&self.get_opts_hits),
            get_opts_misses: Arc::clone(&self.get_opts_misses),
            get_ranges_hits: Arc::clone(&self.get_ranges_hits),
            get_ranges_misses: Arc::clone(&self.get_ranges_misses),
            eviction_count: Arc::clone(&self.eviction_count),
            data_cache_size: self.data_cache.entry_count(),
            data_cache_weight: self.data_cache.weighted_size(),
        }
    }
}

impl ObjectStoreRegistry for CachingObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> DFResult<Arc<dyn ObjectStore>> {
        let store = self.inner.get_store(url)?;

        // Memory stores are already entirely in RAM.
        if url.scheme() == "memory" {
            return Ok(store);
        }

        Ok(Arc::new(CachingObjectStore {
            inner: store,
            meta_cache: self.meta_cache.clone(),
            data_cache: self.data_cache.clone(),
            get_opts_hits: Arc::clone(&self.get_opts_hits),
            get_opts_misses: Arc::clone(&self.get_opts_misses),
            get_ranges_hits: Arc::clone(&self.get_ranges_hits),
            get_ranges_misses: Arc::clone(&self.get_ranges_misses),
        }))
    }
}

impl CacheMetrics {
    pub fn get_opts_hits(&self) -> u64 {
        self.get_opts_hits.value() as u64
    }
    pub fn get_opts_misses(&self) -> u64 {
        self.get_opts_misses.value() as u64
    }
    pub fn get_ranges_hits(&self) -> u64 {
        self.get_ranges_hits.value() as u64
    }
    pub fn get_ranges_misses(&self) -> u64 {
        self.get_ranges_misses.value() as u64
    }
    pub fn eviction_count(&self) -> u64 {
        self.eviction_count.value() as u64
    }
    pub fn data_cache_size(&self) -> u64 {
        self.data_cache_size
    }
    pub fn data_cache_weight(&self) -> u64 {
        self.data_cache_weight
    }
}
