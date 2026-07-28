use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::future::{FutureExt, Shared};
use object_store::ObjectMeta;
use object_store::{
    ObjectStore, ObjectStoreExt, Result as OSResult, path::Path as OSPath,
};
use quick_cache::sync::Cache;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub path: OSPath,
    pub block_index: u64,
}

type InFlightMap = HashMap<
    BlockKey,
    Shared<BoxFuture<'static, Result<Bytes, Arc<object_store::Error>>>>,
>;

pub struct CachedObjectStore {
    inner: Arc<dyn ObjectStore>,
    block_size: u64,
    cache: Arc<Cache<BlockKey, Bytes>>,
    file_sizes: Arc<Cache<OSPath, usize>>,
    in_flight: Arc<Mutex<InFlightMap>>,
}

impl Debug for CachedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedObjectStore").finish()
    }
}

impl std::fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachedObjectStore")
    }
}

impl CachedObjectStore {
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        block_size: usize,
        num_blocks: usize,
    ) -> Self {
        Self {
            inner,
            block_size: block_size as u64,
            cache: Arc::new(Cache::new(num_blocks)),
            file_sizes: Arc::new(Cache::new(10000)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get_file_size(&self, location: &OSPath) -> OSResult<usize> {
        if let Some(size) = self.file_sizes.get(location) {
            return Ok(size);
        }
        let meta = self.inner.head(location).await?;
        self.file_sizes.insert(location.clone(), meta.size as usize);
        Ok(meta.size as usize)
    }

    async fn get_block(&self, location: &OSPath, block_index: u64) -> OSResult<Bytes> {
        let key = BlockKey {
            path: location.clone(),
            block_index,
        };

        if let Some(bytes) = self.cache.get(&key) {
            return Ok(bytes);
        }

        let mut lock = self.in_flight.lock().await;
        if let Some(fut) = lock.get(&key) {
            let fut = fut.clone();
            drop(lock);
            return fut.await.map_err(|e| object_store::Error::Generic {
                store: "CachedObjectStore",
                source: Box::new(std::io::Error::other(e.to_string())),
            });
        }

        let inner = Arc::clone(&self.inner);
        let location_clone = location.clone();
        let file_size = self.get_file_size(location).await?;
        let block_size = self.block_size;

        let start = block_index * block_size;
        let end = (start + block_size).min(file_size as u64);

        if start >= file_size as u64 {
            return Ok(Bytes::new());
        }

        let fut = async move {
            inner
                .get_range(&location_clone, start..end)
                .await
                .map(|bytes| Bytes::copy_from_slice(&bytes))
                .map_err(Arc::new)
        }
        .boxed()
        .shared();

        lock.insert(key.clone(), fut.clone());
        drop(lock);

        let res = fut.await;

        let mut lock = self.in_flight.lock().await;
        lock.remove(&key);
        drop(lock);

        match res {
            Ok(bytes) => {
                self.cache.insert(key, bytes.clone());
                Ok(bytes)
            }
            Err(e) => Err(object_store::Error::Generic {
                store: "CachedObjectStore",
                source: Box::new(std::io::Error::other(e.to_string())),
            }),
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for CachedObjectStore {
    async fn put_opts(
        &self,
        location: &OSPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> OSResult<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &OSPath,
        opts: object_store::PutMultipartOptions,
    ) -> OSResult<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &OSPath,
        options: object_store::GetOptions,
    ) -> OSResult<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        _locations: futures::stream::BoxStream<'static, OSResult<OSPath>>,
    ) -> futures::stream::BoxStream<'static, OSResult<OSPath>> {
        futures::stream::empty().boxed()
    }

    fn list(
        &self,
        prefix: Option<&OSPath>,
    ) -> futures::stream::BoxStream<'static, OSResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OSPath>,
    ) -> OSResult<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &OSPath,
        to: &OSPath,
        opts: object_store::CopyOptions,
    ) -> OSResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }

    async fn get_ranges(
        &self,
        location: &OSPath,
        ranges: &[Range<u64>],
    ) -> OSResult<Vec<Bytes>> {
        let mut results = Vec::with_capacity(ranges.len());
        for range in ranges {
            if range.start >= range.end {
                results.push(Bytes::new());
                continue;
            }
            let start_block = range.start / self.block_size;
            let end_block = range.end.saturating_sub(1) / self.block_size;

            let mut block_futures = Vec::new();
            for block_idx in start_block..=end_block {
                block_futures.push(self.get_block(location, block_idx));
            }

            let blocks = futures::future::try_join_all(block_futures).await?;

            if blocks.len() == 1 {
                let block_start = range.start % self.block_size;
                let block_end = block_start + (range.end - range.start);
                results.push(blocks[0].slice(block_start as usize..block_end as usize));
            } else {
                let mut combined = Vec::with_capacity((range.end - range.start) as usize);
                for (i, block) in blocks.iter().enumerate() {
                    let block_idx = start_block + i as u64;
                    let block_start_in_file = block_idx * self.block_size;

                    let slice_start = range.start.saturating_sub(block_start_in_file);

                    let slice_end = if block_start_in_file + self.block_size > range.end {
                        range.end - block_start_in_file
                    } else {
                        block.len() as u64
                    };

                    combined.extend_from_slice(
                        &block[slice_start as usize..slice_end as usize],
                    );
                }
                results.push(Bytes::from(combined));
            }
        }
        Ok(results)
    }
}
