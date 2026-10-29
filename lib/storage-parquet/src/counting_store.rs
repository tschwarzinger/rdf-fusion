use async_trait::async_trait;
use bytes::Bytes;
use datafusion::physical_plan::metrics::Count;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

/// An [`ObjectStore`] that records the number of bytes read through it in a [`Count`].
///
/// Like DataFusion's `bytes_scanned` metric, this counts the *requested* bytes. Hence, bytes that
/// the inner store fetches additionally due to coalescing ranges in
/// [`ObjectStore::get_ranges`] are not included.
#[derive(Debug)]
pub struct ByteCountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    bytes_read: Count,
}

impl ByteCountingObjectStore {
    /// Creates a new [`ByteCountingObjectStore`] that records the bytes read from `inner` in
    /// `bytes_read`.
    pub fn new(inner: Arc<dyn ObjectStore>, bytes_read: Count) -> Self {
        Self { inner, bytes_read }
    }
}

impl Display for ByteCountingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ByteCountingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ByteCountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            self.bytes_read
                .add((result.range.end - result.range.start) as usize);
        }
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>> {
        let result = self.inner.get_ranges(location, ranges).await?;
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.bytes_read.add(total as usize);
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn counts_requested_bytes() {
        let inner = Arc::new(InMemory::new());
        let path = Path::from("test");
        inner
            .put(&path, Bytes::from(vec![0u8; 1000]).into())
            .await
            .unwrap();

        let count = Count::new();
        let store = ByteCountingObjectStore::new(inner, count.clone());
        store.get_range(&path, 10..20).await.unwrap();
        store.get_ranges(&path, &[0..5, 100..200]).await.unwrap();
        store.head(&path).await.unwrap();

        assert_eq!(count.value(), 10 + 5 + 100);
    }
}
