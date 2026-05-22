use crate::DFResult;
use datafusion::common::exec_datafusion_err;
use datafusion::execution::SessionState;
use object_store::ObjectStoreExt;
use oxrdf::GraphName;
use oxrdfio::RdfFormat;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio::sync::Mutex;
use url::Url;

/// An input for the [`RdfParquetLoader`].
#[derive(Debug, Clone)]
pub struct RdfInput {
    /// The URL of the RDF file.
    pub url: Url,
    /// The default graph name that is used when parsing the RDF file.
    pub default_graph: GraphName,
    /// The RDF format.
    pub format: RdfFormat,
}

#[derive(Debug, thiserror::Error)]
#[error("Could not identify RDF format for URL {0}.")]
pub struct RdfFormatInferenceError(Url);

impl RdfInput {
    /// Creates a new [`RdfInput`].
    pub fn try_new(
        url: Url,
        default_graph: impl Into<GraphName>,
    ) -> Result<Self, RdfFormatInferenceError> {
        let extension = url.path().split('.').next_back().unwrap_or_default();
        let format = RdfFormat::from_extension(extension)
            .ok_or_else(|| RdfFormatInferenceError(url.clone()))?;
        Ok(Self {
            url,
            default_graph: default_graph.into(),
            format,
        })
    }

    /// Creates a new [`RdfInput`].
    pub fn new_with_format(
        url: Url,
        default_graph: impl Into<GraphName>,
        format: RdfFormat,
    ) -> Self {
        Self {
            url,
            default_graph: default_graph.into(),
            format,
        }
    }
}

/// A source for RDF data.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RdfInputSource {
    inner: RdfFileSourceInner,
}

#[derive(Clone)]
enum RdfFileSourceInner {
    Url(Url),
    Stream(Arc<Mutex<Option<Box<dyn AsyncRead + Unpin + Send + 'static>>>>),
}

impl RdfInputSource {
    /// Creates a new [`RdfInputSource`] from a URL.
    pub fn from_url(url: Url) -> Self {
        Self {
            inner: RdfFileSourceInner::Url(url),
        }
    }

    /// Creates a new [`RdfInputSource`] from a stream.
    pub fn from_stream(reader: impl AsyncRead + Unpin + Send + 'static) -> Self {
        Self {
            inner: RdfFileSourceInner::Stream(Arc::new(Mutex::new(Some(Box::new(
                reader,
            ))))),
        }
    }

    /// Returns a stream of quads from the source.
    pub async fn stream(
        &self,
        state: &SessionState,
    ) -> DFResult<Box<dyn AsyncRead + Unpin + Send + 'static>> {
        match &self.inner {
            RdfFileSourceInner::Url(url) => {
                let runtime = state.runtime_env();
                let object_store =
                    runtime.object_store_registry.get_store(url).map_err(|e| {
                        exec_datafusion_err!("Failed to get object store: {e}")
                    })?;
                let path = object_store::path::Path::from(url.path());
                let get_result = object_store.get(&path).await.map_err(|e| {
                    exec_datafusion_err!("Failed to get object {}: {e}", url)
                })?;
                let stream = get_result.into_stream();
                Ok(Box::new(tokio_util::io::StreamReader::new(stream)))
            }
            RdfFileSourceInner::Stream(stream) => stream
                .lock()
                .await
                .take()
                .ok_or_else(|| exec_datafusion_err!("Stream already consumed")),
        }
    }
}

impl PartialEq for RdfFileSourceInner {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Url(l0), Self::Url(r0)) => l0 == r0,
            (Self::Stream(l0), Self::Stream(r0)) => Arc::ptr_eq(l0, r0),
            _ => false,
        }
    }
}

impl Eq for RdfFileSourceInner {}

impl Hash for RdfFileSourceInner {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Url(url) => {
                0.hash(state);
                url.hash(state);
            }
            Self::Stream(stream) => {
                1.hash(state);
                Arc::as_ptr(stream).hash(state);
            }
        }
    }
}

impl PartialOrd for RdfFileSourceInner {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RdfFileSourceInner {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Url(l0), Self::Url(r0)) => l0.cmp(r0),
            (Self::Url(_), Self::Stream(_)) => Ordering::Less,
            (Self::Stream(_), Self::Url(_)) => Ordering::Greater,
            (Self::Stream(l0), Self::Stream(r0)) => Arc::as_ptr(l0).cmp(&Arc::as_ptr(r0)),
        }
    }
}

impl Debug for RdfFileSourceInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Url(url) => write!(f, "Url({url})"),
            Self::Stream(_) => write!(f, "Stream"),
        }
    }
}

impl Debug for RdfInputSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.inner, f)
    }
}
