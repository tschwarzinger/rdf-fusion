use anyhow::{Context, Result};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::{GetOptions, GetResult, ObjectMeta, ObjectStore, ObjectStoreExt};
use oxttl::N3Parser;
use oxttl::n3::N3Quad;
use rdf_fusion::io::{RdfFormat, RdfParser};
use rdf_fusion::model::{Dataset, Graph};
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::bytes::Bytes;
use url::Url;

/// A shared [`RuntimeEnv`] used by the test suite to avoid creating too many thread pools.
pub static TEST_RUNTIME_ENV: LazyLock<Arc<RuntimeEnv>> =
    LazyLock::new(create_test_runtime_env);

pub fn create_test_runtime_env() -> Arc<RuntimeEnv> {
    let runtime_env = RuntimeEnvBuilder::new().build_arc().unwrap();

    let local_store = Arc::new(
        LocalFileSystem::new_with_prefix(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("suites"),
        )
        .unwrap(),
    );

    let test_store = Arc::new(TestSuiteObjectStore { inner: local_store });

    runtime_env.register_object_store(
        &Url::parse("https://w3c.github.io/").unwrap(),
        Arc::clone(&test_store) as Arc<dyn ObjectStore>,
    );
    runtime_env.register_object_store(
        &Url::parse("http://www.w3.org/").unwrap(),
        Arc::clone(&test_store) as Arc<dyn ObjectStore>,
    );
    runtime_env
        .register_object_store(&Url::parse("https://codeberg.org/").unwrap(), test_store);

    runtime_env
}

#[derive(Debug)]
struct TestSuiteObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl TestSuiteObjectStore {
    fn map_path(&self, location: &object_store::path::Path) -> object_store::path::Path {
        let p = location.as_ref();
        if p.starts_with(
            "tschwarzinger/rdf-fusion/raw/branch/main/testsuite/rdf-fusion-tests/",
        ) {
            object_store::path::Path::from(p.replace(
                "tschwarzinger/rdf-fusion/raw/branch/main/testsuite/rdf-fusion-tests/",
                "rdf-fusion-tests/",
            ))
        } else if p.starts_with("2001/sw/DataAccess/tests/data-r2/") {
            object_store::path::Path::from(p.replace(
                "2001/sw/DataAccess/tests/data-r2/",
                "rdf-tests/sparql/sparql10/",
            ))
        } else if p.starts_with("2009/sparql/docs/tests/data-sparql11/") {
            object_store::path::Path::from(p.replace(
                "2009/sparql/docs/tests/data-sparql11/",
                "rdf-tests/sparql/sparql11/",
            ))
        } else {
            location.clone()
        }
    }
}

impl std::fmt::Display for TestSuiteObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestSuiteObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for TestSuiteObjectStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner
            .put_opts(&self.map_path(location), payload, options)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner
            .put_multipart_opts(&self.map_path(location), options)
            .await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(&self.map_path(location), options).await
    }

    async fn get_ranges(
        &self,
        location: &object_store::path::Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner
            .get_ranges(&self.map_path(location), ranges)
            .await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        // This is not used in tests, but for completeness:
        // We would need to map paths in the stream, which is complex.
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner
            .copy_opts(&self.map_path(from), &self.map_path(to), options)
            .await
    }

    async fn rename_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::RenameOptions,
    ) -> object_store::Result<()> {
        self.inner
            .rename_opts(&self.map_path(from), &self.map_path(to), options)
            .await
    }
}

#[derive(Clone)]
pub struct W3CTestRuntime {
    pub env: Arc<RuntimeEnv>,
}

impl W3CTestRuntime {
    pub fn new(env: Arc<RuntimeEnv>) -> Self {
        Self { env }
    }

    pub fn fresh_env(&self) -> Arc<RuntimeEnv> {
        create_test_runtime_env()
    }

    pub async fn read_file(
        &self,
        url: &str,
    ) -> Result<impl AsyncRead + Unpin + Send + 'static> {
        let parsed_url = Url::parse(url)?;

        let object_store = self.env.object_store_registry.get_store(&parsed_url)?;
        let path = object_store::path::Path::from(parsed_url.path());

        let get_result = object_store
            .get(&path)
            .await
            .with_context(|| format!("Failed to read {url} from shared object store"))?;
        Ok(tokio_util::io::StreamReader::new(
            get_result
                .into_stream()
                .map(|res: object_store::Result<Bytes>| {
                    res.map_err(std::io::Error::other)
                }),
        ))
    }

    pub async fn read_file_to_string(&self, url: &str) -> Result<String> {
        let mut buf = String::new();
        self.read_file(url).await?.read_to_string(&mut buf).await?;
        Ok(buf)
    }

    pub async fn load_to_graph(
        &self,
        url: &str,
        graph: &mut Graph,
        format: RdfFormat,
        base_iri: Option<&str>,
        ignore_errors: bool,
    ) -> Result<()> {
        let parser =
            RdfParser::from_format(format).with_base_iri(base_iri.unwrap_or(url))?;
        let mut stream = parser.for_tokio_async_reader(self.read_file(url).await?);
        while let Some(t) = stream.next().await {
            match t {
                Ok(t) => {
                    graph.insert(&t.into());
                }
                Err(e) => {
                    if !ignore_errors {
                        return Err(e.into());
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn load_graph(
        &self,
        url: &str,
        format: RdfFormat,
        ignore_errors: bool,
    ) -> Result<Graph> {
        let mut graph = Graph::new();
        self.load_to_graph(url, &mut graph, format, None, ignore_errors)
            .await?;
        Ok(graph)
    }

    pub async fn load_to_dataset(
        &self,
        url: &str,
        dataset: &mut Dataset,
        format: RdfFormat,
        ignore_errors: bool,
        unchecked: bool,
    ) -> Result<()> {
        let mut parser = RdfParser::from_format(format).with_base_iri(url)?;
        if unchecked {
            parser = parser.lenient();
        }
        let mut stream = parser.for_tokio_async_reader(self.read_file(url).await?);
        while let Some(q) = stream.next().await {
            match q {
                Ok(q) => {
                    dataset.insert(&q);
                }
                Err(e) => {
                    if !ignore_errors {
                        return Err(e.into());
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn load_dataset(
        &self,
        url: &str,
        format: RdfFormat,
        ignore_errors: bool,
        unchecked: bool,
    ) -> Result<Dataset> {
        let mut dataset = Dataset::new();
        self.load_to_dataset(url, &mut dataset, format, ignore_errors, unchecked)
            .await?;
        Ok(dataset)
    }

    pub async fn load_n3(&self, url: &str, ignore_errors: bool) -> Result<Vec<N3Quad>> {
        let mut quads = Vec::new();
        let mut stream = N3Parser::new()
            .with_base_iri(url)?
            .with_prefix("", format!("{url}#"))?
            .for_tokio_async_reader(self.read_file(url).await?);
        while let Some(q) = stream.next().await {
            match q {
                Ok(q) => quads.push(q),
                Err(e) => {
                    if !ignore_errors {
                        return Err(e.into());
                    }
                }
            }
        }
        Ok(quads)
    }
}

pub fn guess_rdf_format(url: &str) -> Result<RdfFormat> {
    url.rsplit_once('.')
        .and_then(|(_, extension)| RdfFormat::from_extension(extension))
        .with_context(|| format!("Serialization type not found for {url}"))
}
