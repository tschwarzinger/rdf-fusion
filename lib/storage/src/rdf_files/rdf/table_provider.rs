use crate::rdf_files::RdfParserOptions;
use crate::rdf_files::rdf::exec::RdfParserExec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::exec_datafusion_err;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use object_store::ObjectStoreExt;
use oxrdfio::{RdfParser, TokioAsyncReaderQuadParser};
use rdf_fusion_common::{DFResult, IriParseError};
use rdf_fusion_encoding::QuadStorageEncoding;
use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncRead;
use tokio_util::bytes::Bytes;
use url::Url;

/// Creates a [`TableProvider`] that reads RDF data from an [`AsyncRead`] stream.
pub struct RdfParserTableProvider<R: AsyncRead + Unpin + Send + 'static> {
    /// The quad schema.
    schema: SchemaRef,
    /// The parser that reads the RDF data. The Mutex allows us to move the parser out of it, once
    /// the table has been scanned.
    quad_parser: Mutex<Option<TokioAsyncReaderQuadParser<R>>>,
    /// Whether to track the progress of the stream.
    track_progress: bool,
}

impl<R: AsyncRead + Unpin + Send + 'static> RdfParserTableProvider<R> {
    /// Creates a new [`RdfParserTableProvider`].
    pub fn new(read: R, options: RdfParserOptions) -> Result<Self, IriParseError> {
        let mut reader = RdfParser::from_format(options.format);

        if let Some(base_iri) = options.base_iri {
            reader = reader.with_base_iri(base_iri.to_string())?;
        }

        if options.rename_blank_nodes {
            reader = reader.rename_blank_nodes();
        }

        if options.without_named_graphs {
            reader = reader.without_named_graphs();
        }

        if let Some(default_graph) = options.default_graph {
            reader = reader.with_default_graph(default_graph);
        }

        let quad_parser = reader.for_tokio_async_reader(read);
        let schema = QuadStorageEncoding::PlainTerm.quad_schema();
        Ok(Self {
            schema: Arc::clone(schema.inner()),
            quad_parser: Mutex::new(Some(quad_parser)),
            track_progress: false,
        })
    }

    /// Enables progress tracking for this table provider.
    pub fn with_track_progress(self, track_progress: bool) -> Self {
        Self {
            track_progress,
            ..self
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> Debug for RdfParserTableProvider<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RdfParserTableProvider")
    }
}

#[async_trait::async_trait]
impl<R: AsyncRead + Unpin + Send + 'static> TableProvider for RdfParserTableProvider<R> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Extract the parser. If it's already gone, the stream was consumed.
        let mut guard = self.quad_parser.lock().unwrap();
        let parser = guard.take().ok_or_else(|| {
            exec_datafusion_err!("RDF stream has already been consumed")
        })?;

        // Create and return the custom physical plan, passing the track_progress flag
        Ok(Arc::new(RdfParserExec::new(
            parser,
            Arc::clone(&self.schema),
            self.track_progress,
        )))
    }
}

/// Creates a [`TableProvider`] that reads RDF data from a URL.
pub struct UrlRdfParserTableProvider {
    /// The URL of the RDF data.
    url: String,
    /// The options for the RDF parser.
    options: RdfParserOptions,
    /// The quad schema.
    schema: SchemaRef,
    /// Whether to track the progress of the stream.
    track_progress: bool,
}

impl UrlRdfParserTableProvider {
    /// Creates a new [`UrlRdfParserTableProvider`].
    pub fn try_new(url: String, options: RdfParserOptions) -> DFResult<Self> {
        let schema = QuadStorageEncoding::PlainTerm.quad_schema();
        Ok(Self {
            url,
            options,
            schema: Arc::clone(schema.inner()),
            track_progress: false,
        })
    }

    /// Returns the URL of the RDF data.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the options for the RDF parser.
    pub fn options(&self) -> &RdfParserOptions {
        &self.options
    }

    /// Enables progress tracking for this table provider.
    pub fn with_track_progress(self, track_progress: bool) -> Self {
        Self {
            track_progress,
            ..self
        }
    }
}

impl Debug for UrlRdfParserTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UrlRdfParserTableProvider")
            .field("url", &self.url)
            .field("options", &self.options)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for UrlRdfParserTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let runtime = state.runtime_env();
        let parsed_url = Url::parse(&self.url)
            .map_err(|e| exec_datafusion_err!("Invalid URL {}: {e}", self.url))?;
        let object_store = runtime
            .object_store_registry
            .get_store(&parsed_url)
            .map_err(|e| exec_datafusion_err!("Failed to get object store: {e}"))?;
        let path = object_store::path::Path::from(parsed_url.path());
        let get_result = object_store.get(&path).await.map_err(|e| {
            exec_datafusion_err!("Failed to get object {}: {e}", self.url)
        })?;
        let stream = get_result
            .into_stream()
            .map(|res: object_store::Result<Bytes>| res.map_err(std::io::Error::other));
        let read = tokio_util::io::StreamReader::new(stream);

        let mut reader = RdfParser::from_format(self.options.format);

        if let Some(base_iri) = &self.options.base_iri {
            reader = reader
                .with_base_iri(base_iri.to_string())
                .map_err(|e| exec_datafusion_err!("Invalid base IRI: {e}"))?;
        }

        if self.options.rename_blank_nodes {
            reader = reader.rename_blank_nodes();
        }

        if self.options.without_named_graphs {
            reader = reader.without_named_graphs();
        }

        if let Some(default_graph) = &self.options.default_graph {
            reader = reader.with_default_graph(default_graph.clone());
        }

        let parser = reader.for_tokio_async_reader(read);

        // Create and return the custom physical plan, passing the track_progress flag
        Ok(Arc::new(RdfParserExec::new(
            parser,
            Arc::clone(&self.schema),
            self.track_progress,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use insta::assert_snapshot;
    use oxrdfio::RdfFormat;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_parse_turtle_and_print_dataframe() {
        let turtle_data = b"
            @prefix ex: <http://example.org/> .

            ex:subject1 ex:predicate1 ex:object1 .
            ex:subject1 ex:predicate2 \"Hello DataFusion!\" .
            ex:subject2 ex:predicate3 \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .
        "
        .to_vec();
        let reader = Cursor::new(turtle_data);

        let table_provider = RdfParserTableProvider::new(
            reader,
            RdfParserOptions::with_format(RdfFormat::Turtle),
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("rdf", Arc::new(table_provider)).unwrap();

        let df = ctx.sql("SELECT * FROM rdf").await.unwrap();
        assert_snapshot!(
            df.to_string().await.unwrap(),
            @"
        +-------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
        | graph | subject                                                                         | predicate                                                                         | object                                                                                                       |
        +-------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
        |       | {term_type: 0, value: http://example.org/subject1, data_type: , language_tag: } | {term_type: 0, value: http://example.org/predicate1, data_type: , language_tag: } | {term_type: 0, value: http://example.org/object1, data_type: , language_tag: }                               |
        |       | {term_type: 0, value: http://example.org/subject1, data_type: , language_tag: } | {term_type: 0, value: http://example.org/predicate2, data_type: , language_tag: } | {term_type: 2, value: Hello DataFusion!, data_type: http://www.w3.org/2001/XMLSchema#string, language_tag: } |
        |       | {term_type: 0, value: http://example.org/subject2, data_type: , language_tag: } | {term_type: 0, value: http://example.org/predicate3, data_type: , language_tag: } | {term_type: 2, value: 42, data_type: http://www.w3.org/2001/XMLSchema#integer, language_tag: }               |
        +-------+---------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------+
        "
        );
    }
}
