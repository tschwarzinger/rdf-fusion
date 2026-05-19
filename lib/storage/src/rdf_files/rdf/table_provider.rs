use crate::rdf_files::RdfFileScanOptions;
use crate::rdf_files::rdf::exec::RdfParserExec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{ScalarValue, exec_datafusion_err, exec_err};
use datafusion::datasource::TableType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use futures::StreamExt;
use object_store::ObjectStoreExt;
use oxrdfio::{RdfParser, TokioAsyncReaderQuadParser};
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{DFResult, IriParseError, RdfFormat};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::{QuadStorageEncoding, TermEncoding};
use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use thiserror::Error;
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

#[derive(Debug, Error)]
pub enum RdfParserTableProviderError {
    #[error(transparent)]
    IriParseError(#[from] IriParseError),
    #[error("Unsupported RDF format: {0}")]
    UnsupportedRdfFormat(RdfFormat),
}

impl<R: AsyncRead + Unpin + Send + 'static> RdfParserTableProvider<R> {
    /// Creates a new [`RdfParserTableProvider`].
    pub fn new(
        read: R,
        options: RdfFileScanOptions,
    ) -> Result<Self, RdfParserTableProviderError> {
        let oxigraph_format = options.format.to_oxigraph().ok_or(
            RdfParserTableProviderError::UnsupportedRdfFormat(options.format),
        )?;
        let mut reader = RdfParser::from_format(oxigraph_format);

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
        let schema = QuadStorageEncoding::String.quad_schema();
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
    options: RdfFileScanOptions,
    /// The quad schema.
    schema: SchemaRef,
    /// Whether to track the progress of the stream.
    track_progress: bool,
}

impl UrlRdfParserTableProvider {
    /// Creates a new [`UrlRdfParserTableProvider`].
    pub fn try_new(url: String, options: RdfFileScanOptions) -> DFResult<Self> {
        let schema = QuadStorageEncoding::String.quad_schema();
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
    pub fn options(&self) -> &RdfFileScanOptions {
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
        if self.options.format == RdfFormat::Parquet {
            return self.scan_parquet(state).await;
        }

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

        let oxigraph_format = self.options.format.to_oxigraph().ok_or_else(|| {
            exec_datafusion_err!("Parquet is not supported by UrlRdfParserTableProvider")
        })?;

        let mut reader = RdfParser::from_format(oxigraph_format);

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

impl UrlRdfParserTableProvider {
    async fn scan_parquet(
        &self,
        state: &dyn Session,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let runtime = state.runtime_env();
        let parsed_url = Url::parse(&self.url)
            .map_err(|e| exec_datafusion_err!("Invalid URL {}: {e}", self.url))?;
        let object_store = runtime
            .object_store_registry
            .get_store(&parsed_url)
            .map_err(|e| exec_datafusion_err!("Failed to get object store: {e}"))?;
        let path = object_store::path::Path::from(parsed_url.path());

        let meta = object_store.head(&path).await.map_err(|e| {
            exec_datafusion_err!("Failed to get object metadata {}: {e}", self.url)
        })?;
        let format = ParquetFormat::default();
        let inferred_schema = format
            .infer_schema(state, &object_store, std::slice::from_ref(&meta))
            .await?;

        // Ensure we use Utf8 instead of Utf8View or LargeUtf8 for string columns
        let fields: Vec<datafusion::arrow::datatypes::Field> = inferred_schema
            .fields()
            .iter()
            .map(|f| {
                if matches!(
                    f.data_type(),
                    datafusion::arrow::datatypes::DataType::Utf8View
                        | datafusion::arrow::datatypes::DataType::LargeUtf8
                ) {
                    f.as_ref()
                        .clone()
                        .with_data_type(datafusion::arrow::datatypes::DataType::Utf8)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let inferred_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields));

        // Strict Schema Validation & Encoding Detection
        let fields = inferred_schema.fields();
        let is_gspo = fields.len() == 4
            && fields[0].name() == COL_GRAPH
            && fields[1].name() == COL_SUBJECT
            && fields[2].name() == COL_PREDICATE
            && fields[3].name() == COL_OBJECT;
        let is_spo = fields.len() == 3
            && fields[0].name() == COL_SUBJECT
            && fields[1].name() == COL_PREDICATE
            && fields[2].name() == COL_OBJECT;

        if !is_gspo && !is_spo {
            return exec_err!(
                "Parquet file must have exactly GSPO or SPO columns. Found: {:?}",
                fields.iter().map(|f| f.name()).collect::<Vec<_>>()
            );
        }

        // Detection: use 'subject' column
        let subject_idx = if is_gspo { 1 } else { 0 };
        let subject_field = &fields[subject_idx];
        let encoding_data_type = subject_field.data_type();

        // Check if all columns have the same type
        for field in fields {
            if field.data_type() != encoding_data_type {
                return exec_err!(
                    "All columns in Parquet file must have the same encoding. Found {:?} and {:?}",
                    encoding_data_type,
                    field.data_type()
                );
            }
        }

        let object_store_url = ObjectStoreUrl::parse(format!(
            "{}://{}",
            parsed_url.scheme(),
            parsed_url.host_str().unwrap_or_default()
        ))
        .map_err(|e| exec_datafusion_err!("Failed to parse ObjectStoreUrl: {e}"))?;

        let file_source =
            format.file_source(TableSchema::new(Arc::clone(&inferred_schema), vec![]));
        let statistics = datafusion::common::Statistics::new_unknown(&inferred_schema);

        let file_scan_config = FileScanConfigBuilder::new(object_store_url, file_source)
            .with_file_group(FileGroup::new(vec![PartitionedFile::from(meta.clone())]))
            .with_statistics(statistics)
            .build();

        let mut plan = format.create_physical_plan(state, file_scan_config).await?;

        if is_spo {
            // Add virtual graph column
            let mut exprs = Vec::new();

            // Graph column (null)
            let null_scalar = if encoding_data_type == PLAIN_TERM_ENCODING.data_type() {
                ScalarValue::try_new_null(PLAIN_TERM_ENCODING.data_type())?
            } else if encoding_data_type == STRING_ENCODING.data_type() {
                ScalarValue::try_new_null(STRING_ENCODING.data_type())?
            } else {
                return exec_err!(
                    "Unsupported encoding data type: {:?}",
                    encoding_data_type
                );
            };

            exprs.push((
                Arc::new(Literal::new(null_scalar))
                    as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
                COL_GRAPH.to_string(),
            ));

            for (i, field) in fields.iter().enumerate() {
                exprs.push((
                    Arc::new(Column::new(field.name(), i))
                        as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
                    field.name().to_string(),
                ));
            }

            plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
        }

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use insta::assert_snapshot;
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
            RdfFileScanOptions::with_format(RdfFormat::Turtle),
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("rdf", Arc::new(table_provider)).unwrap();

        let df = ctx.sql("SELECT * FROM rdf").await.unwrap();
        assert_snapshot!(
            df.to_string().await.unwrap(),
            @r#"
        +-------+-------------------------------+---------------------------------+--------------------------------------------------+
        | graph | subject                       | predicate                       | object                                           |
        +-------+-------------------------------+---------------------------------+--------------------------------------------------+
        |       | <http://example.org/subject1> | <http://example.org/predicate1> | <http://example.org/object1>                     |
        |       | <http://example.org/subject1> | <http://example.org/predicate2> | "Hello DataFusion!"                              |
        |       | <http://example.org/subject2> | <http://example.org/predicate3> | "42"^^<http://www.w3.org/2001/XMLSchema#integer> |
        +-------+-------------------------------+---------------------------------+--------------------------------------------------+
        "#
        );
    }

    #[tokio::test]
    async fn test_parse_parquet_and_print_dataframe() {
        use datafusion::dataframe::DataFrameWriteOptions;
        use datafusion::logical_expr::col;
        use object_store::memory::InMemory;
        use rdf_fusion_common::quads::COL_GRAPH;
        use rdf_fusion_common::{GraphNameRef, NamedNode, Quad};
        use rdf_fusion_encoding::plain_term::PlainTermQuadsBuilder;

        let ctx = SessionContext::new();
        ctx.runtime_env().register_object_store(
            &Url::parse("memory://").unwrap(),
            Arc::new(InMemory::new()),
        );

        // 1. Create some quads and write to Parquet
        let mut builder = PlainTermQuadsBuilder::with_capacity(1);
        builder.append_quad(
            Quad::new(
                NamedNode::new_unchecked("http://example.org/s"),
                NamedNode::new_unchecked("http://example.org/p"),
                NamedNode::new_unchecked("http://example.org/o"),
                GraphNameRef::DefaultGraph,
            )
            .as_ref(),
        );
        let batch = builder.finish().into_record_batch();

        let df = ctx.read_batch(batch).unwrap();
        df.clone()
            .write_parquet(
                "memory:///test.parquet",
                DataFrameWriteOptions::new().with_single_file_output(true),
                None,
            )
            .await
            .unwrap();

        // 2. Query using UrlRdfParserTableProvider
        let provider = UrlRdfParserTableProvider::try_new(
            "memory:///test.parquet".to_owned(),
            RdfFileScanOptions::with_format(RdfFormat::Parquet),
        )
        .unwrap();

        ctx.register_table("rdf_parquet", Arc::new(provider))
            .unwrap();

        let df_read = ctx.sql("SELECT * FROM rdf_parquet").await.unwrap();
        assert_snapshot!(
            df_read.to_string().await.unwrap(),
            @"
        +-------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+
        | graph | subject                                                                  | predicate                                                                | object                                                                   |
        +-------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+
        |       | {term_type: 0, value: http://example.org/s, data_type: , language_tag: } | {term_type: 0, value: http://example.org/p, data_type: , language_tag: } | {term_type: 0, value: http://example.org/o, data_type: , language_tag: } |
        +-------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+
        "
        );

        // 3. Test SPO-only Parquet (no graph column)
        df.select(vec![col(COL_SUBJECT), col(COL_PREDICATE), col(COL_OBJECT)])
            .unwrap()
            .write_parquet(
                "memory:///test_spo.parquet",
                DataFrameWriteOptions::new().with_single_file_output(true),
                None,
            )
            .await
            .unwrap();

        let spo_provider = UrlRdfParserTableProvider::try_new(
            "memory:///test_spo.parquet".to_owned(),
            RdfFileScanOptions::with_format(RdfFormat::Parquet),
        )
        .unwrap();

        ctx.register_table("rdf_spo_parquet", Arc::new(spo_provider))
            .unwrap();
        let df_spo = ctx.sql("SELECT * FROM rdf_spo_parquet").await.unwrap();

        // Should have a virtual graph column
        assert!(df_spo.schema().field_with_name(None, COL_GRAPH).is_ok());
        assert_snapshot!(
            df_spo.to_string().await.unwrap(),
            @"
        +-------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+
        | graph | subject                                                                  | predicate                                                                | object                                                                   |
        +-------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+
        |       | {term_type: 0, value: http://example.org/s, data_type: , language_tag: } | {term_type: 0, value: http://example.org/p, data_type: , language_tag: } | {term_type: 0, value: http://example.org/o, data_type: , language_tag: } |
        +-------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+--------------------------------------------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_parse_string_encoded_parquet() {
        use datafusion::dataframe::DataFrameWriteOptions;
        use object_store::memory::InMemory;

        let ctx = SessionContext::new();
        ctx.runtime_env().register_object_store(
            &Url::parse("memory://").unwrap(),
            Arc::new(InMemory::new()),
        );

        // Create String encoded data
        let df = ctx
            .sql(
                "SELECT CAST(NULL AS STRING) as graph,
                        'http://example.org/s' as subject,
                        'http://example.org/p' as predicate,
                        'http://example.org/o' as object",
            )
            .await
            .unwrap();

        df.write_parquet(
            "memory:///test.parquet",
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await
        .unwrap();

        let provider = UrlRdfParserTableProvider::try_new(
            "memory:///test.parquet".to_owned(),
            RdfFileScanOptions::with_format(RdfFormat::Parquet),
        )
        .unwrap();

        ctx.register_table("rdf_str_parquet", Arc::new(provider))
            .unwrap();

        let df_read = ctx.sql("SELECT * FROM rdf_str_parquet").await.unwrap();
        assert_snapshot!(
            df_read.to_string().await.unwrap(),
            @"
        +-------+----------------------+----------------------+----------------------+
        | graph | subject              | predicate            | object               |
        +-------+----------------------+----------------------+----------------------+
        |       | http://example.org/s | http://example.org/p | http://example.org/o |
        +-------+----------------------+----------------------+----------------------+
        "
        );
    }
}
