use crate::rdf_files::RdfParserOptions;
use crate::rdf_files::rdf::RdfDataSink;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileOpener, FileScanConfig, FileSinkConfig, FileSource,
};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::Stream;
use object_store::{GetOptions, ObjectStore};
use oxrdfio::RdfParser;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::QuadStorageEncoding;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

/// A [`FileFormat`] implementation for RDF files.
#[derive(Debug)]
pub struct RdfFileFormat {
    options: RdfParserOptions,
}

impl RdfFileFormat {
    /// Creates a new [`RdfFileFormat`] with the given options.
    pub fn new(options: RdfParserOptions) -> Self {
        Self { options }
    }
}

#[async_trait]
impl FileFormat for RdfFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.options.format.file_extension().to_string()
    }

    fn get_ext_with_compression(&self, _c: &FileCompressionType) -> DFResult<String> {
        Ok(self.get_ext())
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[object_store::ObjectMeta],
    ) -> DFResult<SchemaRef> {
        Ok(Arc::clone(
            QuadStorageEncoding::PlainTerm.quad_schema().inner(),
        ))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _schema: SchemaRef,
        _object: &object_store::ObjectMeta,
    ) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(
            QuadStorageEncoding::PlainTerm.quad_schema().inner(),
        ))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSourceExec::new(Arc::new(conf))))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if conf.table_paths.len() != 1 {
            return Err(DataFusionError::Internal(
                "RdfDataSink only supports single file output".to_string(),
            ));
        }
        let sink_path = conf.table_paths[0].prefix().clone();
        let store = state
            .runtime_env()
            .object_store(conf.object_store_url.clone())?;

        let sink = Arc::new(RdfDataSink::new_rdf(
            store,
            sink_path,
            self.options.format,
            Arc::clone(&conf.output_schema),
        ));

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)))
    }

    fn file_source(&self, schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(RdfFileSource {
            options: self.options.clone(),
            table_schema: schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

#[derive(Debug)]
struct RdfFileSource {
    options: RdfParserOptions,
    table_schema: TableSchema,
    metrics: ExecutionPlanMetricsSet,
}

impl FileSource for RdfFileSource {
    fn create_file_opener(
        &self,
        store: Arc<dyn ObjectStore>,
        _conf: &FileScanConfig,
        _batch_size: usize,
    ) -> DFResult<Arc<dyn FileOpener>> {
        Ok(Arc::new(RdfFileOpener::new(self.options.clone(), store)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(RdfFileSource {
            options: self.options.clone(),
            table_schema: self.table_schema.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "rdf"
    }
}

/// A [`FileOpener`] for RDF files.
struct RdfFileOpener {
    options: RdfParserOptions,
    store: Arc<dyn ObjectStore>,
}

impl RdfFileOpener {
    fn new(options: RdfParserOptions, store: Arc<dyn ObjectStore>) -> Self {
        Self { options, store }
    }
}

impl FileOpener for RdfFileOpener {
    fn open(
        &self,
        file: PartitionedFile,
    ) -> DFResult<datafusion::datasource::physical_plan::FileOpenFuture> {
        let options = self.options.clone();
        let schema = Arc::clone(QuadStorageEncoding::PlainTerm.quad_schema().inner());
        let store = Arc::clone(&self.store);

        Ok(Box::pin(async move {
            let get_result = store.get_opts(file.path(), GetOptions::default()).await?;
            let reader = tokio_util::io::StreamReader::new(
                get_result
                    .into_stream()
                    .map(|res| res.map_err(std::io::Error::other)),
            );

            let oxigraph_format = options.format.to_oxigraph().ok_or_else(|| {
                DataFusionError::Internal(
                    "Parquet is not supported by RdfFileFormat".to_string(),
                )
            })?;

            let mut parser_builder = RdfParser::from_format(oxigraph_format);
            if let Some(base_iri) = options.base_iri {
                parser_builder = parser_builder
                    .with_base_iri(base_iri.to_string())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }
            if options.rename_blank_nodes {
                parser_builder = parser_builder.rename_blank_nodes();
            }
            if options.without_named_graphs {
                parser_builder = parser_builder.without_named_graphs();
            }
            if let Some(default_graph) = options.default_graph {
                parser_builder = parser_builder.with_default_graph(default_graph);
            }

            let parser = parser_builder.for_tokio_async_reader(reader);

            let target_batch_size = 8192; // Default batch size

            use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
            use futures::StreamExt;
            use futures::stream::try_unfold;
            use rdf_fusion_encoding::plain_term::PlainTermQuadsBuilder;

            struct ParserStreamState<R: tokio::io::AsyncRead + Unpin + Send + 'static> {
                parser: oxrdfio::TokioAsyncReaderQuadParser<R>,
                builder: PlainTermQuadsBuilder,
                target_batch_size: usize,
            }

            let initial_state = ParserStreamState {
                parser,
                builder: PlainTermQuadsBuilder::with_capacity(target_batch_size),
                target_batch_size,
            };

            let stream = try_unfold(initial_state, |mut state| async move {
                loop {
                    match state.parser.next().await {
                        Some(Ok(quad)) => {
                            state.builder.append_quad(quad.as_ref());
                            if state.builder.len() >= state.target_batch_size {
                                let batch = state.builder.finish().into_record_batch();
                                state.builder = PlainTermQuadsBuilder::with_capacity(
                                    state.target_batch_size,
                                );
                                return Ok(Some((batch, state)));
                            }
                        }
                        Some(Err(e)) => {
                            return Err(DataFusionError::External(Box::new(e)));
                        }
                        None => {
                            let result = if !state.builder.is_empty() {
                                let batch = state.builder.finish().into_record_batch();
                                state.builder = PlainTermQuadsBuilder::with_capacity(0);
                                Some((batch, state))
                            } else {
                                None
                            };
                            return Ok(result);
                        }
                    }
                }
            });

            let record_batch_stream =
                RecordBatchStreamAdapter::new(schema, Box::pin(stream));
            let stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
                Box::pin(record_batch_stream);
            Ok(stream)
        }))
    }
}
