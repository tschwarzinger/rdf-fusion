use crate::ingest::RdfParserOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, exec_datafusion_err};
use datafusion::datasource::TableType;
use datafusion::error::Result as DFCoreResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Partitioning, PlanProperties, SendableRecordBatchStream,
};
use futures::stream::try_unfold;
use oxrdfio::{RdfParser, TokioAsyncReaderQuadParser};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::plain_term::PlainTermQuadsBuilder;
use rdf_fusion_model::{DFResult, IriParseError};
use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncRead;
use tracing::info;

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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
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
            projection.cloned(),
            limit,
            self.track_progress,
        )))
    }
}

/// The execution plan for reading RDF data from an [`AsyncRead`] stream.
struct RdfParserExec<R: AsyncRead + Unpin + Send + 'static> {
    /// The quad schema.
    schema: SchemaRef,
    /// The parser that reads the RDF data. The Mutex allows us to move the parser out of it once
    /// the stream has been created.
    parser: Mutex<Option<TokioAsyncReaderQuadParser<R>>>,
    /// The properties of the execution plan.
    properties: Arc<PlanProperties>,
    /// Whether to track and log the progress of the execution
    track_progress: bool,
}

impl<R: AsyncRead + Unpin + Send + 'static> RdfParserExec<R> {
    /// Creates a new [`RdfParserExec`].
    pub fn new(
        parser: TokioAsyncReaderQuadParser<R>,
        schema: SchemaRef,
        _projection: Option<Vec<usize>>,
        _limit: Option<usize>,
        track_progress: bool,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            parser: Mutex::new(Some(parser)),
            schema,
            properties: Arc::new(properties),
            track_progress,
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> Debug for RdfParserExec<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RdfParserExec")
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> DisplayAs for RdfParserExec<R> {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RdfParserExec:")
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> ExecutionPlan for RdfParserExec<R> {
    fn name(&self) -> &str {
        "RdfParserExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFCoreResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DFCoreResult<SendableRecordBatchStream> {
        let parser = self.parser.lock().unwrap().take().ok_or_else(|| {
            exec_datafusion_err!("ExecutionPlan has already been executed")
        })?;

        let schema = Arc::clone(&self.schema);
        let target_batch_size = context.session_config().batch_size();

        struct ParserStreamState<R: AsyncRead + Unpin + Send + 'static> {
            parser: TokioAsyncReaderQuadParser<R>,
            builder: PlainTermQuadsBuilder,
            target_batch_size: usize,
            progress: ProgressState,
            track_progress: bool,
        }

        let initial_state = ParserStreamState {
            parser,
            builder: PlainTermQuadsBuilder::with_capacity(target_batch_size),
            target_batch_size,
            progress: ProgressState {
                pending_batch: 0,
                total_processed: 0,
            },
            track_progress: self.track_progress,
        };

        let stream = try_unfold(initial_state, |mut state| async move {
            loop {
                match state.parser.next().await {
                    Some(Ok(quad)) => {
                        state.builder.append_quad(quad.as_ref());
                        state.progress.pending_batch += 1;
                        state.progress.total_processed += 1;

                        // Periodically log progress if enabled (every 100k rows)
                        if state.track_progress
                            && state.progress.total_processed % 100_000 == 0
                        {
                            info!(
                                "RDF Scan Progress: {} quads processed",
                                state.progress.total_processed
                            );
                        }

                        // Yield a batch when we hit the configured batch size. Otherwise, keep parsing.
                        if state.progress.pending_batch >= state.target_batch_size {
                            let batch = state.builder.finish().into_record_batch();
                            state.progress.pending_batch = 0;
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
                        // Stream exhausted. Log final completion and yield any remaining data.
                        if state.track_progress {
                            info!(
                                "RDF Scan Complete: Total of {} quads processed",
                                state.progress.total_processed
                            );
                        }

                        return if state.progress.pending_batch > 0 {
                            let batch = state.builder.finish().into_record_batch();
                            state.progress.pending_batch = 0;
                            state.builder = PlainTermQuadsBuilder::with_capacity(0);

                            Ok(Some((batch, state)))
                        } else {
                            Ok(None)
                        };
                    }
                }
            }
        });

        // 4. Return the RecordBatchStreamAdapter
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            Box::pin(stream),
        )))
    }
}

/// The state that tracks the progress of the parser.
#[derive(Debug, Clone, Default)]
struct ProgressState {
    /// The number of rows in the pending batch.
    pending_batch: usize,
    /// The total number of processed rows.
    total_processed: usize,
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
