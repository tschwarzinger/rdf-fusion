use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, exec_datafusion_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::stream::try_unfold;
use oxrdfio::TokioAsyncReaderQuadParser;
use rdf_fusion_common::{DFResult, QuadRef};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::plain_term::PlainTermQuadsBuilder;
use rdf_fusion_encoding::string::StringQuadsBuilder;
use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncRead;

use crate::rdf_files::detect_encoding_from_schema;

/// The execution plan for reading RDF data from an [`AsyncRead`] stream.
pub struct RdfParserExec<R: AsyncRead + Unpin + Send + 'static> {
    /// The quad schema.
    schema: SchemaRef,
    /// The parser that reads the RDF data. The Mutex allows us to move the parser out of it once
    /// the stream has been created.
    parser: Mutex<Option<TokioAsyncReaderQuadParser<R>>>,
    /// The properties of the execution plan.
    properties: Arc<PlanProperties>,
}

impl<R: AsyncRead + Unpin + Send + 'static> RdfParserExec<R> {
    /// Creates a new [`RdfParserExec`].
    pub fn new(parser: TokioAsyncReaderQuadParser<R>, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            // This assumes that the underlying stream is bounded.
            Boundedness::Bounded,
        );

        Self {
            parser: Mutex::new(Some(parser)),
            schema,
            properties: Arc::new(properties),
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
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let parser = self.parser.lock().unwrap().take().ok_or_else(|| {
            exec_datafusion_err!("ExecutionPlan has already been executed")
        })?;

        let schema = Arc::clone(&self.schema);
        let encoding = detect_encoding_from_schema(&schema)?;
        let target_batch_size = context.session_config().batch_size();

        enum QuadsBuilder {
            PlainTerm(Box<PlainTermQuadsBuilder>),
            String(Box<StringQuadsBuilder>),
        }

        impl QuadsBuilder {
            fn new(encoding: &QuadStorageEncoding, capacity: usize) -> Self {
                match encoding {
                    QuadStorageEncoding::PlainTerm => Self::PlainTerm(Box::new(
                        PlainTermQuadsBuilder::with_capacity(capacity),
                    )),
                    QuadStorageEncoding::String => Self::String(Box::new(
                        StringQuadsBuilder::with_capacity(capacity),
                    )),
                    _ => unreachable!("Unsupported encoding for RDF parser"),
                }
            }

            fn append_quad(&mut self, quad: QuadRef<'_>) {
                match self {
                    Self::PlainTerm(b) => b.append_quad(quad),
                    Self::String(b) => b.append_quad(quad),
                }
            }

            fn finish(self) -> RecordBatch {
                match self {
                    Self::PlainTerm(b) => b.finish().into_record_batch(),
                    Self::String(b) => b.finish().into_record_batch(),
                }
            }
        }

        struct ParserStreamState<R: AsyncRead + Unpin + Send + 'static> {
            parser: TokioAsyncReaderQuadParser<R>,
            builder: QuadsBuilder,
            encoding: QuadStorageEncoding,
            target_batch_size: usize,
            progress: ProgressState,
        }

        let initial_state = ParserStreamState {
            parser,
            builder: QuadsBuilder::new(&encoding, target_batch_size),
            encoding,
            target_batch_size,
            progress: ProgressState {
                pending_batch: 0,
                total_processed: 0,
            },
        };

        let stream = try_unfold(initial_state, |mut state| async move {
            loop {
                match state.parser.next().await {
                    Some(Ok(quad)) => {
                        state.builder.append_quad(quad.as_ref());
                        state.progress.pending_batch += 1;
                        state.progress.total_processed += 1;

                        // Yield a batch when we hit the configured batch size. Otherwise, keep parsing.
                        if state.progress.pending_batch >= state.target_batch_size {
                            let batch = state.builder.finish();
                            state.progress.pending_batch = 0;
                            state.builder = QuadsBuilder::new(
                                &state.encoding,
                                state.target_batch_size,
                            );

                            return Ok(Some((batch, state)));
                        }
                    }
                    Some(Err(e)) => {
                        return Err(DataFusionError::External(Box::new(e)));
                    }
                    None => {
                        return if state.progress.pending_batch > 0 {
                            let batch = state.builder.finish();
                            state.progress.pending_batch = 0;
                            state.builder = QuadsBuilder::new(&state.encoding, 0);

                            Ok(Some((batch, state)))
                        } else {
                            Ok(None)
                        };
                    }
                }
            }
        });

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
