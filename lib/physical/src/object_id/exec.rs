use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::assert_eq_or_internal_err;
use datafusion::config::ConfigOptions;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, Time,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use futures::future::BoxFuture;
use futures::Stream;
use rdf_fusion_common::{DFResult, MeasurePoll};
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectIdDecodingExecProjection {
    Column {
        source_column: String,
        target_column: String,
    },
    Decode {
        source_column: String,
        target_column: String,
    },
}

#[derive(Clone)]
pub enum DecodeBatchTask {
    Retain {
        src_idx: usize,
    },
    Decode {
        src_idx: usize,
        dest_field: Arc<Field>,
    },
}

#[derive(Debug, Clone)]
pub struct DecodeObjectIdsExec {
    input: Arc<dyn ExecutionPlan>,
    projections: Vec<ObjectIdDecodingExecProjection>,
    decoding_udf: Arc<ScalarUDF>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DecodeObjectIdsExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        projections: Vec<ObjectIdDecodingExecProjection>,
        decoding_udf: Arc<ScalarUDF>,
    ) -> DFResult<Self> {
        let input_schema = input.schema();
        let mut fields = Vec::with_capacity(projections.len());

        for proj in &projections {
            match proj {
                ObjectIdDecodingExecProjection::Column {
                    source_column: source_column_name,
                    target_column: target_column_name,
                } => {
                    let src_idx = input_schema.index_of(source_column_name)?;
                    let mut field = input_schema.field(src_idx).as_ref().clone();
                    if source_column_name != target_column_name {
                        field = field.with_name(target_column_name.clone());
                    }
                    fields.push(Arc::new(field));
                }
                ObjectIdDecodingExecProjection::Decode {
                    source_column: source_column_name,
                    target_column: target_column_name,
                } => {
                    let src_idx = input_schema.index_of(source_column_name)?;
                    let input_field = input_schema.field(src_idx);
                    let return_type =
                        decoding_udf.return_field_from_args(ReturnFieldArgs {
                            arg_fields: &[Arc::new(input_field.clone())],
                            scalar_arguments: &[],
                        })?;
                    fields.push(Arc::new(
                        return_type
                            .as_ref()
                            .clone()
                            .with_name(target_column_name.clone()),
                    ));
                }
            }
        }
        let schema = Arc::new(Schema::new(fields));

        // Build custom properties based on the input
        let input_props = input.properties();
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(
                input.output_partitioning().partition_count(),
            ),
            input_props.emission_type,
            input.boundedness(),
        ));
        Ok(Self {
            input,
            projections,
            decoding_udf,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for DecodeObjectIdsExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let cols = self
            .projections
            .iter()
            .map(|p| match p {
                ObjectIdDecodingExecProjection::Column {
                    source_column: source_column_name,
                    target_column: target_column_name,
                } => {
                    if source_column_name == target_column_name {
                        source_column_name.clone()
                    } else {
                        format!("{source_column_name} -> {target_column_name}")
                    }
                }
                ObjectIdDecodingExecProjection::Decode {
                    source_column: source_column_name,
                    target_column: target_column_name,
                } => {
                    format!("{source_column_name} -> {target_column_name}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "DecodeObjectIdsExec: projections=[{cols}]")
    }
}

impl ExecutionPlan for DecodeObjectIdsExec {
    fn name(&self) -> &str {
        "DecodeObjectIdsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        assert_eq_or_internal_err!(
            children.len(),
            1,
            "DecodeObjectIdsExec wrong number of children"
        );
        let mut new_plan = DecodeObjectIdsExec::try_new(
            children.swap_remove(0),
            self.projections.clone(),
            Arc::clone(&self.decoding_udf),
        )?;
        new_plan.metrics = self.metrics.clone();
        Ok(Arc::new(new_plan))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let input_schema = self.input.schema();

        let mut decode_tasks = Vec::with_capacity(self.projections.len());
        for (dest_idx, proj) in self.projections.iter().enumerate() {
            match proj {
                ObjectIdDecodingExecProjection::Column {
                    source_column: source_column_name, ..
                } => {
                    let src_idx = input_schema.index_of(source_column_name).unwrap();
                    decode_tasks.push(DecodeBatchTask::Retain { src_idx });
                }
                ObjectIdDecodingExecProjection::Decode {
                    source_column: source_column_name, ..
                } => {
                    let src_idx = input_schema.index_of(source_column_name).unwrap();
                    let dest_field = Arc::new(self.schema.field(dest_idx).clone());
                    decode_tasks.push(DecodeBatchTask::Decode {
                        src_idx,
                        dest_field,
                    });
                }
            }
        }

        let target_batch_size = context.session_config().batch_size();
        let coalescer = BatchCoalescer::new(Arc::clone(&input_schema), target_batch_size);

        let stream = DecodingStream {
            input: input_stream,
            coalescer,
            schema: Arc::clone(&self.schema),
            input_schema,
            decode_tasks,
            decoding_udf: Arc::clone(&self.decoding_udf),
            config_options: Arc::clone(context.session_config().options()),
            baseline_metrics,
            decoding_future: None,
            is_exhausted: false,
        };

        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

pub struct DecodingStream {
    input: SendableRecordBatchStream,
    coalescer: BatchCoalescer,

    // Captured execution context
    schema: Arc<Schema>,
    input_schema: Arc<Schema>,
    decode_tasks: Vec<DecodeBatchTask>,
    decoding_udf: Arc<ScalarUDF>,
    config_options: Arc<ConfigOptions>,
    baseline_metrics: BaselineMetrics,

    // State machine for async processing
    decoding_future: Option<BoxFuture<'static, DFResult<RecordBatch>>>,
    is_exhausted: bool,
}

impl Stream for DecodingStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(mut fut) = self.decoding_future.take() {
                return match fut.as_mut().poll(cx) {
                    Poll::Ready(result) => Poll::Ready(Some(result)),
                    Poll::Pending => {
                        self.decoding_future = Some(fut);
                        Poll::Pending
                    }
                };
            }

            if self.is_exhausted {
                self.coalescer.finish_buffered_batch()?;
                if let Some(final_batch) = self.coalescer.next_completed_batch() {
                    self.spawn_decode_task(final_batch);
                    continue; // Process the newly spawned task
                }
                return Poll::Ready(None);
            }

            match ready!(Pin::new(&mut self.input).poll_next(cx)) {
                Some(Ok(batch)) => {
                    self.coalescer.push_batch(batch)?;
                    if let Some(coalesced_batch) = self.coalescer.next_completed_batch() {
                        self.spawn_decode_task(coalesced_batch);
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    self.is_exhausted = true;
                }
            }
        }
    }
}

impl RecordBatchStream for DecodingStream {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

impl DecodingStream {
    // Helper to spawn the async task cleanly
    fn spawn_decode_task(&mut self, batch: RecordBatch) {
        let fut = decode_batch(
            batch,
            Arc::clone(&self.schema),
            Arc::clone(&self.input_schema),
            self.decode_tasks.clone(),
            Arc::clone(&self.decoding_udf),
            Arc::clone(&self.config_options),
            self.baseline_metrics.elapsed_compute().clone(),
        );
        self.decoding_future = Some(Box::pin(fut));
    }
}

async fn decode_batch(
    batch: RecordBatch,
    schema_captured: Arc<Schema>,
    input_schema_captured: Arc<Schema>,
    decode_tasks: Vec<DecodeBatchTask>,
    decoding_udf: Arc<ScalarUDF>,
    config_options: Arc<ConfigOptions>,
    timer: Time,
) -> DFResult<RecordBatch> {
    let mut final_arrays: Vec<ArrayRef> = Vec::with_capacity(decode_tasks.len());

    for task in decode_tasks {
        match task {
            DecodeBatchTask::Retain { src_idx } => {
                final_arrays.push(Arc::clone(batch.column(src_idx)));
            }
            DecodeBatchTask::Decode {
                src_idx,
                dest_field,
            } => {
                let col_array = batch.column(src_idx);
                let args = ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(Arc::clone(col_array))],
                    arg_fields: vec![Arc::new(
                        input_schema_captured.field(src_idx).clone(),
                    )],
                    number_rows: batch.num_rows(),
                    return_field: dest_field,
                    config_options: Arc::clone(&config_options),
                };

                let result = match decoding_udf.as_async() {
                    None => decoding_udf.invoke_with_args(args)?,
                    Some(async_udf) => {
                        let future = async_udf.invoke_async_with_args(args);
                        MeasurePoll {
                            inner: Box::pin(future),
                            time_metric: timer.clone(),
                        }
                        .await?
                    }
                };

                final_arrays.push(result.into_array(batch.num_rows())?);
            }
        }
    }

    RecordBatch::try_new(schema_captured, final_arrays).map_err(|e| e.into())
}
