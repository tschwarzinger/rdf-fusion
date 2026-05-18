use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use futures::{Stream, StreamExt};
use rdf_fusion_common::DFResult;
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// An [`ExecutionPlan`] that verifies that certain columns are not null.
#[derive(Debug)]
pub struct VerifyNotNullExec {
    inner: Arc<dyn ExecutionPlan>,
    columns_to_verify: Vec<usize>,
    plan_properties: Arc<PlanProperties>,
}

impl VerifyNotNullExec {
    pub fn try_new(
        inner: Arc<dyn ExecutionPlan>,
        columns_to_verify: Vec<usize>,
    ) -> DFResult<Self> {
        let schema = inner.schema();
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                if columns_to_verify.contains(&i) {
                    let mut field = (**field).clone();
                    field.set_nullable(false);
                    field
                } else {
                    (**field).clone()
                }
            })
            .collect::<Vec<_>>();
        let new_schema = Arc::new(Schema::new(fields));

        let plan_properties = Arc::new(
            inner.properties().as_ref().clone().with_eq_properties(
                inner
                    .properties()
                    .eq_properties
                    .clone()
                    .with_new_schema(new_schema)?,
            ),
        );

        Ok(Self {
            inner,
            columns_to_verify,
            plan_properties,
        })
    }
}

impl DisplayAs for VerifyNotNullExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "VerifyNotNullExec: columns={:?}", self.columns_to_verify)
    }
}

impl ExecutionPlan for VerifyNotNullExec {
    fn name(&self) -> &str {
        "VerifyNotNullExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.plan_properties.eq_properties.schema())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            Arc::clone(&children[0]),
            self.columns_to_verify.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        Ok(Box::pin(VerifyNotNullStream {
            inner: inner_stream,
            columns_to_verify: self.columns_to_verify.clone(),
            schema: self.schema(),
        }))
    }
}

struct VerifyNotNullStream {
    inner: SendableRecordBatchStream,
    columns_to_verify: Vec<usize>,
    schema: SchemaRef,
}

impl Stream for VerifyNotNullStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx).map(|opt| {
            opt.map(|batch_result: DFResult<RecordBatch>| {
                batch_result.and_then(|batch| {
                    for &col_idx in &self.columns_to_verify {
                        let array = batch.column(col_idx);
                        if array.null_count() > 0 {
                            return Err(DataFusionError::Execution(format!(
                                "Null value found in non-nullable column {}",
                                self.schema.field(col_idx).name()
                            )));
                        }
                    }
                    // We need to return the batch with the new schema (non-nullable fields)
                    RecordBatch::try_new(
                        Arc::clone(&self.schema),
                        batch.columns().to_vec(),
                    )
                    .map_err(DataFusionError::from)
                })
            })
        })
    }
}

impl RecordBatchStream for VerifyNotNullStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
