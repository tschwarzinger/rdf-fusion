use crate::delta::objectids::DeltaObjectIdMapping;
use crate::delta::objectids::encoding::stream::ObjectIdEncodingStream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_datafusion_err;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

#[derive(Debug)]
pub struct EncodeAsObjectIdDeltaExec {
    /// The physical plan of the input DataFrame
    input: Arc<dyn ExecutionPlan>,
    /// The mapping used for encoding
    mapping: Arc<DeltaObjectIdMapping>,
    /// The schema of the result
    output_schema: SchemaRef,
    /// The properties of the plan
    properties: Arc<PlanProperties>,
    /// The number of partitions in the plan
    num_running_streams: Arc<AtomicI64>,
}

impl EncodeAsObjectIdDeltaExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        mapping: Arc<DeltaObjectIdMapping>,
        output_schema: SchemaRef,
    ) -> DFResult<Self> {
        let eq_properties = EquivalenceProperties::new(Arc::clone(&output_schema));
        let properties = input
            .properties()
            .as_ref()
            .clone()
            .with_eq_properties(eq_properties);
        let partition_count = i64::try_from(properties.partitioning.partition_count())
            .map_err(|_| {
                exec_datafusion_err!("Could not convert number of partitions to i64")
            })?;

        Ok(Self {
            input,
            mapping,
            output_schema,
            properties: Arc::new(properties),
            num_running_streams: Arc::new(AtomicI64::new(partition_count)),
        })
    }
}

impl DisplayAs for EncodeAsObjectIdDeltaExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "DeltaObjectIdEncodingExec")
    }
}

impl ExecutionPlan for EncodeAsObjectIdDeltaExec {
    fn name(&self) -> &str {
        "DeltaObjectIdEncodingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            Arc::clone(&children[0]),
            Arc::clone(&self.mapping),
            Arc::clone(&self.schema()),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(ObjectIdEncodingStream::new(
            input_stream,
            Arc::clone(&self.mapping),
            Arc::clone(&self.num_running_streams),
        )))
    }
}
