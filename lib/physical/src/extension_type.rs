//! Ensures that physical plans carry the RDF term extension types of their logical counterparts.
//!
//! RDF Fusion identifies the encoding of RDF term columns using Arrow extension types (see
//! [`rdf_fusion_encoding::EncodingName::extension_type_name`]). The logical plan always carries
//! this information. However, physical plans that are created by extension planners (e.g., the
//! scans of a storage layer that reads Parquet files) may not attach the extension types to their
//! output schema. As expressions are evaluated on the physical schema, this information must be
//! restored.
//!
//! [`RdfTermExtensionTypePlanner`] wraps an [`ExtensionPlanner`] and, if necessary, wraps the
//! created [`ExecutionPlan`] in an [`AnnotateRdfTermsExec`] that attaches the extension types of
//! the logical schema to the physical schema. This is a zero-copy operation.

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingName, with_rdf_term_extension_type};
use std::fmt::Formatter;
use std::sync::Arc;

/// Wraps an [`ExtensionPlanner`] and ensures that the created [`ExecutionPlan`] carries the RDF term
/// extension types of the planned logical node.
///
/// See the [module documentation](self) for further information.
pub struct RdfTermExtensionTypePlanner {
    inner: Arc<dyn ExtensionPlanner + Send + Sync>,
}

impl std::fmt::Debug for RdfTermExtensionTypePlanner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdfTermExtensionTypePlanner").finish()
    }
}

impl RdfTermExtensionTypePlanner {
    /// Creates a new [`RdfTermExtensionTypePlanner`].
    pub fn new(inner: Arc<dyn ExtensionPlanner + Send + Sync>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl ExtensionPlanner for RdfTermExtensionTypePlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let plan = self
            .inner
            .plan_extension(
                planner,
                node,
                logical_inputs,
                physical_inputs,
                session_state,
                planning_ctx,
            )
            .await?;
        plan.map(|plan| annotate_rdf_terms(plan, node.schema()))
            .transpose()
    }
}

/// Attaches the RDF term extension types of `logical_schema` to the schema of `plan`.
///
/// Returns `plan` unchanged if its schema already carries all extension types. Returns an error if
/// the physical schema declares a different encoding than the logical schema.
pub fn annotate_rdf_terms(
    plan: Arc<dyn ExecutionPlan>,
    logical_schema: &DFSchema,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let physical_schema = plan.schema();
    if physical_schema.fields().len() != logical_schema.fields().len() {
        return plan_err!(
            "Physical plan has {} columns, while the logical plan has {} columns.",
            physical_schema.fields().len(),
            logical_schema.fields().len()
        );
    }

    let mut modified = false;
    let mut fields = Vec::with_capacity(physical_schema.fields().len());
    for (physical, logical) in physical_schema
        .fields()
        .iter()
        .zip(logical_schema.fields().iter())
    {
        let logical_encoding = EncodingName::try_from_field(logical);
        let physical_encoding = EncodingName::try_from_field(physical);
        match (logical_encoding, physical_encoding) {
            // Only annotate columns whose storage type matches the logical plan. Other columns
            // (e.g., object ids that are decoded by a parent plan) are left unchanged.
            (Some(logical_encoding), None)
                if physical.data_type() == logical.data_type() =>
            {
                fields.push(with_rdf_term_extension_type(
                    physical.as_ref().clone(),
                    logical_encoding,
                ));
                modified = true;
            }
            (Some(logical_encoding), Some(physical_encoding))
                if logical_encoding != physical_encoding =>
            {
                return plan_err!(
                    "Column '{}' has encoding {physical_encoding} but the logical plan expects {logical_encoding}.",
                    physical.name()
                );
            }
            _ => fields.push(physical.as_ref().clone()),
        }
    }

    if !modified {
        return Ok(plan);
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        physical_schema.metadata().clone(),
    ));
    Ok(Arc::new(AnnotateRdfTermsExec::try_new(plan, schema)?))
}

/// Replaces the schema of the `input` with a schema that carries the RDF term extension types.
///
/// The new schema must only differ in the field metadata. Batches are not copied.
#[derive(Debug)]
pub struct AnnotateRdfTermsExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl AnnotateRdfTermsExec {
    /// Creates a new [`AnnotateRdfTermsExec`].
    pub fn try_new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> DFResult<Self> {
        let eq_properties = input
            .properties()
            .eq_properties
            .clone()
            .with_new_schema(schema)?;
        let properties = input
            .properties()
            .as_ref()
            .clone()
            .with_eq_properties(eq_properties);
        Ok(Self {
            input,
            properties: Arc::new(properties),
        })
    }
}

impl DisplayAs for AnnotateRdfTermsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AnnotateRdfTermsExec")
    }
}

impl ExecutionPlan for AnnotateRdfTermsExec {
    fn name(&self) -> &str {
        "AnnotateRdfTermsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // While the order is maintained, we must not allow optimizers to push operators (e.g.,
        // sorts) below this node, as their expressions would then be evaluated on the
        // unannotated schema. The output ordering is still part of the plan properties.
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            Arc::clone(&children[0]),
            self.schema(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema = self.schema();
        let stream_schema = Arc::clone(&schema);
        let stream = self.input.execute(partition, context)?.map(move |batch| {
            let batch = batch?;
            let options =
                RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(&stream_schema),
                batch.columns().to_vec(),
                &options,
            )?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
