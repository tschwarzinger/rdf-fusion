//! Verifies that physical plans carry the RDF term extension types of their logical counterparts.
//!
//! RDF Fusion identifies the encoding of RDF term columns using Arrow extension types (see
//! [`rdf_fusion_encoding::EncodingName::extension_type_name`]). The logical plan always carries
//! this information. As expressions are evaluated on the *physical* schema, the physical plans
//! created by extension planners (e.g., the scans of a storage layer) must carry the extension types
//! as well. For example, a storage layer that reads Parquet files must use a table schema that
//! includes the extension types (see
//! [`QuadStorageEncoding::quad_schema`](rdf_fusion_encoding::QuadStorageEncoding::quad_schema)).
//!
//! [`RdfTermExtensionTypePlanner`] wraps an [`ExtensionPlanner`] and verifies this invariant, such
//! that a violation results in a descriptive error during planning instead of an error in some
//! function that is evaluated on the output.

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, plan_err};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::EncodingName;
use std::fmt::Formatter;
use std::sync::Arc;

/// Wraps an [`ExtensionPlanner`] and verifies that the created [`ExecutionPlan`] carries the RDF
/// term extension types of the planned logical node.
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
        if let Some(plan) = &plan {
            verify_rdf_term_extension_types(node.name(), plan.as_ref(), node.schema())?;
        }
        Ok(plan)
    }
}

/// Verifies that `plan` carries the RDF term extension types declared in `logical_schema`.
///
/// Columns whose data type differs from the logical schema are not checked (e.g., object ids that
/// are decoded by a parent plan).
pub fn verify_rdf_term_extension_types(
    node_name: &str,
    plan: &dyn ExecutionPlan,
    logical_schema: &DFSchema,
) -> DFResult<()> {
    let physical_schema = plan.schema();
    if physical_schema.fields().len() != logical_schema.fields().len() {
        return plan_err!(
            "Physical plan for '{node_name}' has {} columns, while the logical plan has {} columns.",
            physical_schema.fields().len(),
            logical_schema.fields().len()
        );
    }

    for (physical, logical) in physical_schema
        .fields()
        .iter()
        .zip(logical_schema.fields().iter())
    {
        let Some(logical_encoding) = EncodingName::try_from_field(logical) else {
            continue;
        };
        if physical.data_type() != logical.data_type() {
            continue;
        }

        let physical_encoding = EncodingName::try_from_field(physical);
        if physical_encoding != Some(logical_encoding) {
            return plan_err!(
                "Physical plan for '{node_name}' produces column '{}' with encoding {:?}, but the \
                logical plan expects {logical_encoding}. Physical plans must annotate RDF term \
                columns with the extension type of their encoding (see \
                QuadStorageEncoding::quad_schema).",
                physical.name(),
                physical_encoding
            );
        }
    }
    Ok(())
}
