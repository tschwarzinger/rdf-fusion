//! Keeps the RDF term extension types intact during logical optimization.
//!
//! RDF Fusion identifies the encoding of RDF term columns by their Arrow extension type (see
//! [`EncodingName::extension_type_name`]). Some of DataFusion's optimizer rules rewrite plans
//! in a way that drops field metadata. For example, `PropagateEmptyRelation` replaces the empty
//! side of an outer join with `CAST(NULL AS <type>)`, which loses the extension type.
//!
//! [`PreserveRdfTermExtensionTypes`] wraps such a rule and restores the extension types that were
//! present in the schema of the plan before the rewrite.

use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchema};
use datafusion::logical_expr::{Cast, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingName, with_rdf_term_extension_type};
use std::sync::Arc;

/// Wraps an [`OptimizerRule`] and restores RDF term extension types that the rule dropped.
///
/// See the [module documentation](self) for further information.
#[derive(Debug)]
pub struct PreserveRdfTermExtensionTypes {
    inner: Arc<dyn OptimizerRule + Send + Sync>,
}

impl PreserveRdfTermExtensionTypes {
    /// Creates a new [`PreserveRdfTermExtensionTypes`].
    pub fn new(inner: Arc<dyn OptimizerRule + Send + Sync>) -> Self {
        Self { inner }
    }
}

impl OptimizerRule for PreserveRdfTermExtensionTypes {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        self.inner.apply_order()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let expected_schema = Arc::clone(plan.schema());
        let result = self.inner.rewrite(plan, config)?;
        if !result.transformed {
            return Ok(result);
        }

        result.map_data(|plan| restore_rdf_term_extension_types(plan, &expected_schema))
    }
}

/// Restores the RDF term extension types of `expected_schema` in the output of `plan`.
///
/// If `plan` already carries all extension types, it is returned unchanged. Otherwise, a projection
/// that re-annotates the affected columns is added on top of `plan`. Columns whose data type
/// differs from the `expected_schema` are not modified.
pub fn restore_rdf_term_extension_types(
    plan: LogicalPlan,
    expected_schema: &DFSchema,
) -> DFResult<LogicalPlan> {
    let actual_schema = Arc::clone(plan.schema());
    if actual_schema.fields().len() != expected_schema.fields().len() {
        return Ok(plan);
    }

    let mut modified = false;
    let mut exprs = Vec::with_capacity(actual_schema.fields().len());
    for ((qualifier, actual), expected) in
        actual_schema.iter().zip(expected_schema.fields().iter())
    {
        let column = Expr::Column(Column::new(qualifier.cloned(), actual.name()));
        let expected_encoding = EncodingName::try_from_field(expected);
        let actual_encoding = EncodingName::try_from_field(actual);
        match expected_encoding {
            Some(encoding)
                if actual_encoding.is_none()
                    && actual.data_type() == expected.data_type() =>
            {
                // The cast (to the same storage type) ensures that the physical plan carries the
                // extension type. The alias ensures that the logical plan carries it.
                let field =
                    with_rdf_term_extension_type(actual.as_ref().clone(), encoding);
                let metadata = FieldMetadata::from(field.metadata());
                let expr =
                    Expr::Cast(Cast::new_from_field(Box::new(column), Arc::new(field)))
                        .alias_qualified_with_metadata(
                            qualifier.cloned(),
                            actual.name(),
                            Some(metadata),
                        );
                exprs.push(expr);
                modified = true;
            }
            _ => exprs.push(column),
        }
    }

    if !modified {
        return Ok(plan);
    }

    LogicalPlanBuilder::from(plan).project(exprs)?.build()
}
