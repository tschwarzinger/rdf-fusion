//! Keeps the RDF term extension types intact during logical optimization.
//!
//! RDF Fusion identifies the encoding of RDF term columns by their Arrow extension type (see
//! [`EncodingName::extension_type_name`]). Some of DataFusion's optimizer rules rewrite plans
//! in a way that drops field metadata. For example, `PropagateEmptyRelation` replaces the empty
//! side of an outer join with `CAST(NULL AS <type>)`, which loses the extension type.
//!
//! [`PreserveRdfTermExtensionTypes`] wraps such a rule and restores the extension types that were
//! present in the schema of the plan before the rewrite.

use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EXTENSION_TYPE_NAME_KEY, EncodingName, rdf_term_lit};
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

        result.map_data(|plan| annotate_null_literals(plan, &expected_schema))
    }
}

/// If `plan` is a projection, annotates its NULL literals that lost the extension type of the
/// corresponding column in `expected_schema` (e.g., `CAST(NULL AS <type>)` created by
/// `PropagateEmptyRelation`).
///
/// The literal itself is annotated, as casting the column instead would be folded into a literal
/// without metadata by the expression simplifier.
fn annotate_null_literals(
    plan: LogicalPlan,
    expected_schema: &DFSchema,
) -> DFResult<LogicalPlan> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(plan);
    };
    if projection.expr.len() != expected_schema.fields().len() {
        return Ok(LogicalPlan::Projection(projection));
    }

    let mut modified = false;
    let exprs = projection
        .expr
        .iter()
        .zip(expected_schema.fields())
        .map(|(expr, expected)| {
            let Some(encoding) = EncodingName::try_from_field(expected) else {
                return Ok(expr.clone());
            };
            // The schema of the projection cannot be used for this check, as rules may create
            // the projection with the (annotated) schema of the replaced plan.
            if !is_unannotated_null_literal(expr) {
                return Ok(expr.clone());
            }

            modified = true;
            let (relation, name) = expr.qualified_name();
            let null = ScalarValue::try_new_null(expected.data_type())?;
            Ok(rdf_term_lit(encoding, null).alias_qualified(relation, name))
        })
        .collect::<DFResult<Vec<_>>>()?;

    if !modified {
        return Ok(LogicalPlan::Projection(projection));
    }

    LogicalPlanBuilder::from(Arc::unwrap_or_clone(projection.input))
        .project(exprs)?
        .build()
}

/// Returns whether `expr` is a (possibly aliased or cast) NULL literal without an RDF term
/// extension type.
fn is_unannotated_null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => is_unannotated_null_literal(&alias.expr),
        // A cast does not propagate the metadata of the literal.
        Expr::Cast(cast) => {
            matches!(cast.expr.as_ref(), Expr::Literal(value, _) if value.is_null())
        }
        Expr::Literal(value, metadata) => {
            let is_annotated = metadata.as_ref().is_some_and(|metadata| {
                metadata.inner().contains_key(EXTENSION_TYPE_NAME_KEY)
            });
            value.is_null() && !is_annotated
        }
        _ => false,
    }
}
