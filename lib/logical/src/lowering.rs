use crate::check_same_schema;
use crate::extend::ExtendNode;
use crate::extend::rewrite_extend_node;
use crate::join::SparqlJoinNode;
use crate::join::rewrite_sparql_join_node;
use crate::minus::MinusNode;
use crate::minus::rewrite_minus_node;
use crate::paths::PropertyPathNode;
use crate::paths::rewrite_property_path_node;
use crate::patterns::PatternNode;
use crate::patterns::rewrite_pattern_node;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use rdf_fusion_common::DFResult;
use rdf_fusion_extensions::RdfFusionContextView;

/// An optimizer rule that lowers all SPARQL-specific operators to DataFusion primitives.
///
/// This combines the individual lowering rules (for `Extend`, `Minus`, property paths, SPARQL
/// joins, and patterns) into a single, bottom-up pass over the logical plan. Children are
/// lowered before their parents so that the rewrites (e.g. the joins produced for `Minus` and
/// SPARQL joins) always build on already-lowered inputs. Each individual lowering is fully
/// responsible for lowering the nodes it produces (e.g. a property path is lowered into
/// patterns which are in turn lowered), so a single pass over the original plan is sufficient.
#[derive(Debug)]
pub struct RdfFusionLoweringRule {
    /// The RDF Fusion configuration.
    context: RdfFusionContextView,
}

impl RdfFusionLoweringRule {
    /// Creates a new [RdfFusionLoweringRule].
    pub fn new(context: RdfFusionContextView) -> Self {
        Self { context }
    }

    /// Lowers `plan` if it is a SPARQL-specific extension node.
    fn try_lower(&self, plan: &LogicalPlan) -> DFResult<Option<LogicalPlan>> {
        let LogicalPlan::Extension(Extension { node }) = plan else {
            return Ok(None);
        };
        let node = node.as_any();

        if let Some(node) = node.downcast_ref::<ExtendNode>() {
            let new_plan = rewrite_extend_node(node)?;
            check_same_schema(node.schema(), new_plan.schema())?;
            Ok(Some(new_plan))
        } else if let Some(node) = node.downcast_ref::<MinusNode>() {
            let new_plan = rewrite_minus_node(node, &self.context)?;
            check_same_schema(node.schema(), new_plan.schema())?;
            Ok(Some(new_plan))
        } else if let Some(node) = node.downcast_ref::<PropertyPathNode>() {
            let new_plan = rewrite_property_path_node(node, &self.context)?;
            check_same_schema(node.schema(), new_plan.schema())?;
            Ok(Some(new_plan))
        } else if let Some(node) = node.downcast_ref::<SparqlJoinNode>() {
            let new_plan = rewrite_sparql_join_node(node, &self.context)?;
            check_same_schema(node.schema(), new_plan.schema())?;
            Ok(Some(new_plan))
        } else if let Some(node) = node.downcast_ref::<PatternNode>() {
            let new_plan = rewrite_pattern_node(node, &self.context)?;
            check_same_schema(node.schema(), new_plan.schema())?;
            Ok(Some(new_plan))
        } else {
            Ok(None)
        }
    }
}

impl OptimizerRule for RdfFusionLoweringRule {
    fn name(&self) -> &str {
        "rdf_fusion_lowering"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            let new_plan = match self.try_lower(&plan)? {
                Some(new_plan) => Transformed::yes(new_plan),
                None => Transformed::no(plan),
            };
            Ok(new_plan)
        })
    }
}
