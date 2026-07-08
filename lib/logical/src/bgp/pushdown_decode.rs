use crate::bgp::BgpNode;
use crate::encoding::object_id::DecodeObjectIdsNode;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::sync::Arc;

/// A rule that absorbs [DecodeObjectIds] nodes into a [BgpNode].
#[derive(Debug)]
pub struct BgpDecodePushdownRule;

impl OptimizerRule for BgpDecodePushdownRule {
    fn name(&self) -> &str {
        "BgpDecodePushdownRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            let LogicalPlan::Extension(Extension { node }) = &plan else {
                return Ok(Transformed::no(plan));
            };

            let Some(decode_node) = node.as_any().downcast_ref::<DecodeObjectIdsNode>()
            else {
                return Ok(Transformed::no(plan));
            };

            let input_plans = decode_node.inputs();
            let input_plan = input_plans
                .first()
                .expect("DecodeObjectIdsNode should have one child");
            let LogicalPlan::Extension(Extension { node: bgp_node_ext }) = input_plan
            else {
                return Ok(Transformed::no(plan));
            };

            let Some(bgp) = bgp_node_ext.as_any().downcast_ref::<BgpNode>() else {
                return Ok(Transformed::no(plan));
            };

            let mut new_columns_to_decode = bgp.columns_to_decode.clone();
            for col in decode_node.columns_to_decode() {
                if !new_columns_to_decode.contains(col) {
                    new_columns_to_decode.push(col.clone());
                }
            }

            let new_bgp = BgpNode::try_new(
                bgp.patterns.clone(),
                bgp.filters.clone(),
                bgp.projection.clone(),
                new_columns_to_decode,
            )?;

            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(new_bgp),
            })))
        })
    }
}
