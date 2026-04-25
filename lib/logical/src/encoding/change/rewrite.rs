use crate::encoding::change::ChangeEncodingNode;
use crate::encoding::object_id::EncodeAsObjectIdNode;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{ExprSchema, Result as DFResult};
use datafusion::logical_expr::{
    Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::col;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionRegistryRef,
};
use std::sync::Arc;

/// Optimizer rule that rewrites [`ChangeEncodingNode`] to either an [`EncodeAsObjectIdNode`] or a
/// [`Projection`] node using built-in encoding UDFs.
#[derive(Debug)]
pub struct LowerChangeEncodingRule {
    registry: RdfFusionFunctionRegistryRef,
}

impl LowerChangeEncodingRule {
    pub fn new(registry: RdfFusionFunctionRegistryRef) -> Self {
        Self { registry }
    }
}

impl OptimizerRule for LowerChangeEncodingRule {
    fn name(&self) -> &str {
        "change-encoding-lowering"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up(|node| {
            let LogicalPlan::Extension(Extension { node: ext_node }) = &node else {
                return Ok(Transformed::no(node));
            };

            let Some(change_encoding) =
                ext_node.as_any().downcast_ref::<ChangeEncodingNode>()
            else {
                return Ok(Transformed::no(node));
            };

            let input = change_encoding.inputs()[0].clone();

            match change_encoding.target_encoding() {
                QuadStorageEncoding::ObjectId(encoding) => {
                    let new_node = EncodeAsObjectIdNode::try_new(
                        input,
                        encoding.object_id_data_type(),
                    )?;

                    Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(new_node),
                    })))
                }
                QuadStorageEncoding::PlainTerm => self.rewrite_to_project(
                    &input,
                    change_encoding.target_encoding().term_type(),
                    BuiltinName::WithPlainTermEncoding,
                ),
                QuadStorageEncoding::String => self.rewrite_to_project(
                    &input,
                    change_encoding.target_encoding().term_type(),
                    BuiltinName::WithStringEncoding,
                ),
            }
        })
    }
}

impl LowerChangeEncodingRule {
    /// Helper method to rewrite an input plan into a Projection using a specific UDF.
    fn rewrite_to_project(
        &self,
        input: &LogicalPlan,
        target_type: &DataType,
        udf_name: BuiltinName,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let udf = self.registry.udf(&FunctionName::Builtin(udf_name))?;

        // Apply the UDF to every column in the input plan, keeping the original column names.
        let exprs = input
            .schema()
            .columns()
            .into_iter()
            .map(|column| {
                let field = input.schema().field_from_column(&column).unwrap();
                if field.data_type() != target_type {
                    let alias = column.name().to_owned();
                    udf.call(vec![col(column)]).alias(alias)
                } else {
                    col(column)
                }
            })
            .collect::<Vec<_>>();

        let project_plan = LogicalPlanBuilder::from(input.clone())
            .project(exprs)?
            .build()?;

        Ok(Transformed::yes(project_plan))
    }
}
