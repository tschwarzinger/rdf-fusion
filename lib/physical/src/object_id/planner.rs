use crate::object_id::exec::DecodeObjectIdsExec;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, plan_err};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, ScalarUDF, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_encoding::object_id::is_object_id_data_type;
use rdf_fusion_logical::encoding::object_id::DecodeObjectIdsNode;
use std::sync::Arc;

#[derive(Debug)]
pub struct DecodeObjectIdsPlanner {
    decode_udf: Arc<ScalarUDF>,
}

impl DecodeObjectIdsPlanner {
    /// Creates a new [`DecodeObjectIdsPlanner`].
    pub fn new(decode_udf: Arc<ScalarUDF>) -> Self {
        Self { decode_udf }
    }
}

#[async_trait]
impl ExtensionPlanner for DecodeObjectIdsPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        let Some(decode_node) = node.as_any().downcast_ref::<DecodeObjectIdsNode>()
        else {
            return Ok(None);
        };

        if physical_inputs.len() != 1 {
            return plan_err!(
                "Expected a single child for DecodeObjectIds but got {}.",
                physical_inputs.len()
            );
        }

        let input_exec = Arc::clone(&physical_inputs[0]);

        let mut projections = Vec::new();
        let decode_set: std::collections::HashSet<String> = decode_node
            .columns_to_decode()
            .iter()
            .map(|c| c.flat_name())
            .collect();

        for field in input_exec.schema().fields() {
            let name = field.name().clone();
            if decode_set.contains(&name) && is_object_id_data_type(field.data_type()) {
                projections.push(
                    crate::object_id::exec::ObjectIdDecodingExecProjection::Decode {
                        source_column: name.clone(),
                        target_column: name,
                    },
                );
            } else {
                projections.push(
                    crate::object_id::exec::ObjectIdDecodingExecProjection::Column {
                        source_column: name.clone(),
                        target_column: name,
                    },
                );
            }
        }

        let exec = DecodeObjectIdsExec::try_new(
            input_exec,
            projections,
            Arc::clone(&self.decode_udf),
        )?;

        Ok(Some(Arc::new(exec)))
    }
}
