use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::DefaultTableSource;
use datafusion::datasource::listing::ListingTable;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::quads::QUADS_TABLE_DEFAULT_NAME;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use std::sync::Arc;

pub struct ParquetQuadStoragePlanner {
    table: Arc<ListingTable>,
}

impl ParquetQuadStoragePlanner {
    pub fn new(table: Arc<ListingTable>) -> Self {
        Self { table }
    }
}

#[async_trait]
impl ExtensionPlanner for ParquetQuadStoragePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &datafusion::execution::context::SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<QuadPatternNode>() else {
            return Ok(None);
        };

        let table_source =
            DefaultTableSource::new(Arc::clone(&self.table) as Arc<dyn TableProvider>);
        let mut plan_builder = LogicalPlanBuilder::scan(
            QUADS_TABLE_DEFAULT_NAME,
            Arc::new(table_source),
            None,
        )?;

        let pattern = node.quad_pattern();
        let filters = pattern.compute_filters(node.storage_encoding())?;
        for filter in filters {
            plan_builder = plan_builder.filter(filter)?;
        }

        let projections = pattern.compute_projection();
        let expected_schema = node.schema();
        let mut exprs = Vec::new();
        for (expr, name) in projections {
            let field = expected_schema.field_with_name(None, &name)?;
            exprs.push(
                datafusion::logical_expr::cast(expr, field.data_type().clone())
                    .alias(name),
            );
        }
        plan_builder = plan_builder.project(exprs)?;

        let plan = plan_builder.build()?;
        let df = DataFrame::new(session_state.clone(), plan);
        let physical_plan = df.create_physical_plan().await?;

        Ok(Some(physical_plan))
    }
}
