use crate::rdf_files::manager::RdfFileManager;
use crate::rdf_files::planner::RdfFileQuadStoragePlanner;
use crate::rdf_files::rdf::RdfFileSourceConfig;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_stream,
};
use futures::{StreamExt, TryStreamExt};
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult, GraphName, NamedNodePattern};
use rdf_fusion_common::{StorageError, TermPattern, TriplePattern, Variable};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::object_id::ObjectIdMapping;
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;

/// A quad storage that reads from data dumps.
#[derive(Clone)]
pub struct RdfFileQuadStorage {
    manager: RdfFileManager,
    sources: Arc<RwLock<Vec<(GraphName, RdfFileSourceConfig)>>>,
}

impl RdfFileQuadStorage {
    /// Creates a new [`RdfFileQuadStorage`] with the given sources.
    pub fn new(sources: Vec<(GraphName, RdfFileSourceConfig)>) -> Self {
        Self {
            manager: RdfFileManager::new(),
            sources: Arc::new(RwLock::new(sources)),
        }
    }

    /// Adds a source to the storage.
    pub fn add_source(&self, graph_name: GraphName, source: RdfFileSourceConfig) {
        self.sources.write().unwrap().push((graph_name, source));
    }
}

#[async_trait]
impl QuadStorage for RdfFileQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        QuadStorageEncoding::PlainTerm
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        None
    }

    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        Ok(Arc::new(RdfFileQuadStorageSnapshot {
            manager: self.manager.clone(),
            sources: self.sources.read().unwrap().clone(),
        }))
    }

    async fn begin_transaction(
        &self,
        _state: &SessionState,
    ) -> Result<Box<dyn QuadStorageTransaction>, StorageError> {
        Err(StorageError::Other("Data dump storage is read-only".into()))
    }

    async fn optimize(&self, _state: &SessionState) -> Result<(), StorageError> {
        Ok(())
    }

    async fn validate(&self, _state: &SessionState) -> Result<(), StorageError> {
        Ok(())
    }
}

pub struct RdfFileQuadStorageSnapshot {
    manager: RdfFileManager,
    sources: Vec<(GraphName, RdfFileSourceConfig)>,
}

#[async_trait]
impl QuadStorageSnapshot for RdfFileQuadStorageSnapshot {
    async fn planners(
        &self,
        _context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn datafusion::physical_planner::ExtensionPlanner + Send + Sync>> {
        vec![Arc::new(RdfFileQuadStoragePlanner::new(
            self.manager.clone(),
            self.sources.clone(),
        ))]
    }

    async fn named_graphs(
        &self,
        _session: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        let pattern = QuadPattern::new(
            ActiveGraph::AnyNamedGraph,
            Some(Variable::new_unchecked(COL_GRAPH)),
            TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked(COL_SUBJECT)),
                predicate: NamedNodePattern::Variable(Variable::new_unchecked(
                    COL_PREDICATE,
                )),
                object: TermPattern::Variable(Variable::new_unchecked(COL_OBJECT)),
            },
            BlankNodeMatchingMode::Filter,
        );
        let schema = pattern.compute_schema(&QuadStorageEncoding::PlainTerm);
        let all_quads = Arc::new(RdfFileQuadPatternScanExec::new(
            pattern,
            self.manager.clone(),
            self.sources.clone(),
            Arc::clone(schema.inner()),
        ));

        const COL_GRAPH_IDX: usize = 0;
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new(COL_GRAPH, COL_GRAPH_IDX)),
            COL_GRAPH.to_string(),
        )]);

        let dedup_plan = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            all_quads,
            Arc::clone(schema.inner()),
        )?);

        Ok(dedup_plan)
    }

    async fn len(&self, _session: &SessionState) -> Result<usize, StorageError> {
        let pattern = QuadPattern::all_quads();
        let schema = Arc::clone(QuadStorageEncoding::PlainTerm.quad_schema().inner());

        let all_quads = Arc::new(RdfFileQuadPatternScanExec::new(
            pattern,
            self.manager.clone(),
            self.sources.clone(),
            schema,
        )) as Arc<dyn ExecutionPlan>;

        let mut total_count = 0;
        let mut stream = execute_stream(all_quads, _session.task_ctx())?;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(StorageError::from)?;
            total_count += batch.num_rows();
        }

        Ok(total_count)
    }
}

/// A physical execution plan for scanning a [`RdfFileQuadStorage`].
///
/// This plan wraps the underlying scans (e.g., `DataSourceExec`) to provide a cleaner
/// representation in the query plan.
#[derive(Debug, Clone)]
pub struct RdfFileQuadPatternScanExec {
    quad_pattern: QuadPattern,
    manager: RdfFileManager,
    sources: Vec<(GraphName, RdfFileSourceConfig)>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    inner: Arc<tokio::sync::OnceCell<Arc<dyn ExecutionPlan>>>,
}

impl RdfFileQuadPatternScanExec {
    /// Creates a new [`RdfFileQuadPatternScanExec`].
    pub fn new(
        quad_pattern: QuadPattern,
        manager: RdfFileManager,
        sources: Vec<(GraphName, RdfFileSourceConfig)>,
        schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            quad_pattern,
            manager,
            sources,
            schema,
            properties: Arc::new(properties),
            inner: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// Returns the inner execution plan, initializing it if necessary.
    pub async fn inner(&self, state: &SessionState) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner
            .get_or_try_init(|| async { self.build_inner_plan(state).await })
            .await
            .cloned()
    }

    /// Builds the inner execution plan.
    async fn build_inner_plan(
        &self,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        use crate::rdf_files::rdf::RdfParserOptions;
        use datafusion::common::DataFusionError;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::projection::ProjectionExec;
        use datafusion::physical_plan::union::UnionExec;

        let mut plans = Vec::new();
        for (graph_name, source) in &self.sources {
            if self.quad_pattern.active_graph.matches(graph_name) {
                let options = RdfParserOptions::with_format(source.format)
                    .with_base_iri(source.url.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .with_default_graph(graph_name.clone())
                    .with_rename_blank_nodes(true);

                let mem_table = self
                    .manager
                    .get_or_parse(source.url.clone(), options, session_state)
                    .await?;
                let plan = mem_table.scan(session_state, None, &[], None).await?;
                plans.push(plan);
            }
        }

        if plans.is_empty() {
            // Return empty plan if no tables match the graph
            use datafusion::physical_plan::empty::EmptyExec;
            return Ok(Arc::new(EmptyExec::new(Arc::clone(
                QuadStorageEncoding::PlainTerm.quad_schema().inner(),
            ))));
        }

        let mut inner_plan: Arc<dyn ExecutionPlan> = if plans.len() == 1 {
            plans.remove(0)
        } else {
            UnionExec::try_new(plans)?
        };

        let filters = self
            .quad_pattern
            .compute_filters(&QuadStorageEncoding::PlainTerm)?;
        if !filters.is_empty() {
            let filter_expr = conjunction(filters).unwrap();
            let physical_filter_expr = session_state.create_physical_expr(
                filter_expr,
                &QuadStorageEncoding::PlainTerm.quad_schema(),
            )?;
            inner_plan = Arc::new(FilterExec::try_new(physical_filter_expr, inner_plan)?);
        }

        // De-duplicate quads
        inner_plan = Arc::new(CoalescePartitionsExec::new(inner_plan));

        let group_exprs = inner_plan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                (
                    Arc::new(Column::new(field.name(), i))
                        as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
                    field.name().clone(),
                )
            })
            .collect::<Vec<_>>();

        use datafusion::physical_plan::aggregates::{
            AggregateExec, AggregateMode, PhysicalGroupBy,
        };
        inner_plan = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(group_exprs),
            vec![],
            vec![],
            inner_plan,
            Arc::clone(QuadStorageEncoding::PlainTerm.quad_schema().inner()),
        )?);

        // We must project the inner plan to match the logical node's schema.
        // The inner plan has the GSPO schema (graph, subject, predicate, object).
        let mut physical_exprs = Vec::new();
        for (component, name) in self.quad_pattern.compute_projected_components() {
            physical_exprs.push((
                Arc::new(Column::new(component.column_name(), component.gspo_index()))
                    as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
                name,
            ));
        }

        Ok(Arc::new(ProjectionExec::try_new(
            physical_exprs,
            inner_plan,
        )?))
    }
}

impl DisplayAs for RdfFileQuadPatternScanExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "DataDumpQuadPatternScanExec: quad_pattern={:?}",
            self.quad_pattern
        )
    }
}

impl ExecutionPlan for RdfFileQuadPatternScanExec {
    fn name(&self) -> &str {
        "DataDumpQuadPatternScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Hide internal plan
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.quad_pattern.clone(),
            self.manager.clone(),
            self.sources.clone(),
            Arc::clone(&self.schema),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let this = self.clone();
        let stream_future = async move {
            let session_state = SessionStateBuilder::new()
                .with_config(context.session_config().clone())
                .with_runtime_env(context.runtime_env())
                .build();
            let inner_plan = this.inner(&session_state).await?;
            inner_plan.execute(partition, context)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(stream_future).try_flatten(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use rdf_fusion_logical::quad_pattern::QuadPattern;

    #[tokio::test]
    async fn test_data_dump_quad_pattern_scan_exec_inner() {
        let manager = RdfFileManager::new();
        let sources = vec![];
        let quad_pattern = QuadPattern::all_quads();
        let schema = QuadStorageEncoding::PlainTerm.quad_schema().inner().clone();

        let exec =
            RdfFileQuadPatternScanExec::new(quad_pattern, manager, sources, schema);

        let ctx = SessionContext::new();
        let inner_plan = exec.inner(&ctx.state()).await.unwrap();

        // Should be an EmptyExec since sources are empty
        assert!(
            inner_plan
                .as_any()
                .is::<datafusion::physical_plan::empty::EmptyExec>()
        );
    }
}
