use crate::rdf_files::scan::RdfFileQuadPatternScanExec;
use crate::rdf_files::{RdfFileManager, RdfFileQuadStoragePlanner, RdfFileSourceConfig};
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use futures::StreamExt;
use rdf_fusion_common::config::RdfFileStorageOptions;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{
    BlankNodeMatchingMode, GraphName, NamedNodePattern, StorageError, TermPattern,
    TriplePattern, Variable,
};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::QuadStorageSnapshot;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::sync::Arc;

pub struct RdfFileQuadStorageSnapshot {
    manager: RdfFileManager,
    sources: Vec<(GraphName, RdfFileSourceConfig)>,
    encoding: QuadStorageEncoding,
    options: RdfFileStorageOptions,
}

impl RdfFileQuadStorageSnapshot {
    /// Creates a new [`RdfFileQuadStorageSnapshot`].
    pub fn new(
        manager: RdfFileManager,
        sources: Vec<(GraphName, RdfFileSourceConfig)>,
        encoding: QuadStorageEncoding,
        options: RdfFileStorageOptions,
    ) -> Self {
        Self {
            manager,
            sources,
            encoding,
            options,
        }
    }
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
            self.options.clone(),
        ))]
    }

    async fn named_graphs(
        &self,
        session: &SessionState,
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
        let schema = pattern.compute_schema(&self.encoding);
        let all_quads = Arc::new(
            RdfFileQuadPatternScanExec::new(
                pattern,
                self.manager.clone(),
                self.sources.clone(),
                self.encoding.clone(),
                Arc::clone(schema.inner()),
                session,
                self.options.clone(),
            )
            .await?,
        );

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

    async fn len(&self, session: &SessionState) -> Result<usize, StorageError> {
        let pattern = QuadPattern::all_quads();
        let schema = Arc::clone(self.encoding.quad_schema().inner());

        let all_quads = Arc::new(
            RdfFileQuadPatternScanExec::new(
                pattern,
                self.manager.clone(),
                self.sources.clone(),
                self.encoding.clone(),
                schema,
                session,
                self.options.clone(),
            )
            .await?,
        ) as Arc<dyn ExecutionPlan>;

        let mut total_count = 0;
        let mut stream = execute_stream(all_quads, session.task_ctx())?;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(StorageError::from)?;
            total_count += batch.num_rows();
        }

        Ok(total_count)
    }
}
