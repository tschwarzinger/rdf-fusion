use crate::parquet::planner::ParquetQuadStoragePlanner;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::catalog::default_table_source::DefaultTableSource;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlanBuilder, col};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::ExtensionPlanner;
use rdf_fusion_common::StorageError;
use rdf_fusion_common::quads::{COL_GRAPH, QUADS_TABLE_DEFAULT_NAME};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::QuadStorageSnapshot;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use url::Url;

/// A snapshot of a [`ParquetQuadStorage`].
pub struct ParquetQuadStorageSnapshot {
    url: Url,
    table: Arc<ListingTable>,
}

impl ParquetQuadStorageSnapshot {
    /// Creates a new [`ParquetQuadStorageSnapshot`].
    pub fn new(
        url: Url,
        _encoding: QuadStorageEncoding,
        table: Arc<ListingTable>,
    ) -> Self {
        Self { url, table }
    }
}

impl Debug for ParquetQuadStorageSnapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetQuadStorageSnapshot")
            .field("url", &self.url)
            .finish()
    }
}

#[async_trait]
impl QuadStorageSnapshot for ParquetQuadStorageSnapshot {
    async fn planners(
        &self,
        _context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        vec![Arc::new(ParquetQuadStoragePlanner::new(Arc::clone(
            &self.table,
        )))]
    }

    async fn named_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        let table_source = Arc::new(DefaultTableSource::new(
            Arc::clone(&self.table) as Arc<dyn TableProvider>
        ));
        let plan =
            LogicalPlanBuilder::scan(QUADS_TABLE_DEFAULT_NAME, table_source, None)?
                .build()?;
        let df = DataFrame::new(state.clone(), plan);

        let df = df
            .filter(col(COL_GRAPH).is_not_null())?
            .select([col(COL_GRAPH)])?
            .distinct()?;

        df.create_physical_plan()
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))
    }

    async fn len(&self, state: &SessionState) -> Result<usize, StorageError> {
        let table_source = Arc::new(DefaultTableSource::new(
            Arc::clone(&self.table) as Arc<dyn TableProvider>
        ));
        let plan =
            LogicalPlanBuilder::scan(QUADS_TABLE_DEFAULT_NAME, table_source, None)?
                .build()?;
        let df = DataFrame::new(state.clone(), plan);

        let count = df.count().await?;

        Ok(count)
    }
}
