use crate::error::SerializerError;
use crate::store::Store;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::{FileOutputMode, FileSinkConfig};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::SessionState;
use datafusion::logical_expr::col;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use object_store::path::Path;
use rdf_fusion_common::GraphNameRef;
use rdf_fusion_common::quads::{COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{GraphName, QuadComponent, RdfFormat};
use rdf_fusion_storage::rdf_files::RdfDataSink;
use std::sync::Arc;
use url::Url;

/// The sort order for dumping a store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DumpSortOrder {
    /// Standard lexicographical sort as defined by SPARQL.
    SparqlOrder(Vec<QuadComponent>),
    /// Use the native order (i.e., DataFusion's order) of the respective encoding.
    NativeOrder(Vec<QuadComponent>),
    /// Z-Order clustering.
    ZOrder(Vec<QuadComponent>),
}

impl DumpSortOrder {
    /// Validates that all components in the sort order are unique.
    pub fn validate(&self) -> anyhow::Result<()> {
        let components = match self {
            DumpSortOrder::SparqlOrder(c) => c,
            DumpSortOrder::NativeOrder(c) => c,
            DumpSortOrder::ZOrder(c) => c,
        };
        let mut seen = std::collections::HashSet::new();
        for component in components {
            if !seen.insert(component) {
                anyhow::bail!("Duplicate quad component in sort order: {component}");
            }
        }
        Ok(())
    }
}

/// Options for dumping a store.
#[derive(Debug, Default, Clone)]
pub struct DumpOptions {
    graph: Option<GraphName>,
    sort_by: Option<DumpSortOrder>,
    triple_fallback: TripleFallbackStrategy,
}

impl DumpOptions {
    /// The graph to dump. If `None`, the whole store is dumped.
    pub fn graph(&self) -> Option<&GraphName> {
        self.graph.as_ref()
    }

    /// The sort order.
    pub fn sort_by(&self) -> Option<&DumpSortOrder> {
        self.sort_by.as_ref()
    }

    /// The strategy to use when exporting quads to a triple-only format.
    pub fn triple_fallback(&self) -> TripleFallbackStrategy {
        self.triple_fallback
    }

    /// See [`Self::graph`]
    pub fn with_graph(mut self, graph: Option<GraphName>) -> Self {
        self.graph = graph;
        self
    }

    /// See [`Self::sort_by`]
    pub fn with_sort_by(mut self, sort_by: Option<DumpSortOrder>) -> Self {
        self.sort_by = sort_by;
        self
    }

    /// See [`Self::with_triple_fallback_strategy`]
    pub fn with_triple_fallback_strategy(
        mut self,
        triple_fallback: TripleFallbackStrategy,
    ) -> Self {
        self.triple_fallback = triple_fallback;
        self
    }
}

/// The strategy to use when exporting quads to a triple-only format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TripleFallbackStrategy {
    /// Errors if a non-default graph is encountered.
    #[default]
    ErrorOnNonDefaultGraph,
    /// Ignores the graph column and deduplicates the triples.
    IgnoreGraph,
}

pub(crate) async fn dump_store(
    store: &Store,
    output_url: String,
    format: RdfFormat,
    options: DumpOptions,
) -> Result<(), SerializerError> {
    if let Some(sort_by) = &options.sort_by {
        sort_by.validate().map_err(SerializerError::Other)?;
    }

    let url = Url::parse(&output_url).map_err(|e| anyhow::anyhow!(e))?;

    let mut df = if let Some(sort_order) = &options.sort_by {
        create_dump_query(
            store,
            options.graph.as_ref().map(|g| g.as_ref()),
            sort_order,
        )
        .await
        .map_err(SerializerError::Other)?
    } else {
        store
            .context()
            .quads_for_pattern(
                options.graph.as_ref().map(|g| g.as_ref()),
                None,
                None,
                None,
            )
            .await?
    };

    if !format.supports_datasets()
        && options.triple_fallback == TripleFallbackStrategy::IgnoreGraph
    {
        df = df
            .select(vec![col(COL_SUBJECT), col(COL_PREDICATE), col(COL_OBJECT)])?
            .distinct()?;
    }

    let (session, plan): (SessionState, _) = df.into_parts();
    let physical_plan: Arc<dyn ExecutionPlan> =
        session.create_physical_plan(&plan).await?;

    let physical_plan = if physical_plan.output_partitioning().partition_count() > 1 {
        Arc::new(CoalescePartitionsExec::new(physical_plan))
    } else {
        physical_plan
    };

    let runtime_env = session.runtime_env();
    let object_store_url = url.as_object_store_url();
    let object_store = runtime_env.object_store(&object_store_url)?;

    let sink: Arc<dyn DataSink> = match format {
        RdfFormat::Parquet => {
            let config = FileSinkConfig {
                original_url: output_url.clone(),
                object_store_url: object_store_url.clone(),
                file_group: Default::default(),
                table_paths: vec![
                    ListingTableUrl::parse(&output_url)
                        .map_err(|e| anyhow::anyhow!(e))?,
                ],
                output_schema: physical_plan.schema(),
                table_partition_cols: vec![],
                insert_op: InsertOp::Overwrite,
                keep_partition_by_columns: false,
                file_extension: "parquet".to_string(),
                file_output_mode: FileOutputMode::SingleFile,
            };
            let parquet_sink = ParquetSink::new(config, Default::default());
            Arc::new(RdfDataSink::new_parquet(
                parquet_sink,
                physical_plan.schema(),
            ))
        }
        _ => Arc::new(RdfDataSink::new_rdf(
            object_store,
            Path::from(url.path()),
            format,
            physical_plan.schema(),
        )),
    };

    let sink_exec = DataSinkExec::new(physical_plan, sink, None);
    collect(Arc::new(sink_exec), session.task_ctx()).await?;

    Ok(())
}

async fn create_dump_query(
    store: &Store,
    graph: Option<GraphNameRef<'_>>,
    sort_order: &DumpSortOrder,
) -> anyhow::Result<datafusion::prelude::DataFrame> {
    let ctx_view = store.context().create_view();
    let df = store
        .context()
        .quads_for_pattern(graph, None, None, None)
        .await?;

    apply_dump_sort_order(&ctx_view, df, sort_order)
}

fn apply_dump_sort_order(
    context: &rdf_fusion_extensions::RdfFusionContextView,
    df: datafusion::prelude::DataFrame,
    sort_order: &DumpSortOrder,
) -> anyhow::Result<datafusion::prelude::DataFrame> {
    use datafusion::logical_expr::SortExpr;
    use rdf_fusion_logical::{
        RdfFusionExprBuilderContext, RdfFusionLogicalPlanBuilderContext,
    };

    match sort_order {
        DumpSortOrder::SparqlOrder(components) => {
            let sort_exprs: Vec<_> = components
                .iter()
                .map(|c| SortExpr::new(col(c.column_name()), true, true))
                .collect();

            let builder_ctx = RdfFusionLogicalPlanBuilderContext::new(context.clone());
            let (session, plan) = df.into_parts();
            let builder = builder_ctx.create(Arc::new(plan));
            let plan = builder.order_by(sort_exprs)?.build()?;
            Ok(datafusion::prelude::DataFrame::new(session, plan))
        }
        DumpSortOrder::NativeOrder(components) => {
            let sort_exprs: Vec<_> = components
                .iter()
                .map(|c| SortExpr::new(col(c.column_name()), true, true))
                .collect();
            Ok(df.sort(sort_exprs)?)
        }
        DumpSortOrder::ZOrder(components) => {
            let zorder_args: Vec<_> =
                components.iter().map(|c| col(c.column_name())).collect();

            let (session, plan) = df.into_parts();
            let expr = RdfFusionExprBuilderContext::new(context, plan.schema())
                .zorder(zorder_args)?;
            let df = datafusion::prelude::DataFrame::new(session, plan);
            df.sort(vec![expr.sort(true, true)])
                .map_err(|e| anyhow::anyhow!("Failed to apply ZOrder sort: {e}"))
        }
    }
}
