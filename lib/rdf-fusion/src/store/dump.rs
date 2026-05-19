use crate::error::SerializerError;
use crate::store::Store;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::{FileOutputMode, FileSinkConfig};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{SortExpr, col};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use object_store::path::Path;
use rdf_fusion_common::quads::{COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{GraphName, QuadComponent, RdfFormat};
use rdf_fusion_logical::RdfFusionLogicalPlanBuilder;
use rdf_fusion_storage::rdf_files::RdfDataSink;
use std::sync::Arc;
use url::Url;

/// The sort order for dumping a store.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    /// Returns the components of the sort order.
    pub fn components(&self) -> &[QuadComponent] {
        match self {
            DumpSortOrder::SparqlOrder(c) => c,
            DumpSortOrder::NativeOrder(c) => c,
            DumpSortOrder::ZOrder(c) => c,
        }
    }
}

impl std::str::FromStr for DumpSortOrder {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper = s.trim().to_uppercase();

        if upper.starts_with("ZORDER(") && upper.ends_with(')') {
            let inner = &upper[7..upper.len() - 1];
            if inner.is_empty() {
                anyhow::bail!(
                    "ZORDER() requires at least one argument (e.g., ZORDER(S))"
                );
            }

            let mut components = Vec::new();
            for c in inner.chars() {
                if c == ',' || c.is_whitespace() {
                    continue;
                }
                let comp = QuadComponent::from_char(c)
                    .ok_or_else(|| anyhow::anyhow!("Unknown ZOrder column: '{c}'"))?;
                components.push(comp);
            }

            if components.is_empty() {
                anyhow::bail!("ZORDER() contains no valid columns");
            }

            Ok(DumpSortOrder::ZOrder(components))
        } else if upper.starts_with("NATIVE(") && upper.ends_with(')') {
            let inner = &upper[7..upper.len() - 1];
            if inner.is_empty() {
                anyhow::bail!(
                    "NATIVE() requires at least one argument (e.g., NATIVE(GSPO))"
                );
            }

            let mut components = Vec::new();
            for c in inner.chars() {
                if c == ',' || c.is_whitespace() {
                    continue;
                }
                let comp = QuadComponent::from_char(c).ok_or_else(|| {
                    anyhow::anyhow!("Unknown native sort column: '{c}'")
                })?;
                components.push(comp);
            }

            if components.is_empty() {
                anyhow::bail!("NATIVE() contains no valid columns");
            }

            Ok(DumpSortOrder::NativeOrder(components))
        } else {
            let mut components = Vec::new();
            for c in upper.chars() {
                if c.is_whitespace() {
                    continue;
                }
                let comp = QuadComponent::from_char(c)
                    .ok_or_else(|| anyhow::anyhow!("Unknown sort column: '{c}'"))?;
                components.push(comp);
            }

            if components.is_empty() {
                anyhow::bail!("Sort argument contains no valid columns");
            }

            Ok(DumpSortOrder::SparqlOrder(components))
        }
    }
}

/// The encoding to use for dumping.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum DumpEncoding {
    /// Use the plain term encoding.
    #[default]
    PlainTerm,
    /// Use the string encoding.
    String,
}

/// Options for dumping a store.
#[derive(Debug, Default, Clone)]
pub struct DumpOptions {
    graph: Option<GraphName>,
    sort_by: Option<DumpSortOrder>,
    triple_fallback: TripleFallbackStrategy,
    encoding: DumpEncoding,
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

    /// The encoding to use.
    pub fn encoding(&self) -> DumpEncoding {
        self.encoding
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

    /// See [`Self::encoding`]
    pub fn with_encoding(mut self, encoding: DumpEncoding) -> Self {
        self.encoding = encoding;
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

    let mut builder = store.context().quads_for_pattern_as_builder(
        options.graph.as_ref().map(|g| g.as_ref()),
        None,
        None,
        None,
    );

    if let Some(sort_order) = &options.sort_by {
        builder =
            apply_dump_sort_order(builder, sort_order).map_err(SerializerError::Other)?
    }

    let builder = if format != RdfFormat::Parquet {
        match options.encoding {
            DumpEncoding::PlainTerm => builder
                .with_plain_terms()
                .map_err(|e| SerializerError::Other(e.into()))?,
            DumpEncoding::String => builder
                .with_encoding(rdf_fusion_encoding::EncodingName::String)
                .map_err(|e| SerializerError::Other(e.into()))?,
        }
    } else {
        builder
    };

    let mut df = datafusion::prelude::DataFrame::new(
        store.context().session_context().state(),
        builder
            .build()
            .map_err(|e| SerializerError::Other(e.into()))?,
    );

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

fn apply_dump_sort_order(
    builder: RdfFusionLogicalPlanBuilder,
    sort_order: &DumpSortOrder,
) -> anyhow::Result<RdfFusionLogicalPlanBuilder> {
    match sort_order {
        DumpSortOrder::SparqlOrder(components) => {
            let sort_exprs: Vec<_> = components
                .iter()
                .map(|c| SortExpr::new(col(c.column_name()), true, true))
                .collect();
            Ok(builder.order_by(sort_exprs)?)
        }
        DumpSortOrder::NativeOrder(components) => {
            let sort_exprs: Vec<_> = components
                .iter()
                .map(|c| SortExpr::new(col(c.column_name()), true, true))
                .collect();
            let context = builder.context().clone();
            let builder = builder.into_inner().sort(sort_exprs)?;
            Ok(context.create(Arc::new(builder.build()?)))
        }
        DumpSortOrder::ZOrder(components) => {
            let zorder_args: Vec<_> =
                components.iter().map(|c| col(c.column_name())).collect();

            let expr = builder.expr_builder_root().zorder(zorder_args)?;
            Ok(builder.order_by(vec![expr.sort(true, true)])?)
        }
    }
}
