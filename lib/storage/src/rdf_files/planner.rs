use crate::rdf_files::logical_node::ParseRdfFileNode;
use crate::rdf_files::{RdfParserExec, open_line_range_reader};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use object_store::ObjectStoreExt;
use object_store::path::Path;
use oxrdfio::RdfFormat;
use rdf_fusion_common::DFResult;
use rdf_fusion_common::config::RdfFusionSessionConfigExt;
use std::sync::Arc;
use tokio::io::AsyncRead;

pub struct RdfFilePlanner;

#[async_trait]
impl ExtensionPlanner for RdfFilePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<ParseRdfFileNode>() else {
            return Ok(None);
        };

        let session_state = session
            .as_any()
            .downcast_ref::<SessionState>()
            .expect("session must be a SessionState");
        let parsers = open_readers(node, session_state)
            .await?
            .into_iter()
            .map(|reader| node.options.create_parser(reader))
            .collect();
        let blank_node_renamer = node.options.blank_node_renamer.clone();

        Ok(Some(Arc::new(RdfParserExec::new(
            parsers,
            Arc::clone(node.schema.inner()),
            blank_node_renamer,
        ))))
    }
}

/// Opens the readers for parsing the source of the `node`.
///
/// Files in a line-based format (N-Triples and N-Quads) are split into up to `target_partitions`
/// chunks of at least `parallel_parsing_min_chunk_size` bytes that are parsed in parallel. All
/// other sources are read by a single reader.
async fn open_readers(
    node: &ParseRdfFileNode,
    session_state: &SessionState,
) -> DFResult<Vec<Box<dyn AsyncRead + Unpin + Send + 'static>>> {
    let line_based =
        matches!(node.options.format, RdfFormat::NTriples | RdfFormat::NQuads);
    let target_partitions = session_state.config().target_partitions() as u64;
    if let Some(url) = node.source.url()
        && line_based
        && target_partitions > 1
    {
        let object_store = session_state
            .runtime_env()
            .object_store_registry
            .get_store(url)?;
        let path = Path::from(url.path());
        let size = object_store.head(&path).await?.size;

        let min_chunk_size = session_state
            .config()
            .rdf_fusion_options_or_default()
            .storage
            .rdf_files
            .parallel_parsing_min_chunk_size
            .max(1) as u64;
        let num_chunks = target_partitions.min(size / min_chunk_size);
        if num_chunks > 1 {
            let chunk_size = size.div_ceil(num_chunks);
            let readers = (0..num_chunks).map(|i| {
                let range = (i * chunk_size)..((i + 1) * chunk_size).min(size);
                open_line_range_reader(Arc::clone(&object_store), &path, range)
            });
            return futures::future::try_join_all(readers).await;
        }
    }

    Ok(vec![node.source.stream(session_state).await?])
}
