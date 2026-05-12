use anyhow::{Context, anyhow, bail};
use datafusion::logical_expr::{SortExpr, col};
use datafusion::prelude::{DataFrame, Expr};
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use futures::StreamExt;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use rdf_fusion::execution::results::{QuadStream, QuerySolutionStream};
use rdf_fusion::io::{RdfFormat, RdfSerializer};
use rdf_fusion::logical::{
    RdfFusionExprBuilderContext, RdfFusionLogicalPlanBuilderContext,
};
use rdf_fusion::model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion::model::{NamedNode, NamedNodeRef, VariableRef};
use rdf_fusion::store::Store;
use rdf_fusion_extensions::RdfFusionContextView;
use std::sync::Arc;
use url::Url;

/// Dumps the store to the given output url.
pub async fn dump(
    store: Store,
    output_url: String,
    format: Option<String>,
    graph: Option<String>,
    sort_by: Option<String>,
) -> anyhow::Result<()> {
    let url = Url::parse(&output_url).context("Invalid output URL")?;
    let runtime_env = store.context().session_context().runtime_env();
    let object_store_url = url.as_object_store_url();

    let object_store = runtime_env
        .object_store(object_store_url)
        .map_err(|e| anyhow!("Failed to get object store for output: {e}"))?;

    let rdf_format = try_identify_format(format, &url)?;
    let path = Path::from(url.path());

    // Initialize the writer
    let writer = BufWriter::new(object_store, path);

    let mut writer = if let Some(sort_by_str) = &sort_by {
        let results = create_query(&store, &graph, sort_by_str).await?;
        dump_custom_query(&graph, rdf_format, writer, results).await?
    } else if let Some(graph_name) = &graph {
        let named_node = NamedNode::new(graph_name).context("Invalid graph name IRI")?;
        store
            .dump_graph_to_writer(named_node.as_ref(), rdf_format, writer)
            .await
            .context("Failed to dump graph")?
    } else {
        store
            .dump_to_writer(rdf_format, writer)
            .await
            .context("Failed to dump store")?
    };

    // Ensure all buffered data is flushed and the stream is closed
    use tokio::io::AsyncWriteExt;
    writer
        .shutdown()
        .await
        .context("Failed to shutdown writer")?;

    Ok(())
}

/// Tries to identify the RDF format from the given format string or URL.
fn try_identify_format(format: Option<String>, url: &Url) -> anyhow::Result<RdfFormat> {
    if let Some(format_str) = format {
        RdfFormat::from_extension(&format_str)
            .or_else(|| RdfFormat::from_extension(&format_str.to_lowercase()))
            .context(format!("Unknown RDF format: {format_str}"))
    } else {
        let path = std::path::Path::new(url.path());
        let extension = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default();

        RdfFormat::from_extension(extension).ok_or_else(|| {
            anyhow!("Failed to identify RDF format from extension: '{extension}'. Please specify a format explicitly.")
        })
    }
}

/// Creates a custom SPARQL query to sort the store.
async fn create_query(
    store: &Store,
    graph: &Option<String>,
    sort_by_arg: &str,
) -> anyhow::Result<DataFrame> {
    let ctx_view = store.context().create_view();

    let builder_ctx = RdfFusionLogicalPlanBuilderContext::new(ctx_view.clone());

    let df = match graph.as_deref() {
        None => {
            store
                .context()
                .quads_for_pattern(None, None, None, None)
                .await?
        }
        Some(graph) => {
            let graph = NamedNodeRef::new(graph).context("Invalid graph name")?;
            let (session, plan) = store
                .context()
                .quads_for_pattern(None, None, None, None)
                .await?
                .into_parts();
            let builder = builder_ctx.create(Arc::new(plan));
            let filter_expr = builder
                .expr_builder_root()
                .variable(VariableRef::new_unchecked(COL_GRAPH))?
                .build_same_term_scalar(graph.into())?;
            let plan = builder.filter(filter_expr)?.build()?;
            DataFrame::new(session, plan)
        }
    };

    apply_sort_order(&ctx_view, df, sort_by_arg)
}

/// Applies the sort order to the given dataframe.
fn apply_sort_order(
    context: &RdfFusionContextView,
    df: DataFrame,
    sort_by_str: &str,
) -> anyhow::Result<DataFrame> {
    let mut clauses = Vec::new();
    let upper = sort_by_str.trim().to_uppercase();

    if upper.starts_with("ZORDER(") && upper.ends_with(')') {
        // Parse ZORDER(args)
        let inner = &upper[7..upper.len() - 1]; // Strip "ZORDER(" and ")"
        if inner.is_empty() {
            bail!("ZORDER() requires at least one argument (e.g., ZORDER(S))");
        }

        let mut zorder_args = Vec::new();
        for c in inner.chars() {
            if c == ',' || c.is_whitespace() {
                continue;
            }
            let var = map_sort_char(c)
                .ok_or_else(|| anyhow!("Unknown ZOrder column: '{c}'"))?;
            zorder_args.push(var);
        }

        if zorder_args.is_empty() {
            bail!("ZORDER() contains no valid columns");
        }

        let (session, plan) = df.into_parts();
        let expr = RdfFusionExprBuilderContext::new(context, plan.schema())
            .zorder(zorder_args)?;
        let df = DataFrame::new(session, plan);
        df.sort(vec![expr.sort(true, true)])
            .map_err(|e| anyhow!("Failed to apply ZOrder sort: {e}"))
    } else {
        // Standard GSPO string
        for c in upper.chars() {
            if c.is_whitespace() {
                continue;
            }
            let var =
                map_sort_char(c).ok_or_else(|| anyhow!("Unknown sort column: '{c}'"))?;
            clauses.push(SortExpr::new(var, true, true));
        }

        if clauses.is_empty() {
            bail!("Sort argument contains no valid columns");
        }

        let builder_ctx = RdfFusionLogicalPlanBuilderContext::new(context.clone());
        let (session, plan) = df.into_parts();
        let builder = builder_ctx.create(Arc::new(plan));
        let plan = builder.order_by(clauses)?.build()?;
        Ok(DataFrame::new(session, plan))
    }
}

/// Maps a single character to a SPARQL variable.
fn map_sort_char(c: char) -> Option<Expr> {
    match c.to_ascii_uppercase() {
        'G' => Some(col(COL_GRAPH)),
        'S' => Some(col(COL_SUBJECT)),
        'P' => Some(col(COL_PREDICATE)),
        'O' => Some(col(COL_OBJECT)),
        _ => None,
    }
}

/// Dumps the results of a custom query to the given writer.
async fn dump_custom_query(
    graph: &Option<String>,
    rdf_format: RdfFormat,
    writer: BufWriter,
    results: DataFrame,
) -> anyhow::Result<BufWriter> {
    let solutions = QuerySolutionStream::try_from_dataframe(results)
        .await
        .map_err(|e| anyhow!("Failed to create query solution stream: {e}"))?;

    let quad_stream = QuadStream::try_new(solutions)
        .map_err(|e| anyhow!("Failed to create quad stream: {e}"))?;

    let mut serializer =
        RdfSerializer::from_format(rdf_format).for_tokio_async_writer(writer);
    let mut stream = quad_stream;

    let has_graph = graph.is_some();

    while let Some(item) = stream.next().await {
        let quad = item.context("Error reading next quad from stream")?;

        if has_graph {
            serializer
                .serialize_triple(quad.as_ref())
                .await
                .context("Failed to serialize triple")?;
        } else {
            serializer
                .serialize_quad(&quad)
                .await
                .context("Failed to serialize quad")?;
        }
    }

    serializer
        .finish()
        .await
        .context("Failed to finish serializer")
}
