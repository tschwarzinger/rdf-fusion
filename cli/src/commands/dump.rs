use anyhow::Context;
use rdf_fusion::common::{GraphName, IriParseError, NamedNode, RdfFormat};
use rdf_fusion::store::{
    DumpEncoding, DumpOptions, DumpSortOrder, Store, TripleFallbackStrategy,
};
use url::Url;

/// Dumps the store content to the specified output URL in the desired format.
///
/// This command supports dumping to RDF formats (Turtle, N-Quads, etc.) and Parquet.
/// It can also handle sorting and filtering by graph.
pub async fn dump(
    store: Store,
    output_url: String,
    format: Option<String>,
    graph: Option<String>,
    sort_by: Option<String>,
    triple_fallback: String,
    encoding: String,
) -> anyhow::Result<()> {
    let url = Url::parse(&output_url).context("Invalid output URL")?;
    let dump_format = try_identify_format(format, &url)?;

    let sort_by = sort_by
        .map(|sort_by| sort_by.parse::<DumpSortOrder>())
        .transpose()?;

    let triple_fallback = match triple_fallback.as_str() {
        "ignore" => TripleFallbackStrategy::IgnoreGraph,
        "error" => TripleFallbackStrategy::ErrorOnNonDefaultGraph,
        _ => anyhow::bail!("Invalid triple fallback strategy. Use 'ignore' or 'error'"),
    };

    let encoding = match encoding.as_str() {
        "plain-term" => DumpEncoding::PlainTerm,
        "string" => DumpEncoding::String,
        _ => anyhow::bail!("Invalid encoding. Use 'plain-term' or 'string'"),
    };

    let graph = graph
        .map(|gn| {
            Ok::<GraphName, IriParseError>(match gn.as_ref() {
                "default" => GraphName::DefaultGraph,
                name => GraphName::NamedNode(NamedNode::new(name)?),
            })
        })
        .transpose()
        .context("Invalid graph name")?;

    let options = DumpOptions::default()
        .with_graph(graph)
        .with_sort_by(sort_by)
        .with_triple_fallback_strategy(triple_fallback)
        .with_encoding(encoding);

    store
        .dump(output_url, dump_format, options)
        .await
        .context("Failed to dump store")?;

    Ok(())
}

/// Tries to identify the format from the given format string or URL.
fn try_identify_format(format: Option<String>, url: &Url) -> anyhow::Result<RdfFormat> {
    let format_str = format
        .as_deref()
        .or_else(|| {
            let path = std::path::Path::new(url.path());
            path.extension().and_then(|e| e.to_str())
        })
        .unwrap_or_default();

    RdfFormat::from_extension(format_str)
        .or_else(|| RdfFormat::from_extension(&format_str.to_lowercase()))
        .context(format!("Unknown format: {format_str}"))
}
