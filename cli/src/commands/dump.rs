use anyhow::{Context, anyhow};
use rdf_fusion::common::{GraphName, IriParseError, NamedNode, QuadComponent, RdfFormat};
use rdf_fusion::store::{DumpOptions, DumpSortOrder, Store, TripleFallbackStrategy};
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
    triple_fallback: Option<String>,
) -> anyhow::Result<()> {
    let url = Url::parse(&output_url).context("Invalid output URL")?;
    let dump_format = try_identify_format(format, &url)?;

    let sort_by = if let Some(sort_by_str) = sort_by {
        Some(parse_sort_by(&sort_by_str)?)
    } else {
        None
    };

    let triple_fallback = match triple_fallback.as_deref() {
        Some("ignore") => TripleFallbackStrategy::IgnoreGraph,
        Some("error") | None => TripleFallbackStrategy::ErrorOnNonDefaultGraph,
        _ => anyhow::bail!("Invalid triple fallback strategy. Use 'ignore' or 'error'"),
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
        .with_triple_fallback_strategy(triple_fallback);

    store
        .dump(output_url, dump_format, options)
        .await
        .context("Failed to dump store")?;

    Ok(())
}

fn parse_sort_by(sort_by_str: &str) -> anyhow::Result<DumpSortOrder> {
    let upper = sort_by_str.trim().to_uppercase();

    if upper.starts_with("ZORDER(") && upper.ends_with(')') {
        let inner = &upper[7..upper.len() - 1];
        if inner.is_empty() {
            anyhow::bail!("ZORDER() requires at least one argument (e.g., ZORDER(S))");
        }

        let mut components = Vec::new();
        for c in inner.chars() {
            if c == ',' || c.is_whitespace() {
                continue;
            }
            let comp = QuadComponent::from_char(c)
                .ok_or_else(|| anyhow!("Unknown ZOrder column: '{c}'"))?;
            components.push(comp);
        }

        if components.is_empty() {
            anyhow::bail!("ZORDER() contains no valid columns");
        }

        Ok(DumpSortOrder::ZOrder(components))
    } else if upper.starts_with("NATIVE(") && upper.ends_with(')') {
        let inner = &upper[7..upper.len() - 1];
        if inner.is_empty() {
            anyhow::bail!("NATIVE() requires at least one argument (e.g., NATIVE(GSPO))");
        }

        let mut components = Vec::new();
        for c in inner.chars() {
            if c == ',' || c.is_whitespace() {
                continue;
            }
            let comp = QuadComponent::from_char(c)
                .ok_or_else(|| anyhow!("Unknown native sort column: '{c}'"))?;
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
                .ok_or_else(|| anyhow!("Unknown sort column: '{c}'"))?;
            components.push(comp);
        }

        if components.is_empty() {
            anyhow::bail!("Sort argument contains no valid columns");
        }

        Ok(DumpSortOrder::SparqlOrder(components))
    }
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
