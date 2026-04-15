use anyhow::{Context, Result, bail};
use oxttl::N3Parser;
use oxttl::n3::N3Quad;
use rdf_fusion::io::{RdfFormat, RdfParser};
use rdf_fusion::model::{Dataset, Graph};
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn read_file(url: &str) -> Result<impl AsyncRead + Unpin + Send + 'static> {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("suites").join(if url.starts_with("https://w3c.github.io/") {
            url.replace("https://w3c.github.io/", "")
        } else if url.starts_with(
            "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/testsuite/rdf-fusion-tests/",
        ) {
            url.replace(
                "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/testsuite/rdf-fusion-tests/",
                "rdf-fusion-tests/",
            )
        } else {
            bail!("Not supported url for file: {url}")
        });
    tokio::fs::File::open(&path)
        .await
        .with_context(|| format!("Failed to read {}", path.display()))
}

pub async fn read_file_to_string(url: &str) -> Result<String> {
    let mut buf = String::new();
    read_file(url).await?.read_to_string(&mut buf).await?;
    Ok(buf)
}

pub async fn load_to_graph(
    url: &str,
    graph: &mut Graph,
    format: RdfFormat,
    base_iri: Option<&str>,
    ignore_errors: bool,
) -> Result<()> {
    let parser = RdfParser::from_format(format).with_base_iri(base_iri.unwrap_or(url))?;
    let mut stream = parser.for_tokio_async_reader(read_file(url).await?);
    while let Some(t) = stream.next().await {
        match t {
            Ok(t) => {
                graph.insert(&t.into());
            }
            Err(e) => {
                if !ignore_errors {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(())
}

pub async fn load_graph(
    url: &str,
    format: RdfFormat,
    ignore_errors: bool,
) -> Result<Graph> {
    let mut graph = Graph::new();
    load_to_graph(url, &mut graph, format, None, ignore_errors).await?;
    Ok(graph)
}

pub async fn load_to_dataset(
    url: &str,
    dataset: &mut Dataset,
    format: RdfFormat,
    ignore_errors: bool,
    unchecked: bool,
) -> Result<()> {
    let mut parser = RdfParser::from_format(format).with_base_iri(url)?;
    if unchecked {
        parser = parser.lenient();
    }
    let mut stream = parser.for_tokio_async_reader(read_file(url).await?);
    while let Some(q) = stream.next().await {
        match q {
            Ok(q) => {
                dataset.insert(&q);
            }
            Err(e) => {
                if !ignore_errors {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(())
}

pub async fn load_dataset(
    url: &str,
    format: RdfFormat,
    ignore_errors: bool,
    unchecked: bool,
) -> Result<Dataset> {
    let mut dataset = Dataset::new();
    load_to_dataset(url, &mut dataset, format, ignore_errors, unchecked).await?;
    Ok(dataset)
}

pub fn guess_rdf_format(url: &str) -> Result<RdfFormat> {
    url.rsplit_once('.')
        .and_then(|(_, extension)| RdfFormat::from_extension(extension))
        .with_context(|| format!("Serialization type not found for {url}"))
}

pub async fn load_n3(url: &str, ignore_errors: bool) -> Result<Vec<N3Quad>> {
    let mut quads = Vec::new();
    let mut stream = N3Parser::new()
        .with_base_iri(url)?
        .with_prefix("", format!("{url}#"))?
        .for_tokio_async_reader(read_file(url).await?);
    while let Some(q) = stream.next().await {
        match q {
            Ok(q) => quads.push(q),
            Err(e) => {
                if !ignore_errors {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(quads)
}
