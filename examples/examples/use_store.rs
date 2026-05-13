use futures::StreamExt;
use rdf_fusion::common::{GraphName, NamedNode, Quad};
use rdf_fusion::store::Store;

/// This example shows how to use RDF Fusion as an RDF Store. While this example only shows the
/// basic usage, the documentation of [Store] contains additional methods with examples.
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let store = Store::new_in_memory().await;

    // Insert quad
    let quad = Quad::new(
        NamedNode::new("http://example.com/use_store")?,
        NamedNode::new("http://example.com/complexity")?,
        NamedNode::new("http://example.com/simple")?,
        GraphName::DefaultGraph,
    );
    store.insert(&quad).await?;

    // List all quads. RDF Fusion uses an asynchronous API.
    let mut stream = store.stream().await?;
    println!("Quads:");
    while let Some(quad) = stream.next().await {
        println!("{}", quad?);
    }

    Ok(())
}
