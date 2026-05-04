use rdf_fusion::store::Store;

pub async fn print_store_stats(store: &Store) -> anyhow::Result<()> {
    println!("Store stats:");
    println!("- Triples: {}", store.len().await?);
    // TODO #206: Enable after named graphs index exists
    // println!(
    //     "- Number of Named Graphs: {}",
    //     store.named_graphs().await?.len()
    // );

    Ok(())
}
