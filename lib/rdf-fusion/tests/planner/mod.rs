use insta::Settings;
use rdf_fusion::common::{GraphNameRef, NamedNode, Quad, RdfFormat};
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion::store::Store;
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_execution::RdfFusionContextBuilder;
use rdf_fusion_execution::sparql::QueryExplanation;
use rdf_fusion_storage::delta::DeltaQuadsStorage;
use rdf_fusion_storage::index::IndexComponents;
use rdf_fusion_storage::rdf_files::RdfFileScanOptions;
use std::sync::Arc;
use tokio::fs::File;

mod bgp;
mod exists;

async fn assert_query_explanation(
    store: Store,
    query: &str,
    assert: impl FnOnce(QueryExplanation),
) {
    let (_, explanation) = store
        .explain_query_opt(query, QueryOptions::default())
        .await
        .unwrap();

    let mut settings = Settings::default();
    settings.add_filter(r"part-.*\.parquet", "<name>.parquet");
    settings.bind(move || assert(explanation));
}

/// Helper function to create an in-memory DeltaQuads store loaded with specific quads.
async fn setup_store(quads: Vec<Quad>) -> Store {
    let storage = DeltaQuadsStorage::new_in_memory(
        QuadStorageEncodingName::ObjectId,
        vec![IndexComponents::GPOS],
    )
    .await;

    let ctx = RdfFusionContextBuilder::new(Arc::new(storage))
        .with_register_in_memory_store(true)
        .with_single_partition_session_config()
        .build()
        .unwrap();

    let store = Store::new(ctx);

    for quad in quads {
        store.insert(quad.as_ref()).await.unwrap();
    }
    store.optimize().await.unwrap();

    store
}

/// Helper for the basic tests
async fn setup_basic_store() -> Store {
    setup_store(vec![Quad::new(
        NamedNode::new_unchecked("http://example.org/s1"),
        NamedNode::new_unchecked("http://example.org/p1"),
        NamedNode::new_unchecked("http://example.org/o1"),
        GraphNameRef::DefaultGraph,
    )])
    .await
}

/// Helper for the basic tests
async fn setup_store_with_graph_data(ttl_file_path: &str) -> Store {
    let store = setup_store(vec![]).await;
    store
        .load_from_reader(
            File::open(ttl_file_path).await.unwrap(),
            RdfFileScanOptions::with_format(RdfFormat::Turtle),
        )
        .await
        .unwrap();
    store.optimize().await.unwrap();
    store
}
