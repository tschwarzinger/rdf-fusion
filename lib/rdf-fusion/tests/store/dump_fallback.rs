use crate::store::read_dump;
use rdf_fusion::common::*;
use rdf_fusion::store::{RdfDumpOptions, Store, TripleFallbackStrategy};

#[tokio::test]
async fn test_dump_turtle_error_on_named_graph() {
    let store = Store::new_in_memory().await;
    let ex = NamedNode::new("http://example.com/s").unwrap();
    let g = NamedNode::new("http://example.com/g").unwrap();
    store.insert(QuadRef::new(&ex, &ex, &ex, &g)).await.unwrap();

    let options = RdfDumpOptions::default()
        .with_triple_fallback_strategy(TripleFallbackStrategy::ErrorOnNonDefaultGraph);

    let result = store
        .dump("memory:///test".to_owned(), RdfFormat::Turtle, options)
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Encountered non-default graph"));
}

#[tokio::test]
async fn test_dump_request_default_graph() {
    let store = Store::new_in_memory().await;
    let ex = NamedNode::new("http://example.com/s").unwrap();
    let g = NamedNode::new("http://example.com/g").unwrap();

    store
        .insert(QuadRef::new(&ex, &ex, &ex, GraphNameRef::DefaultGraph))
        .await
        .unwrap();
    store.insert(QuadRef::new(&ex, &ex, &ex, &g)).await.unwrap();

    let options = RdfDumpOptions::default()
        .with_graph(Some(GraphName::DefaultGraph))
        .with_triple_fallback_strategy(TripleFallbackStrategy::ErrorOnNonDefaultGraph);

    store
        .dump("memory:///test".to_owned(), RdfFormat::Turtle, options)
        .await
        .unwrap();

    let content = read_dump(store.context().session_context(), "memory:///test").await;
    insta::assert_snapshot!(
        content,
        @"<http://example.com/s> <http://example.com/s> <http://example.com/s> ."
    );
}

#[tokio::test]
async fn test_dump_turtle_ignore_named_graph() {
    let store = Store::new_in_memory().await;
    let ex = NamedNode::new("http://example.com/s").unwrap();
    let g1 = NamedNode::new("http://example.com/g1").unwrap();
    let g2 = NamedNode::new("http://example.com/g2").unwrap();

    // Insert same triple in two different graphs
    store
        .insert(QuadRef::new(&ex, &ex, &ex, &g1))
        .await
        .unwrap();
    store
        .insert(QuadRef::new(&ex, &ex, &ex, &g2))
        .await
        .unwrap();

    let options = RdfDumpOptions::default()
        .with_triple_fallback_strategy(TripleFallbackStrategy::IgnoreGraph);

    store
        .dump("memory:///test".to_owned(), RdfFormat::Turtle, options)
        .await
        .unwrap();

    let content = read_dump(store.context().session_context(), "memory:///test").await;
    insta::assert_snapshot!(
        content,
        @"<http://example.com/s> <http://example.com/s> <http://example.com/s> ."
    );
}

#[tokio::test]
async fn test_dump_turtle_default_graph_success() {
    let store = Store::new_in_memory().await;
    let ex = NamedNode::new("http://example.com/s").unwrap();

    // Insert into default graph
    store
        .insert(QuadRef::new(&ex, &ex, &ex, GraphNameRef::DefaultGraph))
        .await
        .unwrap();

    let options = RdfDumpOptions::default()
        .with_triple_fallback_strategy(TripleFallbackStrategy::ErrorOnNonDefaultGraph);

    store
        .dump("memory:///test".to_owned(), RdfFormat::Turtle, options)
        .await
        .unwrap();

    let content = read_dump(store.context().session_context(), "memory:///test").await;
    insta::assert_snapshot!(
        content,
        @"<http://example.com/s> <http://example.com/s> <http://example.com/s> ."
    );
}
