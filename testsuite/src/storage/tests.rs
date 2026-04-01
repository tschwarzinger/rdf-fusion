use anyhow::Result;
use rdf_fusion::api::storage::QuadStorage;
use rdf_fusion::model::{
    GraphName, GraphNameRef, Literal, NamedNode, NamedOrBlankNode, Quad, Term,
};
use std::sync::Arc;

pub async fn insert_quad(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let inserted = storage.extend(vec![example_quad()]).await?;
    assert_eq!(inserted, 1);
    assert_eq!(storage.len().await?, 1);
    storage.validate().await?;
    Ok(())
}

pub async fn insert_duplicate_quads_no_effect(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    storage.extend(vec![example_quad()]).await?;
    let inserted = storage.extend(vec![example_quad()]).await?;
    assert_eq!(inserted, 0);
    assert_eq!(storage.len().await?, 1);
    storage.validate().await?;
    Ok(())
}

pub async fn insert_duplicate_quads_in_same_operation(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    let inserted = storage.extend(vec![example_quad(), example_quad()]).await?;
    assert_eq!(inserted, 1);
    assert_eq!(storage.len().await?, 1);
    storage.validate().await?;
    Ok(())
}

pub async fn named_graph_insertion_and_query(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.com/graph"));

    let inserted = storage.insert_named_graph(graph.as_ref()).await?;
    assert!(inserted);

    let exists = storage.contains_named_graph(graph.as_ref()).await?;
    assert!(exists);

    let graphs = storage.named_graphs().await?;
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0], graph);

    storage.validate().await?;
    Ok(())
}

pub async fn remove_quad(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let quad = example_quad_in_graph("http://example.com/g");

    storage.extend(vec![quad.clone()]).await?;
    assert_eq!(storage.len().await?, 1);

    let removed = storage.remove(quad.as_ref()).await?;
    assert!(removed);
    assert_eq!(storage.len().await?, 0);

    storage.validate().await?;
    Ok(())
}

pub async fn clear_graph(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let g1 = "http://example.com/g1";
    let g2 = "http://example.com/g2";

    storage
        .extend(vec![example_quad_in_graph(g1), example_quad_in_graph(g2)])
        .await?;
    assert_eq!(storage.len().await?, 2);

    storage
        .clear_graph(GraphNameRef::NamedNode(
            NamedNode::new_unchecked(g1).as_ref(),
        ))
        .await?;

    assert_eq!(storage.len().await?, 1);

    storage.validate().await?;
    Ok(())
}

pub async fn insert_named_graph(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.com/graph"));
    storage.insert_named_graph(graph.as_ref()).await?;
    let exists = storage.contains_named_graph(graph.as_ref()).await?;
    assert!(exists);
    storage.validate().await?;
    Ok(())
}

pub async fn remove_named_graph(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.com/graph"));

    storage.insert_named_graph(graph.as_ref()).await?;
    let removed = storage.drop_named_graph(graph.as_ref()).await?;
    assert!(removed);

    let exists = storage.contains_named_graph(graph.as_ref()).await?;
    assert!(!exists);
    storage.validate().await?;
    Ok(())
}

pub async fn clear_all(storage: Arc<dyn QuadStorage>) -> Result<()> {
    storage
        .extend(vec![
            example_quad(),
            example_quad_in_graph("http://example.com/g1"),
            example_quad_in_graph("http://example.com/g2"),
        ])
        .await?;
    assert_eq!(storage.len().await?, 3);

    storage.clear().await?;
    assert_eq!(storage.len().await?, 0);

    storage.validate().await?;
    Ok(())
}

pub async fn optimize(storage: Arc<dyn QuadStorage>) -> Result<()> {
    storage.extend(vec![example_quad()]).await?;
    storage.optimize().await?;
    storage.validate().await?;
    Ok(())
}

fn example_quad() -> Quad {
    Quad::new(
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(
            "http://example.com/subject",
        )),
        NamedNode::new_unchecked("http://example.com/predicate"),
        Term::Literal(Literal::new_simple_literal("value")),
        GraphName::DefaultGraph,
    )
}

fn example_quad_in_graph(graph: &str) -> Quad {
    Quad::new(
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(
            "http://example.com/subject",
        )),
        NamedNode::new_unchecked("http://example.com/predicate"),
        Term::Literal(Literal::new_simple_literal("value")),
        GraphName::NamedNode(NamedNode::new_unchecked(graph)),
    )
}
