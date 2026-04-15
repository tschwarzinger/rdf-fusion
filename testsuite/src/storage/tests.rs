use anyhow::Result;
use datafusion::prelude::SessionContext;
use rdf_fusion::api::storage::QuadStorage;
use rdf_fusion::encoding::quads_to_plain_term_dataframe;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::model::{
    GraphName, GraphNameRef, Literal, NamedNode, NamedOrBlankNode, Quad, Term,
};
use std::slice;
use std::sync::Arc;

pub async fn insert_quad(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let inserted = storage
        .insert(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
        .await?;

    if let Some(inserted) = inserted {
        assert_eq!(inserted, 1);
    }

    assert_eq!(storage.len(&ctx.state()).await?, 1);
    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn insert_duplicate_quads_no_effect(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    storage
        .insert(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
        .await?;
    let inserted = storage
        .insert(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
        .await?;

    if let Some(inserted) = inserted {
        assert_eq!(inserted, 0);
    }

    assert_eq!(storage.len(&ctx.state()).await?, 1);
    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn insert_duplicate_quads_in_same_operation(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let inserted = storage
        .insert(quads_to_plain_term_dataframe(
            &ctx,
            &[example_quad(), example_quad()],
        ))
        .await?;

    if let Some(inserted) = inserted {
        assert_eq!(inserted, 1);
    }

    assert_eq!(storage.len(&ctx.state()).await?, 1);
    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn named_graph_insertion_and_query(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.com/graph"));

    let inserted = storage
        .insert_named_graph(&ctx.state(), graph.as_ref())
        .await?;

    if let Some(inserted) = inserted {
        assert!(inserted);
    }

    let exists = storage
        .contains_named_graph(&ctx.state(), graph.as_ref())
        .await?;
    assert!(exists);

    let graphs = storage.named_graphs(&ctx.state()).await?;
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0], graph);

    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn remove_quad(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let quad = example_quad_in_graph("http://example.com/g");

    storage
        .insert(quads_to_plain_term_dataframe(&ctx, slice::from_ref(&quad)))
        .await?;
    assert_eq!(storage.len(&ctx.state()).await?, 1);

    let removed = storage
        .remove(quads_to_plain_term_dataframe(&ctx, slice::from_ref(&quad)))
        .await?;
    if let Some(removed) = removed {
        assert!(removed);
    }

    assert_eq!(storage.len(&ctx.state()).await?, 0);

    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn clear_graph(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let g1 = "http://example.com/g1";
    let g2 = "http://example.com/g2";

    storage
        .insert(quads_to_plain_term_dataframe(
            &ctx,
            &[example_quad_in_graph(g1), example_quad_in_graph(g2)],
        ))
        .await?;
    assert_eq!(storage.len(&ctx.state()).await?, 2);

    storage
        .clear_graph(
            &ctx.state(),
            GraphNameRef::NamedNode(NamedNode::new_unchecked(g1).as_ref()),
        )
        .await?;

    assert_eq!(storage.len(&ctx.state()).await?, 1);

    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn insert_named_graph(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.com/graph"));
    storage
        .insert_named_graph(&ctx.state(), graph.as_ref())
        .await?;
    let exists = storage
        .contains_named_graph(&ctx.state(), graph.as_ref())
        .await?;
    assert!(exists);
    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn remove_named_graph(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.com/graph"));

    storage
        .insert_named_graph(&ctx.state(), graph.as_ref())
        .await?;
    let removed = storage
        .drop_named_graph(&ctx.state(), graph.as_ref())
        .await?;
    if let Some(removed) = removed {
        assert!(removed);
    }

    let exists = storage
        .contains_named_graph(&ctx.state(), graph.as_ref())
        .await?;
    assert!(!exists);
    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn clear_all(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    storage
        .insert(quads_to_plain_term_dataframe(
            &ctx,
            &[
                example_quad(),
                example_quad_in_graph("http://example.com/g1"),
                example_quad_in_graph("http://example.com/g2"),
            ],
        ))
        .await?;
    assert_eq!(storage.len(&ctx.state()).await?, 3);

    storage.clear(&ctx.state()).await?;
    assert_eq!(storage.len(&ctx.state()).await?, 0);

    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn optimize(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    storage
        .insert(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
        .await?;
    storage.optimize(&ctx.state()).await?;
    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn optimize_empty(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    storage.optimize(&ctx.state()).await?;
    storage.validate(&ctx.state()).await?;
    Ok(())
}

/// Tries to provoke a failure by calling [`QuadStorage::insert`] with an empty [`DataFrame`].
pub async fn empty_insert(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    storage
        .insert(quads_to_plain_term_dataframe(&ctx, &[]))
        .await?;
    storage.validate(&ctx.state()).await?;

    assert_eq!(storage.len(&ctx.state()).await?, 0);
    Ok(())
}

/// Tries to provoke a failure by calling [`QuadStorage::remove`] with an empty [`DataFrame`].
pub async fn empty_remove(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    storage
        .remove(quads_to_plain_term_dataframe(&ctx, &[]))
        .await?;
    storage.validate(&ctx.state()).await?;

    assert_eq!(storage.len(&ctx.state()).await?, 0);
    Ok(())
}

async fn create_session_context(quad_storage: &Arc<dyn QuadStorage>) -> SessionContext {
    let context = RdfFusionContextBuilder::new(Arc::clone(quad_storage))
        .with_single_partition_session_config()
        .build()
        .expect("Default in-memory works. Session config is set.");
    context.session_context().clone()
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
