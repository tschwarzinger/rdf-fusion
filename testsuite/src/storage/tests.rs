use anyhow::Result;
use datafusion::physical_plan::{collect, execute_stream};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use rdf_fusion::common::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use rdf_fusion::encoding::EncodingScalar;
use rdf_fusion::encoding::quads_to_plain_term_dataframe;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::extensions::storage::QuadStorage;
use std::slice;
use std::sync::Arc;

pub async fn insert_quad(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let transaction = storage.begin_transaction(&ctx.state()).await?;
    let inserted = transaction
        .insert(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
        .await?;
    transaction.commit().await?;

    if let Some(inserted) = inserted {
        assert_eq!(inserted, 1);
    }

    assert_eq!(storage.snapshot().await?.len(&ctx.state()).await?, 1);
    storage.validate(&ctx.state()).await?;
    Ok(())
}

/// Inserts a query and then obtains a snapshot of the same transaction. This snaphsot should return
/// the inserted quad.
pub async fn insert_quad_and_query_within_transaction(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let transaction = storage.begin_transaction(&ctx.state()).await?;
    transaction
        .insert(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
        .await?;
    let snapshot = transaction.snapshot().await?;
    let result = snapshot.len(&ctx.state()).await?;
    assert_eq!(result, 1);
    Ok(())
}

/// Inserts a query and then obtains a snapshot of the same transaction. This snaphsot should return
/// the inserted quad.
pub async fn insert_and_remove_quad_and_query_within_transaction(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let transaction = storage.begin_transaction(&ctx.state()).await?;
    transaction
        .insert(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
        .await?;
    transaction
        .remove(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
        .await?;
    let snapshot = transaction.snapshot().await?;
    let result = snapshot.len(&ctx.state()).await?;
    assert_eq!(result, 0);
    Ok(())
}

pub async fn insert_duplicate_quads_no_effect(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    {
        let transaction = storage.begin_transaction(&ctx.state()).await?;
        transaction
            .insert(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
            .await?;
        transaction.commit().await?;
    }

    let transaction = storage.begin_transaction(&ctx.state()).await?;
    let inserted = transaction
        .insert(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
        .await?;
    transaction.commit().await?;

    if let Some(inserted) = inserted {
        assert_eq!(inserted, 0);
    }

    assert_eq!(storage.snapshot().await?.len(&ctx.state()).await?, 1);
    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn insert_duplicate_quads_in_same_operation(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let transaction = storage.begin_transaction(&ctx.state()).await?;
    let inserted = transaction
        .insert(quads_to_plain_term_dataframe(
            &ctx,
            &[example_quad(), example_quad()],
        ))
        .await?;
    transaction.commit().await?;

    if let Some(inserted) = inserted {
        assert_eq!(inserted, 1);
    }

    assert_eq!(storage.snapshot().await?.len(&ctx.state()).await?, 1);
    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn named_graph_insertion_and_query(
    storage: Arc<dyn QuadStorage>,
) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.com/graph"));

    let transaction = storage.begin_transaction(&ctx.state()).await?;
    let df = graph_to_dataframe(&ctx, storage.as_ref(), graph.as_ref()).await;
    let inserted = transaction.create_named_graph(df).await?;
    transaction.commit().await?;

    if let Some(inserted) = inserted {
        assert!(inserted);
    }

    assert_named_graph_count(storage, &ctx, 1).await?;
    Ok(())
}

pub async fn remove_quad(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let quad = example_quad_in_graph("http://example.com/g");

    {
        let transaction = storage.begin_transaction(&ctx.state()).await?;
        transaction
            .insert(quads_to_plain_term_dataframe(&ctx, slice::from_ref(&quad)))
            .await?;
        transaction.commit().await?;
    }
    assert_eq!(storage.snapshot().await?.len(&ctx.state()).await?, 1);

    let transaction = storage.begin_transaction(&ctx.state()).await?;
    let removed = transaction
        .remove(quads_to_plain_term_dataframe(&ctx, slice::from_ref(&quad)))
        .await?;
    transaction.commit().await?;

    if let Some(removed) = removed {
        assert!(removed);
    }

    assert_eq!(storage.snapshot().await?.len(&ctx.state()).await?, 0);

    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn clear_graph(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let g1 = "http://example.com/g1";
    let g2 = "http://example.com/g2";

    {
        let transaction = storage.begin_transaction(&ctx.state()).await?;
        transaction
            .insert(quads_to_plain_term_dataframe(
                &ctx,
                &[example_quad_in_graph(g1), example_quad_in_graph(g2)],
            ))
            .await?;
        transaction.commit().await?;
    }
    assert_eq!(storage.snapshot().await?.len(&ctx.state()).await?, 2);

    let transaction = storage.begin_transaction(&ctx.state()).await?;
    let df = graph_to_dataframe(
        &ctx,
        storage.as_ref(),
        NamedNode::new_unchecked(g1.to_string()).as_ref(),
    )
    .await;
    transaction.clear_graph(df).await?;
    transaction.commit().await?;

    assert_eq!(storage.snapshot().await?.len(&ctx.state()).await?, 1);

    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn insert_named_graph(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.com/graph"));
    let transaction = storage.begin_transaction(&ctx.state()).await?;
    let df = graph_to_dataframe(&ctx, storage.as_ref(), graph.as_ref()).await;
    transaction.create_named_graph(df).await?;
    transaction.commit().await?;

    assert_named_graph_count(storage, &ctx, 1).await?;

    Ok(())
}

pub async fn remove_named_graph(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let graph = NamedNode::new_unchecked("http://example.com/graph");

    let transaction = storage.begin_transaction(&ctx.state()).await?;
    let df = graph_to_dataframe(&ctx, storage.as_ref(), graph.clone().as_ref()).await;
    transaction.create_named_graph(df).await?;
    transaction.commit().await?;

    let transaction = storage.begin_transaction(&ctx.state()).await?;
    let df2 = graph_to_dataframe(&ctx, storage.as_ref(), graph.clone().as_ref()).await;
    transaction.drop_graph(df2).await?;
    transaction.commit().await?;

    assert_named_graph_count(storage, &ctx, 0).await?;
    Ok(())
}

pub async fn clear_all(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    {
        let transaction = storage.begin_transaction(&ctx.state()).await?;
        transaction
            .insert(quads_to_plain_term_dataframe(
                &ctx,
                &[
                    example_quad(),
                    example_quad_in_graph("http://example.com/g1"),
                    example_quad_in_graph("http://example.com/g2"),
                ],
            ))
            .await?;
        transaction.commit().await?;
    }

    let snapshot = storage.snapshot().await?;
    assert_eq!(snapshot.len(&ctx.state()).await?, 3);

    let named_graphs =
        collect(snapshot.named_graphs(&ctx.state()).await?, ctx.task_ctx()).await?;
    assert_eq!(
        named_graphs.iter().map(|rb| rb.num_rows()).sum::<usize>(),
        2
    );

    let transaction = storage.begin_transaction(&ctx.state()).await?;
    let snapshot = storage.snapshot().await?;
    let named_graphs_df = snapshot.named_graphs(&ctx.state()).await?;
    let schema = Arc::clone(&named_graphs_df.schema());
    let mut batches = collect(named_graphs_df, ctx.task_ctx()).await?;
    let null_value = storage.encoding().create_null_scalar()?;
    batches.push(
        datafusion::arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![null_value.to_array()?],
        )
        .unwrap(),
    );
    let all_graphs_df = ctx.read_batches(batches)?;
    transaction.clear_graph(all_graphs_df).await?;
    transaction.commit().await?;

    assert_eq!(storage.snapshot().await?.len(&ctx.state()).await?, 0);

    storage.validate(&ctx.state()).await?;
    Ok(())
}

pub async fn optimize(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let transaction = storage.begin_transaction(&ctx.state()).await?;
    transaction
        .insert(quads_to_plain_term_dataframe(&ctx, &[example_quad()]))
        .await?;
    transaction.commit().await?;
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

/// Tries to provoke a failure by calling inserting quads with an empty input.
pub async fn empty_insert(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let transaction = storage.begin_transaction(&ctx.state()).await?;
    transaction
        .insert(quads_to_plain_term_dataframe(&ctx, &[]))
        .await?;
    transaction.commit().await?;
    storage.validate(&ctx.state()).await?;

    assert_eq!(storage.snapshot().await?.len(&ctx.state()).await?, 0);
    Ok(())
}

/// Tries to provoke a failure by removing quads with an empty input.
pub async fn empty_remove(storage: Arc<dyn QuadStorage>) -> Result<()> {
    let ctx = create_session_context(&storage).await;
    let transaction = storage.begin_transaction(&ctx.state()).await?;
    transaction
        .remove(quads_to_plain_term_dataframe(&ctx, &[]))
        .await?;
    transaction.commit().await?;
    storage.validate(&ctx.state()).await?;

    assert_eq!(storage.snapshot().await?.len(&ctx.state()).await?, 0);
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

async fn assert_named_graph_count(
    storage: Arc<dyn QuadStorage>,
    ctx: &SessionContext,
    expected: usize,
) -> Result<()> {
    let snapshot = storage.snapshot().await?;
    let exists = snapshot.named_graphs(&ctx.state()).await?;
    let result = execute_stream(exists, ctx.task_ctx())?
        .map(|batch| batch.unwrap())
        .collect::<Vec<_>>()
        .await;
    let count = result.iter().map(|rb| rb.num_rows()).sum::<usize>();
    assert_eq!(count, expected);
    Ok(())
}

async fn graph_to_dataframe<'a>(
    ctx: &SessionContext,
    _storage: &dyn QuadStorage,
    graph: impl Into<rdf_fusion::common::NamedOrBlankNodeRef<'a>>,
) -> datafusion::dataframe::DataFrame {
    let scalar_value = rdf_fusion::encoding::plain_term::PLAIN_TERM_ENCODING
        .encode_term(Ok(graph.into().into()))
        .unwrap()
        .into_scalar_value();
    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            rdf_fusion::common::quads::COL_GRAPH,
            scalar_value.data_type(),
            true,
        ),
    ]));
    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![scalar_value.to_array_of_size(1).expect("Valid array")],
    )
    .unwrap();
    ctx.read_batch(batch).unwrap()
}
