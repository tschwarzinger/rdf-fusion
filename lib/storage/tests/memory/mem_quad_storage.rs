use crate::{example_quad, example_quad_in_graph};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use futures::StreamExt;
use insta::assert_debug_snapshot;
use rdf_fusion_encoding::object_id::{ObjectIdEncoding, ObjectIdMapping};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_model::BlankNodeMatchingMode;
use rdf_fusion_model::{
    GraphNameRef, NamedNode, NamedNodePattern, NamedOrBlankNode, TermPattern,
    TriplePattern, Variable,
};
use rdf_fusion_storage::memory::{MemObjectIdMapping, MemQuadStorage};
use std::sync::Arc;
use tokio;

#[tokio::test]
async fn insert_quad() {
    let storage = create_storage();

    let inserted = storage.extend(vec![example_quad()]).await.unwrap();
    assert_eq!(inserted, 1);

    let len = storage.len().await.unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn insert_quad_then_read() {
    let storage = create_storage();

    let inserted = storage.extend(vec![example_quad()]).await.unwrap();
    assert_eq!(inserted, 1);

    let ep_metrics = ExecutionPlanMetricsSet::default();
    let metrics = BaselineMetrics::new(&ep_metrics, 0);

    let batch = storage
        .snapshot()
        .await
        .plan_pattern_evaluation(
            ActiveGraph::DefaultGraph,
            Some(Variable::new_unchecked("g")),
            TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked("s")),
                predicate: NamedNodePattern::Variable(Variable::new_unchecked("p")),
                object: TermPattern::Variable(Variable::new_unchecked("o")),
            },
            BlankNodeMatchingMode::Filter,
        )
        .await
        .unwrap()
        .create_stream(metrics)
        .next()
        .await
        .unwrap()
        .unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_debug_snapshot!(batch, @r#"
    RecordBatch {
        schema: Schema {
            fields: [
                Field {
                    name: "g",
                    data_type: FixedSizeBinary(
                        4,
                    ),
                    nullable: true,
                },
                Field {
                    name: "s",
                    data_type: FixedSizeBinary(
                        4,
                    ),
                },
                Field {
                    name: "p",
                    data_type: FixedSizeBinary(
                        4,
                    ),
                },
                Field {
                    name: "o",
                    data_type: FixedSizeBinary(
                        4,
                    ),
                },
            ],
            metadata: {},
        },
        columns: [
            FixedSizeBinaryArray<4>
            [
              null,
            ],
            FixedSizeBinaryArray<4>
            [
              [
                0,
                0,
                0,
                1,
            ],
            ],
            FixedSizeBinaryArray<4>
            [
              [
                0,
                0,
                0,
                2,
            ],
            ],
            FixedSizeBinaryArray<4>
            [
              [
                0,
                0,
                0,
                3,
            ],
            ],
        ],
        row_count: 1,
    }
    "#);
}

#[tokio::test]
async fn insert_duplicate_quads_no_effect() {
    let storage = create_storage();

    storage.extend(vec![example_quad()]).await.unwrap();

    let inserted = storage.extend(vec![example_quad()]).await.unwrap();
    assert_eq!(inserted, 0); // duplicate
}

#[tokio::test]
async fn insert_duplicate_quads_in_same_operation_quads() {
    let storage = create_storage();

    let inserted = storage
        .extend(vec![example_quad(), example_quad()])
        .await
        .unwrap();

    assert_eq!(inserted, 1);
}

#[tokio::test]
async fn named_graph_insertion_and_query() {
    let storage = create_storage();
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.com/graph").unwrap());

    let inserted = storage.insert_named_graph(graph.as_ref()).await.unwrap();
    assert!(inserted);

    let exists = storage.contains_named_graph(graph.as_ref()).await.unwrap();
    assert!(exists);

    let graphs = storage.named_graphs().await.unwrap();
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0], graph);
}

#[tokio::test]
async fn remove_quad() {
    let storage = create_storage();
    let quad = example_quad_in_graph("http://example.com/g");

    storage.extend(vec![quad.clone()]).await.unwrap();
    let removed = storage.remove(quad.as_ref()).await.unwrap();
    assert!(removed);

    let len = storage.len().await.unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
async fn clear_graph() {
    let storage = create_storage();

    let g1 = "http://example.com/g1";
    let g2 = "http://example.com/g2";

    storage
        .extend(vec![example_quad_in_graph(g1), example_quad_in_graph(g2)])
        .await
        .unwrap();

    storage
        .clear_graph(GraphNameRef::NamedNode(
            NamedNode::new(g1).unwrap().as_ref(),
        ))
        .await
        .unwrap();

    let len = storage.len().await.unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn insert_named_graph() {
    let storage = create_storage();
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.com/graph").unwrap());
    storage.insert_named_graph(graph.as_ref()).await.unwrap();
    let exists = storage.contains_named_graph(graph.as_ref()).await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn remove_named_graph() {
    let storage = create_storage();
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.com/graph").unwrap());

    storage.insert_named_graph(graph.as_ref()).await.unwrap();
    let removed = storage.drop_named_graph(graph.as_ref()).await.unwrap();
    assert!(removed);

    let exists = storage.contains_named_graph(graph.as_ref()).await.unwrap();
    assert!(!exists);
}

#[tokio::test]
async fn clear_all() {
    let storage = create_storage();
    storage
        .extend(vec![
            example_quad(), // default graph
            example_quad_in_graph("http://example.com/g1"),
            example_quad_in_graph("http://example.com/g2"),
        ])
        .await
        .unwrap();

    storage.clear().await.unwrap();
    let len = storage.len().await.unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
#[ignore = "Currently we lock the entire storage for snapshotting, so this test dead locks."]
async fn snapshot_consistency() {
    let storage = create_storage();
    storage
        .extend(vec![example_quad_in_graph("http://g")])
        .await
        .unwrap();

    let snapshot = storage.snapshot().await;

    // Update storage after snapshot
    storage.clear().await.unwrap();

    // Snapshot should still see the original quad
    assert_eq!(snapshot.len(), 1);
}

#[tokio::test]
async fn validate_storage() {
    let storage = create_storage();

    storage
        .extend(vec![example_quad_in_graph("http://g")])
        .await
        .unwrap();

    let result = storage.validate().await;
    assert!(result.is_ok());
}

fn create_storage() -> MemQuadStorage {
    let mapping = Arc::new(MemObjectIdMapping::new());
    let encoding = Arc::new(ObjectIdEncoding::new(
        Arc::clone(&mapping) as Arc<dyn ObjectIdMapping>
    ));
    MemQuadStorage::try_new(encoding, 10).unwrap()
}
