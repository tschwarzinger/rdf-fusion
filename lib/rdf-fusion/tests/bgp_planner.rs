use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::physical_plan::displayable;
use insta::assert_snapshot;
use object_store::memory::InMemory;
use rdf_fusion::common::{NamedNode, Quad};
use rdf_fusion::encoding::string::StringQuadsBuilder;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion::store::Store;
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_execution::RdfFusionContextBuilder;
use rdf_fusion_storage::parquet::ParquetQuadStorage;
use std::sync::Arc;
use url::Url;

#[tokio::test]
async fn test_bgp_planner_short_circuit() {
    // Create a Parquet file with some data using String encoding
    let context = datafusion::prelude::SessionContext::new();
    context.runtime_env().object_store_registry.register_store(
        &Url::parse("memory:///").unwrap(),
        Arc::new(InMemory::new()),
    );

    let mut builder = StringQuadsBuilder::with_capacity(1);
    builder.append_quad(
        Quad::new(
            NamedNode::new_unchecked("http://example.org/s1"),
            NamedNode::new_unchecked("http://example.org/p1"),
            NamedNode::new_unchecked("http://example.org/o1"),
            rdf_fusion_common::GraphNameRef::DefaultGraph,
        )
        .as_ref(),
    );
    let batch = builder.finish().into_record_batch();
    context
        .read_batch(batch)
        .unwrap()
        .write_parquet(
            "memory:///test.parquet",
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await
        .unwrap();

    let storage = ParquetQuadStorage::try_load(
        Url::parse("memory:///test.parquet").unwrap(),
        QuadStorageEncodingName::String,
        context.runtime_env().object_store_registry.as_ref(),
    )
    .await
    .unwrap();

    let context = RdfFusionContextBuilder::new(Arc::new(storage))
        .with_single_partition_session_config()
        .with_runtime_env(Some(Arc::clone(&context.runtime_env())))
        .build()
        .unwrap();
    let parquet_store = Store::new(context);

    // 1. Test short-circuiting for a pattern that matches nothing
    let query = "SELECT ?s WHERE { ?s <http://example.org/p1> <http://example.org/o1> . ?s <http://example.org/p2> ?o2 }";
    let (_, explanation) = parquet_store
        .explain_query_opt(query, QueryOptions::default())
        .await
        .unwrap();
    let plan = explanation.execution_plan;

    assert_snapshot!(displayable(plan.as_ref()).indent(true), @"
    ProjectionExec: expr=[ENC_PT(s@0) as s]
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s@0, s@0)], projection=[s@0]
        ParquetQuadStorageScanExec:, active_graph=Default Graph, triple_pattern=[?s <http://example.org/p2> ?o2], blank_node_mode=Variable, file_groups={1 group: [[test.parquet]]}, projection=[subject@1 as s], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = <http://example.org/p2>, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= <http://example.org/p2> AND <http://example.org/p2> <= predicate_max@2, required_guarantees=[predicate in (<http://example.org/p2>)]
        ParquetQuadStorageScanExec:, active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> <http://example.org/o1>], blank_node_mode=Variable, file_groups={1 group: [[test.parquet]]}, projection=[subject@1 as s], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = <http://example.org/p1> AND object@3 = <http://example.org/o1> AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= <http://example.org/p1> AND <http://example.org/p1> <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= <http://example.org/o1> AND <http://example.org/o1> <= object_max@6, required_guarantees=[object in (<http://example.org/o1>), predicate in (<http://example.org/p1>)]
    ");
}

#[tokio::test]
async fn test_bgp_planner_empty_bgp() {
    let context = datafusion::prelude::SessionContext::new();
    context.runtime_env().object_store_registry.register_store(
        &Url::parse("memory:///").unwrap(),
        Arc::new(InMemory::new()),
    );

    let mut builder = StringQuadsBuilder::with_capacity(1);
    builder.append_quad(
        Quad::new(
            NamedNode::new_unchecked("http://example.org/s1"),
            NamedNode::new_unchecked("http://example.org/p1"),
            NamedNode::new_unchecked("http://example.org/o1"),
            rdf_fusion_common::GraphNameRef::DefaultGraph,
        )
        .as_ref(),
    );
    let batch = builder.finish().into_record_batch();
    context
        .read_batch(batch)
        .unwrap()
        .write_parquet(
            "memory:///test.parquet",
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await
        .unwrap();

    let storage = ParquetQuadStorage::try_load(
        Url::parse("memory:///test.parquet").unwrap(),
        QuadStorageEncodingName::String,
        context.runtime_env().object_store_registry.as_ref(),
    )
    .await
    .unwrap();

    let context = RdfFusionContextBuilder::new(Arc::new(storage))
        .with_single_partition_session_config()
        .build()
        .unwrap();
    let parquet_store = Store::new(context);

    // SELECT with no patterns (empty BGP)
    let query = "SELECT * WHERE { }";
    let (_, explanation) = parquet_store
        .explain_query_opt(query, QueryOptions::default())
        .await
        .unwrap();
    let plan = explanation.execution_plan;

    assert_snapshot!(displayable(plan.as_ref()).indent(true), @"PlaceholderRowExec");
}
