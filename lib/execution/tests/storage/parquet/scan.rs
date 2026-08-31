use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use insta::assert_snapshot;
use object_store::memory::InMemory;
use rdf_fusion_common::sparql::SparqlParser;
use rdf_fusion_common::{NamedNode, Quad};
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_encoding::string::StringQuadsBuilder;
use rdf_fusion_execution::RdfFusionContext;
use rdf_fusion_execution::RdfFusionContextBuilder;
use rdf_fusion_execution::sparql::QueryOptions;
use rdf_fusion_execution::sparql::{RdfFusionQuery, plan_query};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;
use rdf_fusion_storage::block_cache::BlockCache;
use rdf_fusion_storage::parquet::ParquetQuadStorage;
use std::sync::Arc;
use url::Url;

#[tokio::test]
async fn test_parquet_scan_filter_pushdown_with_equality_with_named_node() {
    let (context, _) = prepare_test_store(
        &[(
            "http://example.org/s1",
            "http://example.org/p1",
            "http://example.org/o1",
        )],
        false,
        "test.parquet",
    )
    .await;

    let query_pushed = plan_query_from_str(
        &context,
        "SELECT ?s WHERE { ?s <http://example.org/p1> ?o . FILTER(?o = <http://example.org/o1>) }",
    );
    let (_, explanation_pushed) = context
        .execute_query(&query_pushed, QueryOptions::default())
        .await
        .unwrap();
    let plan_pushed = explanation_pushed.execution_plan;

    assert_snapshot!(
        displayable(plan_pushed.as_ref()).indent(true),
        @"ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> ?o], blank_node_mode=Variable, file_groups={1 group: [[test.parquet]]}, projection=[ENC_PT(subject@1) as s], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = <http://example.org/p1> AND object@3 = <http://example.org/o1>, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= <http://example.org/p1> AND <http://example.org/p1> <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= <http://example.org/o1> AND <http://example.org/o1> <= object_max@6, required_guarantees=[object in (<http://example.org/o1>), predicate in (<http://example.org/p1>)]"
    );
}

#[tokio::test]
async fn test_parquet_scan_filter_pushdown_with_function_prevented() {
    let (context, _) = prepare_test_store(
        &[(
            "http://example.org/s1",
            "http://example.org/p1",
            "http://example.org/o1",
        )],
        false,
        "test.parquet",
    )
    .await;

    let query_not_pushed = plan_query_from_str(
        &context,
        "SELECT ?s WHERE { ?s <http://example.org/p1> ?o . FILTER(LCASE(STR(?o)) = \"http://example.org/o1\") }",
    );
    let (_, explanation_not_pushed) = context
        .execute_query(&query_not_pushed, QueryOptions::default())
        .await
        .unwrap();
    let plan_not_pushed = explanation_not_pushed.execution_plan;

    assert_snapshot!(displayable(plan_not_pushed.as_ref()).indent(true), @"
        ProjectionExec: expr=[ENC_PT(s@0) as s]
          FilterExec: EBV(EQ(LCASE(ENC_TF(STR(ENC_PT(o@1)))), 3:{value:http://example.org/o1,language:})), projection=[s@0]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> ?o], blank_node_mode=Variable, file_groups={1 group: [[test.parquet]]}, projection=[subject@1 as s, object@3 as o], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = <http://example.org/p1>, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= <http://example.org/p1> AND <http://example.org/p1> <= predicate_max@2, required_guarantees=[predicate in (<http://example.org/p1>)]
        ");
}

#[tokio::test]
async fn test_parquet_bloom_filter_cache_hits() {
    let (rdf_context, storage) = prepare_test_store(
        &[
            (
                "http://example.org/s1",
                "http://example.org/p1",
                "http://example.org/o1",
            ),
            (
                "http://example.org/s2",
                "http://example.org/p1",
                "http://example.org/o3",
            ),
        ],
        true,
        "test_bloom.parquet",
    )
    .await;
    let cache = storage.bloom_filter_cache().clone();

    let query = plan_query_from_str(
        &rdf_context,
        "SELECT ?s WHERE { ?s ?p <http://example.org/o2> . }",
    );
    let (results, _) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();

    // Consume the results to drive execution.
    if let rdf_fusion_execution::results::QueryResults::Solutions(mut solutions) = results
    {
        use futures::StreamExt;
        while let Some(row) = solutions.next().await {
            row.unwrap();
        }
    }

    let hits = cache.hit_count();
    assert_eq!(hits, 1);
}

#[tokio::test]
async fn test_parquet_scan_with_caching() {
    let cache = Arc::new(BlockCache::new(4096, 128));
    let (rdf_context, storage) = prepare_test_store_with_cache(
        &[
            (
                "http://example.org/s1",
                "http://example.org/p1",
                "http://example.org/o1",
            ),
            (
                "http://example.org/s2",
                "http://example.org/p2",
                "http://example.org/o2",
            ),
        ],
        false,
        "cached_test.parquet",
        Some(Arc::clone(&cache)),
    )
    .await;

    assert!(storage.cache().is_some());

    let query = plan_query_from_str(
        &rdf_context,
        "SELECT ?s ?o WHERE { ?s <http://example.org/p1> ?o . }",
    );
    let (results, _) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();

    if let rdf_fusion_execution::results::QueryResults::Solutions(mut solutions) = results
    {
        use futures::StreamExt;
        let mut count = 0;
        while let Some(row) = solutions.next().await {
            row.unwrap();
            count += 1;
        }
        assert_eq!(count, 1);
    } else {
        panic!("Expected solutions");
    }
}

async fn prepare_test_store(
    quads: &[(&str, &str, &str)],
    enable_bloom: bool,
    filename: &str,
) -> (RdfFusionContext, Arc<ParquetQuadStorage>) {
    prepare_test_store_with_cache(quads, enable_bloom, filename, None).await
}

async fn prepare_test_store_with_cache(
    quads: &[(&str, &str, &str)],
    enable_bloom: bool,
    filename: &str,
    cache: Option<Arc<BlockCache>>,
) -> (RdfFusionContext, Arc<ParquetQuadStorage>) {
    let session_config = SessionConfig::default();
    let context = SessionContext::new_with_config(session_config);
    context.runtime_env().object_store_registry.register_store(
        &Url::parse("memory:///").unwrap(),
        Arc::new(InMemory::new()),
    );

    let mut builder = StringQuadsBuilder::with_capacity(quads.len());
    for &(s, p, o) in quads {
        builder.append_quad(
            Quad::new(
                NamedNode::new_unchecked(s),
                NamedNode::new_unchecked(p),
                NamedNode::new_unchecked(o),
                rdf_fusion_common::GraphNameRef::DefaultGraph,
            )
            .as_ref(),
        );
    }
    let batch = builder.finish().into_record_batch();
    let path = format!("memory:///{filename}");

    let mut options = TableParquetOptions::default();
    options.global.bloom_filter_on_write = enable_bloom;
    context
        .read_batch(batch)
        .unwrap()
        .write_parquet(
            &path,
            DataFrameWriteOptions::new().with_single_file_output(true),
            Some(options),
        )
        .await
        .unwrap();

    let runtime = context.runtime_env();
    let mut storage_builder = ParquetQuadStorage::builder(Url::parse(&path).unwrap())
        .with_encoding(QuadStorageEncodingName::String)
        .with_object_store_registry(runtime.object_store_registry.as_ref());

    if let Some(cache) = cache {
        storage_builder = storage_builder.with_cache(Some(cache));
    }

    let storage = Arc::new(storage_builder.build().await.unwrap());

    let rdf_context =
        RdfFusionContextBuilder::new(Arc::clone(&storage) as Arc<dyn QuadStorage>)
            .with_single_partition_session_config()
            .with_runtime_env(Some(Arc::clone(&context.runtime_env())))
            .build()
            .unwrap();

    (rdf_context, storage)
}

fn plan_query_from_str(context: &RdfFusionContext, query: &str) -> RdfFusionQuery {
    let parsed = SparqlParser::new().parse_query(query).unwrap();
    let builder_context = RdfFusionLogicalPlanBuilderContext::new(context.create_view());
    plan_query(builder_context, parsed, None, &Default::default()).unwrap()
}
