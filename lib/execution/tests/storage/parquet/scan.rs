use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
use datafusion::logical_expr::SortExpr;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext, col};
use insta::assert_snapshot;
use object_store::memory::InMemory;
use rdf_fusion_common::config::RdfFusionOptions;
use rdf_fusion_common::{NamedNode, Quad};
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_encoding::string::StringQuadsBuilder;
use rdf_fusion_execution::RdfFusionContext;
use rdf_fusion_execution::RdfFusionContextBuilder;
use rdf_fusion_execution::results::QueryResults;
use rdf_fusion_execution::sparql::QueryOptions;
use rdf_fusion_execution::sparql::RdfFusionQuery;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_sparql_parser::ParserOptions;
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

/// Two quads whose objects (o1 and o3) surround o2. Hence, only the Bloom filter can prune a
/// pattern that binds the object to o2.
const BLOOM_FILTER_TEST_QUADS: [(&str, &str, &str); 2] = [
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
];

#[tokio::test]
async fn test_parquet_bloom_filter_pruning_during_planning() {
    let cache = Arc::new(BlockCache::new(4096, 128));
    let (rdf_context, _) = prepare_test_store_with_cache(
        &BLOOM_FILTER_TEST_QUADS,
        true,
        "test_bloom.parquet",
        Some(Arc::clone(&cache)),
    )
    .await;

    // The file has no sort order. Hence, the Bloom filter of the bound object is used during
    // planning and the file is removed from the scan.
    let query = plan_query_from_str(
        &rdf_context,
        "SELECT ?s WHERE { ?s ?p <http://example.org/o2> . }",
    );
    let (results, explanation) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();
    assert_eq!(count_solutions(results).await, 0);

    let plan = displayable(explanation.execution_plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        !plan.contains("test_bloom.parquet"),
        "Expected the file to be pruned during planning: {plan}"
    );
    assert!(!cache.is_empty(), "Expected the Bloom filter to be cached");
}

#[tokio::test]
async fn test_parquet_bloom_filter_pruning_at_execution_time_without_cache() {
    let (rdf_context, _) = prepare_test_store(
        &BLOOM_FILTER_TEST_QUADS,
        true,
        "test_bloom_no_cache.parquet",
    )
    .await;

    // Without a block cache, the Bloom filters would be read twice. Hence, by default, they are
    // only used during execution.
    let query = plan_query_from_str(
        &rdf_context,
        "SELECT ?s WHERE { ?s ?p <http://example.org/o2> . }",
    );
    let (results, explanation) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();
    assert_eq!(count_solutions(results).await, 0);

    let metrics =
        DisplayableExecutionPlan::with_metrics(explanation.execution_plan.as_ref())
            .indent(true)
            .to_string();
    assert!(
        metrics.contains("row_groups_pruned_bloom_filter=1 total \u{2192} 0 matched"),
        "Expected the Bloom filter to prune the row group during execution: {metrics}"
    );
}

#[tokio::test]
async fn test_parquet_bloom_filter_pruning_at_execution_time_for_sorted_prefix() {
    let cache = Arc::new(BlockCache::new(4096, 128));
    let (rdf_context, _) = prepare_test_store_with_options(
        &BLOOM_FILTER_TEST_QUADS,
        true,
        "test_bloom_sorted.parquet",
        Some(Arc::clone(&cache)),
        0,
        vec![col("object").sort(true, true)],
    )
    .await;

    // The object is the prefix of the sort order. Hence, the Bloom filter is not used during
    // planning but only during execution.
    let query = plan_query_from_str(
        &rdf_context,
        "SELECT ?s WHERE { ?s ?p <http://example.org/o2> . }",
    );
    let (results, explanation) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();
    assert_eq!(count_solutions(results).await, 0);

    let metrics =
        DisplayableExecutionPlan::with_metrics(explanation.execution_plan.as_ref())
            .indent(true)
            .to_string();
    assert!(
        metrics.contains("test_bloom_sorted.parquet"),
        "Expected the file to remain in the scan: {metrics}"
    );
    assert!(
        // This is pruning during execution.
        metrics.contains("row_groups_pruned_bloom_filter=1 total \u{2192} 0 matched"),
        "Expected the Bloom filter to prune the row group: {metrics}"
    );
}

#[tokio::test]
async fn test_parquet_storage_derives_sort_order_from_footers() {
    let (_, sorted_storage) = prepare_test_store_with_options(
        &BLOOM_FILTER_TEST_QUADS,
        false,
        "test_sorted.parquet",
        None,
        0,
        vec![
            col("subject").sort(true, true),
            col("object").sort(true, true),
        ],
    )
    .await;
    assert_eq!(
        sorted_storage
            .output_ordering()
            .map(|ordering| ordering.to_string()),
        Some("subject@1 ASC, object@3 ASC".to_owned())
    );

    let (_, unsorted_storage) =
        prepare_test_store(&BLOOM_FILTER_TEST_QUADS, false, "test_unsorted.parquet")
            .await;
    assert!(unsorted_storage.output_ordering().is_none());
}

/// A URL that points to a single object is used, even if the object has no `.parquet` extension
/// (e.g., objects in IndexedDB).
#[tokio::test]
async fn test_parquet_storage_reads_single_file_without_extension() {
    let (_, storage) =
        prepare_test_store(&BLOOM_FILTER_TEST_QUADS, false, "dataset-without-extension")
            .await;
    assert_eq!(storage.files().len(), 1);
}

#[tokio::test]
async fn test_parquet_scan_sort_order_enables_sorted_aggregation() {
    let query = "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?s";

    // The file is sorted by subject. Hence, the grouping can exploit the order of the scan.
    let (rdf_context, _) = prepare_test_store_with_options(
        &BLOOM_FILTER_TEST_QUADS,
        false,
        "test_sorted_aggregation.parquet",
        None,
        0,
        vec![
            col("subject").sort(true, true),
            col("object").sort(true, true),
        ],
    )
    .await;
    let plan = plan_to_string(&rdf_context, query).await;
    assert!(
        plan.contains("ordering_mode=Sorted"),
        "Expected a sorted aggregation: {plan}"
    );

    let (rdf_context, _) = prepare_test_store(
        &BLOOM_FILTER_TEST_QUADS,
        false,
        "test_unsorted_aggregation.parquet",
    )
    .await;
    let plan = plan_to_string(&rdf_context, query).await;
    assert!(
        !plan.contains("ordering_mode=Sorted"),
        "Expected an unsorted aggregation: {plan}"
    );
}

#[tokio::test]
async fn test_parquet_multi_file_eagerly_loads_footers() {
    let (rdf_context, storage) =
        prepare_multi_file_test_store(&multi_file_data(), 1).await;

    assert_eq!(storage.files().len(), 3);
    assert_eq!(
        cached_metadata(&rdf_context, &storage),
        vec![
            ("dataset/part-00000.parquet".to_owned(), false),
            ("dataset/part-00001.parquet".to_owned(), false),
            ("dataset/part-00002.parquet".to_owned(), false),
        ]
    );
}

#[tokio::test]
async fn test_parquet_multi_file_loads_page_index_only_for_remaining_files() {
    let (rdf_context, storage) =
        prepare_multi_file_test_store(&multi_file_data(), 1).await;

    // s3 is within the subject range of part-00001, which also contains s4. Hence, the page index
    // could prune pages.
    execute_query(
        &rdf_context,
        "SELECT ?p ?o WHERE { <http://example.org/s3> ?p ?o . }",
    )
    .await;

    // Only the file that survives the row group pruning has its page index loaded.
    assert_eq!(
        cached_metadata(&rdf_context, &storage),
        vec![
            ("dataset/part-00000.parquet".to_owned(), false),
            ("dataset/part-00001.parquet".to_owned(), true),
            ("dataset/part-00002.parquet".to_owned(), false),
        ]
    );
}

#[tokio::test]
async fn test_parquet_scan_reports_bytes_scanned_during_planning() {
    let (rdf_context, _) = prepare_multi_file_test_store(&multi_file_data(), 1).await;
    let query = plan_query_from_str(
        &rdf_context,
        "SELECT ?p ?o WHERE { <http://example.org/s3> ?p ?o . }",
    );

    // The first query loads the page index during planning. Afterward, it is served from the
    // metadata cache.
    let mut bytes_scanned_planning = Vec::new();
    for _ in 0..2 {
        let (results, explanation) = rdf_context
            .execute_query(&query, QueryOptions::default())
            .await
            .unwrap();
        count_solutions(results).await;
        bytes_scanned_planning.push(sum_metric(
            &explanation.execution_plan,
            "bytes_scanned_planning",
        ));
    }

    assert!(
        bytes_scanned_planning[0] > 0,
        "Expected the page index to be read during planning: {bytes_scanned_planning:?}"
    );
    assert_eq!(bytes_scanned_planning[1], 0);
}

#[tokio::test]
async fn test_parquet_multi_file_skips_page_index_if_all_rows_match() {
    let (rdf_context, storage) =
        prepare_multi_file_test_store(&multi_file_data(), 1).await;

    // All quads in part-00001 have the predicate p2. Hence, the page index cannot prune any page.
    let results = execute_query(
        &rdf_context,
        "SELECT ?s ?o WHERE { ?s <http://example.org/p2> ?o . }",
    )
    .await;
    assert_eq!(results, 2);

    assert_eq!(
        cached_metadata(&rdf_context, &storage),
        vec![
            ("dataset/part-00000.parquet".to_owned(), false),
            ("dataset/part-00001.parquet".to_owned(), false),
            ("dataset/part-00002.parquet".to_owned(), false),
        ]
    );
}

#[tokio::test]
async fn test_parquet_multi_file_scan_prunes_files_during_planning() {
    let (rdf_context, _) = prepare_multi_file_test_store(&multi_file_data(), 1).await;

    let query = plan_query_from_str(
        &rdf_context,
        "SELECT ?s ?o WHERE { ?s <http://example.org/p2> ?o . }",
    );
    let (results, explanation) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();

    // Only the file that contains p2 remains in the plan.
    assert_snapshot!(
        displayable(explanation.execution_plan.as_ref()).indent(true),
        @"ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p2> ?o], blank_node_mode=Variable, file_groups={1 group: [[dataset/part-00001.parquet]]}, projection=[ENC_PT(subject@1) as s, ENC_PT(object@3) as o], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = <http://example.org/p2>, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= <http://example.org/p2> AND <http://example.org/p2> <= predicate_max@2, required_guarantees=[predicate in (<http://example.org/p2>)]"
    );
    assert_eq!(count_solutions(results).await, 2);
}

#[tokio::test]
async fn test_parquet_multi_file_scan_all_files() {
    let (rdf_context, storage) =
        prepare_multi_file_test_store(&multi_file_data(), 2).await;

    let query = plan_query_from_str(&rdf_context, "SELECT ?s ?o WHERE { ?s ?p ?o . }");
    let (results, explanation) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();

    // The files are spread across the two partitions.
    assert_snapshot!(
        displayable(explanation.execution_plan.as_ref()).indent(true),
        @"ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s ?p ?o], blank_node_mode=Variable, file_groups={2 groups: [[dataset/part-00000.parquet, dataset/part-00001.parquet], [dataset/part-00002.parquet]]}, projection=[ENC_PT(subject@1) as s, ENC_PT(object@3) as o], file_type=parquet, predicate=graph@0 IS NULL, pruning_predicate=graph_null_count@0 > 0, required_guarantees=[]"
    );
    assert_eq!(count_solutions(results).await, 6);

    let state = rdf_context.session_context().state();
    assert_eq!(
        storage.snapshot().await.unwrap().len(&state).await.unwrap(),
        6
    );
}

#[tokio::test]
async fn test_parquet_multi_file_scan_all_files_pruned() {
    let (rdf_context, _) = prepare_multi_file_test_store(&multi_file_data(), 1).await;

    let query = plan_query_from_str(
        &rdf_context,
        "SELECT ?s WHERE { ?s <http://example.org/p9> ?o . }",
    );
    let (results, _) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();
    assert_eq!(count_solutions(results).await, 0);
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

    if let QueryResults::Solutions(mut solutions) = results {
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

#[tokio::test]
async fn test_parquet_scan_small_scan_buffering() {
    let (rdf_context, _storage) = prepare_test_store_with_options(
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
        "buffered_test.parquet",
        None,
        1024 * 1024,
        vec![],
    )
    .await;

    let query = plan_query_from_str(&rdf_context, "SELECT ?s ?o WHERE { ?s ?p ?o . }");
    let (_, explanation) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();

    let plan = displayable(explanation.execution_plan.as_ref())
        .indent(false)
        .to_string();
    assert_snapshot!(
        plan,
        @"
    BufferExec: capacity=1048576
      ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[buffered_test.parquet]]}, projection=[ENC_PT(subject@1) as s, ENC_PT(object@3) as o], file_type=parquet, predicate=graph@0 IS NULL, pruning_predicate=graph_null_count@0 > 0, required_guarantees=[]
    "
    );
}

async fn prepare_test_store(
    quads: &[(&str, &str, &str)],
    enable_bloom: bool,
    filename: &str,
) -> (RdfFusionContext, Arc<ParquetQuadStorage>) {
    prepare_test_store_with_options(quads, enable_bloom, filename, None, 0, vec![]).await
}

async fn prepare_test_store_with_cache(
    quads: &[(&str, &str, &str)],
    enable_bloom: bool,
    filename: &str,
    cache: Option<Arc<BlockCache>>,
) -> (RdfFusionContext, Arc<ParquetQuadStorage>) {
    prepare_test_store_with_options(quads, enable_bloom, filename, cache, 0, vec![]).await
}

async fn prepare_test_store_with_options(
    quads: &[(&str, &str, &str)],
    enable_bloom: bool,
    filename: &str,
    cache: Option<Arc<BlockCache>>,
    small_scan_buffering_threshold: usize,
    sort_by: Vec<SortExpr>,
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
            DataFrameWriteOptions::new()
                .with_single_file_output(true)
                .with_sort_by(sort_by),
            Some(options),
        )
        .await
        .unwrap();

    let state = context.state();
    let mut storage_builder = ParquetQuadStorage::builder(Url::parse(&path).unwrap())
        .with_encoding(QuadStorageEncodingName::String);

    if let Some(cache) = cache {
        storage_builder = storage_builder.with_cache(Some(cache));
    }

    let storage = Arc::new(storage_builder.build(&state).await.unwrap());

    let mut rdf_session_config = SessionConfig::new()
        .with_batch_size(8192)
        .with_target_partitions(1);
    rdf_session_config
        .options_mut()
        .execution
        .parquet
        .pushdown_filters = true;
    if small_scan_buffering_threshold > 0 {
        let mut rdf_options = RdfFusionOptions::default();
        rdf_options.execution.small_scan_buffering_threshold =
            small_scan_buffering_threshold;
        rdf_session_config
            .options_mut()
            .extensions
            .insert(rdf_options);
    }

    let rdf_context =
        RdfFusionContextBuilder::new(Arc::clone(&storage) as Arc<dyn QuadStorage>)
            .with_session_config(Some(rdf_session_config))
            .with_runtime_env(Some(Arc::clone(&context.runtime_env())))
            .build()
            .unwrap();

    (rdf_context, storage)
}

fn plan_query_from_str(context: &RdfFusionContext, query: &str) -> RdfFusionQuery {
    rdf_fusion_sparql_parser::parse_query(
        &context.create_view(),
        query,
        &ParserOptions::default(),
    )
    .unwrap()
}

/// Returns, for each file of the storage that is in the metadata cache, whether its page index is
/// cached.
fn cached_metadata(
    rdf_context: &RdfFusionContext,
    storage: &ParquetQuadStorage,
) -> Vec<(String, bool)> {
    let metadata_cache = rdf_context
        .session_context()
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache();
    storage
        .files()
        .iter()
        .filter_map(|file| {
            let location = &file.object_meta.location;
            let entry = metadata_cache.get(location)?;
            let metadata = entry
                .file_metadata
                .as_any()
                .downcast_ref::<CachedParquetMetaData>()
                .expect("Expected Parquet metadata")
                .parquet_metadata();
            let has_page_index =
                metadata.column_index().is_some() && metadata.offset_index().is_some();
            Some((location.to_string(), has_page_index))
        })
        .collect()
}

/// Sums the metric with the given name over all nodes of the plan.
fn sum_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let own = plan
        .metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map(|value| value.as_usize())
        .unwrap_or(0);
    own + plan
        .children()
        .into_iter()
        .map(|child| sum_metric(child, name))
        .sum::<usize>()
}

/// Executes the query and returns the physical plan as a string.
async fn plan_to_string(rdf_context: &RdfFusionContext, query: &str) -> String {
    let query = plan_query_from_str(rdf_context, query);
    let (_, explanation) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();
    displayable(explanation.execution_plan.as_ref())
        .indent(true)
        .to_string()
}

/// Executes the query and returns the number of solutions.
async fn execute_query(rdf_context: &RdfFusionContext, query: &str) -> usize {
    let query = plan_query_from_str(rdf_context, query);
    let (results, _) = rdf_context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();
    count_solutions(results).await
}

async fn count_solutions(results: QueryResults) -> usize {
    use futures::StreamExt;
    let QueryResults::Solutions(mut solutions) = results else {
        panic!("Expected solutions");
    };
    let mut count = 0;
    while let Some(row) = solutions.next().await {
        row.unwrap();
        count += 1;
    }
    count
}

type TestFile<'a> = (&'a str, Vec<(&'a str, &'a str, &'a str)>);

/// Three files, each containing the quads of a single predicate.
fn multi_file_data() -> Vec<TestFile<'static>> {
    vec![
        (
            "part-00000.parquet",
            vec![
                (
                    "http://example.org/s1",
                    "http://example.org/p1",
                    "http://example.org/o1",
                ),
                (
                    "http://example.org/s2",
                    "http://example.org/p1",
                    "http://example.org/o2",
                ),
            ],
        ),
        (
            "part-00001.parquet",
            vec![
                (
                    "http://example.org/s3",
                    "http://example.org/p2",
                    "http://example.org/o3",
                ),
                (
                    "http://example.org/s4",
                    "http://example.org/p2",
                    "http://example.org/o4",
                ),
            ],
        ),
        (
            "part-00002.parquet",
            vec![
                (
                    "http://example.org/s5",
                    "http://example.org/p3",
                    "http://example.org/o5",
                ),
                (
                    "http://example.org/s6",
                    "http://example.org/p3",
                    "http://example.org/o6",
                ),
            ],
        ),
    ]
}

/// Writes each file into `memory:///dataset/` and creates a storage for the entire directory.
async fn prepare_multi_file_test_store(
    files: &[TestFile<'_>],
    target_partitions: usize,
) -> (RdfFusionContext, Arc<ParquetQuadStorage>) {
    let context = SessionContext::new_with_config(SessionConfig::default());
    context.runtime_env().object_store_registry.register_store(
        &Url::parse("memory:///").unwrap(),
        Arc::new(InMemory::new()),
    );

    for (filename, quads) in files {
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
        context
            .read_batch(builder.finish().into_record_batch())
            .unwrap()
            .write_parquet(
                &format!("memory:///dataset/{filename}"),
                DataFrameWriteOptions::new().with_single_file_output(true),
                None,
            )
            .await
            .unwrap();
    }

    let state = context.state();
    let storage = Arc::new(
        ParquetQuadStorage::builder(Url::parse("memory:///dataset/").unwrap())
            .with_encoding(QuadStorageEncodingName::String)
            .build(&state)
            .await
            .unwrap(),
    );

    let rdf_session_config = SessionConfig::new()
        .with_batch_size(8192)
        .with_target_partitions(target_partitions);
    let rdf_context =
        RdfFusionContextBuilder::new(Arc::clone(&storage) as Arc<dyn QuadStorage>)
            .with_session_config(Some(rdf_session_config))
            .with_runtime_env(Some(Arc::clone(&context.runtime_env())))
            .build()
            .unwrap();

    (rdf_context, storage)
}
