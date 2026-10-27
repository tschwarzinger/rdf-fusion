//! Tests for switching a quad pattern scan of a Parquet storage to another quad table at
//! execution time.

use crate::storage::switch::{
    DISABLE_SWITCHING, PREFIX, SwitchTest, rdf_fusion_context, test_ntriples,
};
use insta::assert_snapshot;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use rdf_fusion_common::{GraphName, RdfInput, RdfSortOrder};
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_storage::delta::DeltaQuadsStorageBuilder;
use rdf_fusion_storage::parquet::{ParquetQuadStorage, RdfParquetLoader};
use std::sync::Arc;
use url::Url;

#[tokio::test]
async fn test_switch_selective_join_switches_probe_scan() {
    let env = create_switch_test().await;
    // Three authors. In GPOS (chosen for the pattern with only the predicate bound), the subjects
    // within the predicate are ordered by the name. Hence, GPOS cannot prune the authors.
    let query = format!(
        "{PREFIX} SELECT ?author ?name WHERE {{ ex:pub3 ex:authoredBy ?author . ?author ex:name ?name }}"
    );

    let switched = env.run(&query, &[]).await;
    let planned = env.run(&query, &[DISABLE_SWITCHING]).await;

    assert_eq!(switched.solutions, planned.solutions);
    assert_eq!(switched.solutions.len(), 3);
    assert_snapshot!(switched.probe_scan_switch(), @"[switched to Native(GSPO) for 3 values of subject, estimated_rows=324 instead of 20053]");
    // By default, only the quad table and the estimated rows are shown.
    assert!(
        switched
            .plan_default
            .contains("quad_table_switch=[Native(GSPO), estimated_rows=324]"),
        "{}",
        switched.plan_default
    );
    // The alternative scan only reads the pages of the three authors. Planning it loads the
    // metadata of the GSPO file, which is reported in `switch_bytes_scanned_planning`.
    assert!(
        switched.bytes_scanned * 4 < planned.bytes_scanned,
        "Expected the switched scan to read much less: {} vs. {}",
        switched.bytes_scanned,
        planned.bytes_scanned
    );
    assert_eq!(
        switched.bytes_scanned_planning,
        planned.bytes_scanned_planning
    );
    assert!(switched.switch_bytes_scanned_planning > 0);
    assert_eq!(planned.switch_bytes_scanned_planning, 0);
    insta::with_settings!({filters => vec![(r"/[A-Za-z0-9]{16}_", "/[WRITE_ID]_")]}, {
        assert_snapshot!(switched.plan_without_metrics, @"
        ProjectionExec: expr=[ENC_PT(author@0) as author, ENC_PT(name@1) as name]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(author@0, author@0)], projection=[author@0, name@2]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<http://example.org/pub3> <http://example.org/authoredBy> ?author], blank_node_mode=Variable, file_groups={1 group: [[dataset/gspo/[WRITE_ID]_3.parquet]]}, projection=[object@3 as author], file_type=parquet, predicate=subject@1 = <http://example.org/pub3> AND predicate@2 = <http://example.org/authoredBy>, pruning_predicate=subject_null_count@2 != row_count@3 AND subject_min@0 <= <http://example.org/pub3> AND <http://example.org/pub3> <= subject_max@1 AND predicate_null_count@6 != row_count@3 AND predicate_min@4 <= <http://example.org/authoredBy> AND <http://example.org/authoredBy> <= predicate_max@5, required_guarantees=[predicate in (<http://example.org/authoredBy>), subject in (<http://example.org/pub3>)]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?author <http://example.org/name> ?name], blank_node_mode=Variable, file_groups={1 group: [[dataset/gpos/[WRITE_ID]_0.parquet, dataset/gpos/[WRITE_ID]_1.parquet, dataset/gpos/[WRITE_ID]_2.parquet, dataset/gpos/[WRITE_ID]_3.parquet]]}, projection=[subject@1 as author, object@3 as name], file_type=parquet, predicate=predicate@2 = <http://example.org/name> AND DynamicFilter [ subject@1 >= <http://example.org/author00003> AND subject@1 <= <http://example.org/author05002> AND subject@1 IN (SET) ([<http://example.org/author00003>, <http://example.org/author01003>, <http://example.org/author05002>]) ], dynamic_rg_pruning=eligible, pruning_predicate=predicate_null_count@2 != row_count@3 AND predicate_min@0 <= <http://example.org/name> AND <http://example.org/name> <= predicate_max@1 AND subject_null_count@5 != row_count@3 AND subject_max@4 >= <http://example.org/author00003> AND subject_null_count@5 != row_count@3 AND subject_min@6 <= <http://example.org/author05002> AND (subject_null_count@5 != row_count@3 AND subject_min@6 <= <http://example.org/author00003> AND <http://example.org/author00003> <= subject_max@4 OR subject_null_count@5 != row_count@3 AND subject_min@6 <= <http://example.org/author01003> AND <http://example.org/author01003> <= subject_max@4 OR subject_null_count@5 != row_count@3 AND subject_min@6 <= <http://example.org/author05002> AND <http://example.org/author05002> <= subject_max@4), required_guarantees=[predicate in (<http://example.org/name>), subject in (<http://example.org/author00003>, <http://example.org/author01003>, <http://example.org/author05002>)], quad_table_switch=[switched to Native(GSPO) for 3 values of subject, estimated_rows=324 instead of 20053]
        ");
    });
}

#[tokio::test]
async fn test_switch_keeps_planned_scan_for_non_selective_join() {
    let env = create_switch_test().await;
    // 100 authors: the IN list is pushed down, but 100 values times the rows per page are not
    // clearly fewer than the 20 000 rows of the planned scan. The rows per page are overestimated,
    // as the page index of the planned files is only loaded for files that are not fully matched.
    let query = format!(
        "{PREFIX} SELECT ?author ?name WHERE {{ ex:pub100 ex:authoredBy ?author . ?author ex:name ?name }}"
    );

    let result = env.run(&query, &[]).await;
    assert_eq!(result.solutions.len(), 100);
    assert_snapshot!(result.probe_scan_switch(), @"[kept: 100 values x 336 rows per page are not clearly fewer than 20053 rows]");
    // By default, a kept quad table is not shown.
    assert!(
        !result.plan_default.contains("quad_table_switch"),
        "{}",
        result.plan_default
    );
}

#[tokio::test]
async fn test_switch_keeps_planned_scan_for_min_max_filter() {
    let env = create_switch_test().await;
    // With too many distinct values, the hash join only pushes down the minimum and the maximum
    // (and a hash table lookup). This never switches.
    let query = format!(
        "{PREFIX} SELECT ?author ?name WHERE {{ ex:pub3 ex:authoredBy ?author . ?author ex:name ?name }}"
    );

    let result = env
        .run(
            &query,
            &[(
                "datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values",
                "1",
            )],
        )
        .await;
    assert_eq!(result.solutions.len(), 3);
    assert_snapshot!(result.probe_scan_switch(), @"[kept: the dynamic filter has no IN list]");
}

#[tokio::test]
async fn test_switch_scan_without_dynamic_filter_is_unchanged() {
    let env = create_switch_test().await;
    let query = format!(
        "{PREFIX} SELECT ?author ?name WHERE {{ ?author ex:name ?name }} LIMIT 5"
    );
    let result = env.run(&query, &[]).await;
    assert_eq!(result.solutions.len(), 5);
    assert!(
        !result.plan.contains("quad_table_switch"),
        "Expected no switch: {}",
        result.plan
    );

    let query = format!(
        "{PREFIX} SELECT ?author ?name WHERE {{ ex:pub3 ex:authoredBy ?author . ?author ex:name ?name }}"
    );
    let result = env
        .run(
            &query,
            &[(
                "datafusion.optimizer.enable_dynamic_filter_pushdown",
                "false",
            )],
        )
        .await;
    assert_eq!(result.solutions.len(), 3);
    assert!(
        !result.plan.contains("quad_table_switch"),
        "Expected no switch: {}",
        result.plan
    );
}

#[tokio::test]
async fn test_switch_with_multiple_partitions() {
    let env = create_switch_test().await;
    let query = format!(
        "{PREFIX} SELECT ?author ?name WHERE {{ ex:pub10 ex:authoredBy ?author . ?author ex:name ?name }}"
    );

    // With multiple partitions, the hash join is partitioned. Its dynamic filter routes each row
    // to the IN list of its partition (CASE hash(author) % 4 WHEN 0 THEN .. END). The scan looks up
    // the union of the IN lists.
    let mut options = vec![("datafusion.execution.target_partitions", "4")];
    let switched = env.run(&query, &options).await;
    options.push(DISABLE_SWITCHING);
    let planned = env.run(&query, &options).await;

    assert_eq!(switched.solutions, planned.solutions);
    assert_eq!(switched.solutions.len(), 10);
    assert!(
        switched.plan.contains("mode=Partitioned"),
        "Expected a partitioned hash join: {}",
        switched.plan
    );
    assert_snapshot!(switched.probe_scan_switch(), @"[switched to Native(GSPO) for 10 values of subject, estimated_rows=824 instead of 20053]");
    // The ten authors are spread across all four GSPO files.
    assert!(
        switched.bytes_scanned * 2 < planned.bytes_scanned,
        "Expected the switched scan to read less: {} vs. {}",
        switched.bytes_scanned,
        planned.bytes_scanned
    );
}

/// Creates a Parquet dataset with the quad tables GPOS, GSPO, and GOSP in an in-memory object
/// store.
async fn create_switch_test() -> SwitchTest {
    let context = SwitchTest::session_context();
    let store = Arc::new(InMemory::new());
    context
        .runtime_env()
        .object_store_registry
        .register_store(&Url::parse("memory:///").unwrap(), Arc::clone(&store) as _);
    store
        .put(&Path::from("input.nt"), PutPayload::from(test_ntriples()))
        .await
        .unwrap();

    let dummy_storage = Arc::new(DeltaQuadsStorageBuilder::new().build().await.unwrap());
    let loading_context = rdf_fusion_context(&context, dummy_storage, &[]);
    let output_url = Url::parse("memory:///dataset/").unwrap();
    let loader = RdfParquetLoader::try_new(
        loading_context.session_context().clone(),
        loading_context.create_view(),
        QuadStorageEncodingName::String,
        ["NATIVE(GPOS)", "NATIVE(GSPO)", "NATIVE(GOSP)"]
            .iter()
            .map(|order| order.parse::<RdfSortOrder>().unwrap())
            .collect(),
    )
    .unwrap();
    let input = RdfInput::try_new(
        Url::parse("memory:///input.nt").unwrap(),
        GraphName::DefaultGraph,
    )
    .unwrap();
    loader.load(input, output_url.clone()).await.unwrap();

    let storage = Arc::new(
        ParquetQuadStorage::builder(output_url)
            .with_encoding(QuadStorageEncodingName::String)
            .build(&context.state())
            .await
            .unwrap(),
    );
    SwitchTest::new(context, storage)
}
