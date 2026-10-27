//! Tests for switching a quad pattern scan of a Delta storage to another quad table at execution
//! time.

use crate::storage::delta::create_test_log_store;
use crate::storage::switch::{DISABLE_SWITCHING, PREFIX, SwitchTest, quad, test_quads};
use rdf_fusion_common::{GraphName, Literal, NamedNode, Quad};
use rdf_fusion_encoding::{QuadStorageEncodingName, quads_to_plain_term_dataframe};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_storage::delta::{DeltaQuadsStorage, DeltaQuadsStorageBuilder};
use std::sync::Arc;
use url::Url;

#[tokio::test]
async fn test_switch_selective_join_switches_probe_scan() {
    let (env, _) = create_switch_test().await;
    let query = format!(
        "{PREFIX} SELECT ?author ?name WHERE {{ ex:pub3 ex:authoredBy ?author . ?author ex:name ?name }}"
    );

    let switched = env.run(&query, &[]).await;
    let planned = env.run(&query, &[DISABLE_SWITCHING]).await;

    assert_eq!(switched.solutions, planned.solutions);
    assert_eq!(switched.solutions.len(), 3);
    let decision = switched.probe_scan_switch();
    assert!(
        decision.starts_with("[switched to Native(GSPO) for 3 values of subject"),
        "Expected a switch: {}",
        switched.plan
    );
    // The scanned bytes are not compared, as the Delta storage caches the data across queries.
    assert!(
        switched.rows_decoded * 4 < planned.rows_decoded,
        "Expected the switched scan to decode much fewer rows: {} vs. {}",
        switched.rows_decoded,
        planned.rows_decoded
    );
}

#[tokio::test]
async fn test_switch_with_changeset_returns_same_solutions() {
    let (env, storage) = create_switch_test().await;
    // The quad tables are not updated with these changes. Hence, the changeset is applied on top
    // of the scan of the quad table.
    let author = "http://example.org/author99999";
    insert(
        &env,
        &storage,
        &[
            quad(
                "http://example.org/pub3",
                "http://example.org/authoredBy",
                author,
            ),
            Quad::new(
                NamedNode::new_unchecked(author),
                NamedNode::new_unchecked("http://example.org/name"),
                Literal::new_simple_literal("New Author"),
                GraphName::DefaultGraph,
            ),
        ],
    )
    .await;
    let query = format!(
        "{PREFIX} SELECT ?author ?name WHERE {{ ex:pub3 ex:authoredBy ?author . ?author ex:name ?name }}"
    );

    let switched = env.run(&query, &[]).await;
    let planned = env.run(&query, &[DISABLE_SWITCHING]).await;

    assert_eq!(switched.solutions, planned.solutions);
    assert_eq!(switched.solutions.len(), 4);
    assert!(
        switched
            .solutions
            .iter()
            .any(|solution| solution.contains("New Author")),
        "{:?}",
        switched.solutions
    );
}

/// Creates a Delta storage with the quad tables GPOS, GSPO, and GOSP. The quad tables are up to
/// date with the log.
async fn create_switch_test() -> (SwitchTest, Arc<DeltaQuadsStorage>) {
    let context = SwitchTest::session_context();
    let log_store = create_test_log_store();
    context.runtime_env().register_object_store(
        &Url::parse("memory://").unwrap(),
        log_store.root_object_store(None),
    );
    let storage = Arc::new(
        DeltaQuadsStorageBuilder::new()
            .with_log_store(log_store)
            .with_encoding(QuadStorageEncodingName::String)
            .build()
            .await
            .unwrap(),
    );
    let test = SwitchTest::new(context, Arc::clone(&storage) as _);
    insert(&test, &storage, &test_quads()).await;
    storage
        .optimize(&test.writing_session().state())
        .await
        .unwrap();
    (test, storage)
}

/// Inserts the `quads` into the Delta `storage`, without updating the quad tables.
async fn insert(test: &SwitchTest, storage: &DeltaQuadsStorage, quads: &[Quad]) {
    let session = test.writing_session();
    let df = quads_to_plain_term_dataframe(&session, quads);
    let transaction = storage.begin_transaction(&session.state()).await.unwrap();
    transaction.insert(df).await.unwrap();
    transaction.commit().await.unwrap();
}
