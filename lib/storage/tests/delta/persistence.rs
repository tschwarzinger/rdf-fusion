use crate::delta::{create_context, create_test_log_store, populate_storage};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use deltalake::logstore::{StorageConfig, logstore_with};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use rdf_fusion_common::NamedNodeRef;
use rdf_fusion_common::config::RdfFusionOptions;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_encoding::plain_term::{PlainTermArrayElementBuilder, PlainTermEncoding};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_storage::delta::DeltaQuadsStorage;
use rdf_fusion_storage::delta::DeltaQuadsStorageBuilder;
use rdf_fusion_storage::index::IndexComponents;
use std::sync::Arc;
use url::Url;

#[tokio::test]
async fn test_reload_storage_object_id() {
    let log_store = create_test_log_store();

    // 1. Create and populate storage
    {
        let storage = Arc::new(
            DeltaQuadsStorageBuilder::new()
                .with_log_store(Arc::clone(&log_store))
                .with_encoding(QuadStorageEncodingName::ObjectId)
                .build()
                .await
                .unwrap(),
        );

        populate_storage(Arc::clone(&storage), "http://example.org/s1").await;

        storage
            .delta_object_id_mapping()
            .unwrap()
            .flush()
            .await
            .unwrap();
    }

    // 2. Reload and verify
    {
        let ctx = SessionContext::new();
        let storage = DeltaQuadsStorage::try_load(
            &ctx.state(),
            &RdfFusionOptions::default(),
            Arc::clone(&log_store),
        )
        .await
        .unwrap();

        assert!(storage.delta_object_id_mapping().is_some());
        assert_eq!(storage.log().version().await, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_reload_storage_plain_term() {
    let log_store = create_test_log_store();
    let session = SessionStateBuilder::new().build();

    // 1. Create and populate storage
    {
        let storage = Arc::new(
            DeltaQuadsStorageBuilder::new()
                .with_log_store(Arc::clone(&log_store))
                .with_encoding(QuadStorageEncodingName::PlainTerm)
                .build()
                .await
                .unwrap(),
        );

        populate_storage(storage, "http://example.org/s1").await;
    }

    // 2. Reload and verify
    {
        let storage = DeltaQuadsStorage::try_load(
            &session,
            &RdfFusionOptions::default(),
            Arc::clone(&log_store),
        )
        .await
        .unwrap();

        assert_eq!(storage.log().version().await, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_reload_storage_with_index_and_optimize() {
    let log_store = create_test_log_store();
    let session = SessionStateBuilder::new().build();

    // 1. Create storage with indexes
    {
        let storage = Arc::new(
            DeltaQuadsStorageBuilder::new()
                .with_log_store(Arc::clone(&log_store))
                .with_encoding(QuadStorageEncodingName::PlainTerm)
                .with_indexes(vec![IndexComponents::GSPO])
                .build()
                .await
                .unwrap(),
        );

        populate_storage(Arc::clone(&storage), "http://example.org/s1").await;

        let ctx = create_context(
            Arc::clone(&storage) as Arc<dyn QuadStorage>,
            Arc::clone(&log_store),
        );
        storage.optimize(&ctx.state()).await.unwrap();
    }

    // 2. Reload, add more data, and optimize again
    {
        let storage = Arc::new(
            DeltaQuadsStorage::try_load(
                &session,
                &RdfFusionOptions::default(),
                Arc::clone(&log_store),
            )
            .await
            .unwrap(),
        );

        populate_storage(Arc::clone(&storage), "http://example.org/s2").await;

        let ctx = create_context(
            Arc::clone(&storage) as Arc<dyn QuadStorage>,
            Arc::clone(&log_store),
        );
        storage.optimize(&ctx.state()).await.unwrap();
        assert_eq!(storage.log().version().await, 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_load_storage_object_id() {
    let log_store = create_test_log_store();
    let session = SessionStateBuilder::new().build();

    // 1. Create and populate storage
    {
        let storage = Arc::new(
            DeltaQuadsStorageBuilder::new()
                .with_log_store(Arc::clone(&log_store))
                .with_encoding(QuadStorageEncodingName::ObjectId)
                .build()
                .await
                .unwrap(),
        );

        populate_storage(Arc::clone(&storage), "http://example.org/s1").await;

        storage
            .delta_object_id_mapping()
            .unwrap()
            .flush()
            .await
            .unwrap();
    }

    // 2. Reload and verify
    {
        let storage = DeltaQuadsStorage::try_load(
            &session,
            &RdfFusionOptions::default(),
            Arc::clone(&log_store),
        )
        .await
        .unwrap();

        assert!(storage.delta_object_id_mapping().is_some());
        assert_eq!(storage.log().version().await, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::disallowed_methods)]
async fn test_concurrent_dictionary_inserts() {
    let object_store = Arc::new(InMemory::new());
    let base_url = Url::parse("memory:///").unwrap();

    // 1. Initialize first instance to create the delta tables
    let log_store_1 = logstore_with(
        Arc::clone(&object_store) as Arc<dyn ObjectStore>,
        &base_url,
        StorageConfig::default(),
    )
    .unwrap();
    let storage_1 = Arc::new(
        DeltaQuadsStorageBuilder::new()
            .with_log_store(log_store_1)
            .with_encoding(QuadStorageEncodingName::ObjectId)
            .build()
            .await
            .unwrap(),
    );
    // Drop to ensure no lingering locks
    drop(storage_1);

    // 2. Now spawn 10 concurrent tasks that each have their own separated LocalObjectIdDictionary
    let num_tasks = 10;
    let mut handles = Vec::new();

    for task_id in 0..num_tasks {
        let os_clone = Arc::clone(&object_store);
        let url_clone = base_url.clone();

        handles.push(tokio::spawn(async move {
            let log_store = logstore_with(
                os_clone as Arc<dyn ObjectStore>,
                &url_clone,
                StorageConfig::default(),
            )
            .unwrap();
            let ctx = SessionContext::new();
            let storage = Arc::new(
                DeltaQuadsStorage::try_load(
                    &ctx.state(),
                    &RdfFusionOptions::default(),
                    Arc::clone(&log_store),
                )
                .await
                .unwrap(),
            );

            for batch_idx in 0..5 {
                let context = create_context(
                    Arc::clone(&storage) as Arc<dyn QuadStorage>,
                    Arc::clone(&log_store),
                );
                let transaction =
                    storage.begin_transaction(&context.state()).await.unwrap();
                let df = create_bulk_quads(&context, task_id, batch_idx);
                transaction.insert(df).await.unwrap();
                transaction.commit().await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // 3. Verify the results
    let log_store = logstore_with(
        Arc::clone(&object_store) as Arc<dyn ObjectStore>,
        &base_url,
        StorageConfig::default(),
    )
    .unwrap();
    let ctx = SessionContext::new();
    let storage = DeltaQuadsStorage::try_load(
        &ctx.state(),
        &RdfFusionOptions::default(),
        log_store,
    )
    .await
    .unwrap();

    let mapping = storage.delta_object_id_mapping().unwrap();
    mapping.flush().await.unwrap();

    let local_dict = mapping.dictionary().snapshot().unwrap();

    // 10 overlapping_subj, 1 predicate, 500 unique objects (10 tasks * 5 batches * 10 objects)
    let expected_unique_terms = 10 + 1 + 500;

    assert_eq!(local_dict.len().unwrap(), expected_unique_terms as u64);
    assert_eq!(local_dict.read_claimed_object_ids().unwrap(), None);
}

fn create_bulk_quads(
    ctx: &SessionContext,
    task_id: usize,
    batch_idx: usize,
) -> datafusion::dataframe::DataFrame {
    let data_type = PlainTermEncoding::data_type().clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new(COL_GRAPH, data_type.clone(), true),
        Field::new(COL_SUBJECT, data_type.clone(), true),
        Field::new(COL_PREDICATE, data_type.clone(), true),
        Field::new(COL_OBJECT, data_type, true),
    ]));
    let mut graph_builder = PlainTermArrayElementBuilder::new();
    let mut subject_builder = PlainTermArrayElementBuilder::new();
    let mut predicate_builder = PlainTermArrayElementBuilder::new();
    let mut object_builder = PlainTermArrayElementBuilder::new();

    for i in 0..10 {
        graph_builder.append_null();
        let overlapping_subj = format!("http://example.org/overlapping_subj_{i}");
        let unique_obj =
            format!("http://example.org/task_{task_id}_batch_{batch_idx}_obj_{i}");
        subject_builder.append_named_node(NamedNodeRef::new_unchecked(&overlapping_subj));
        predicate_builder
            .append_named_node(NamedNodeRef::new_unchecked("http://example.org/p"));
        object_builder.append_named_node(NamedNodeRef::new_unchecked(&unique_obj));
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(graph_builder.finish().into_array_ref()),
            Arc::new(subject_builder.finish().into_array_ref()),
            Arc::new(predicate_builder.finish().into_array_ref()),
            Arc::new(object_builder.finish().into_array_ref()),
        ],
    )
    .unwrap();
    ctx.read_batch(batch).unwrap()
}
