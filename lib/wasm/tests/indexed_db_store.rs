#![cfg(target_arch = "wasm32")]

use bytes::Bytes;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use rdf_fusion_wasm::indexeddb_store::IndexedDbObjectStore;
use wasm_bindgen_test::*;

// This tells the macro to run tests in the browser environment
wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn test_indexeddb_object_store_put_and_get() {
    let db_name = "test_db_put_get";
    let store = IndexedDbObjectStore::new(db_name.to_string());

    let path = Path::from("test_folder/test_file.txt");
    let content = Bytes::from("hello webassembly");

    let payload = PutPayload::from_bytes(content.clone());
    let put_res = store.put(&path, payload).await;
    assert!(put_res.is_ok(), "Failed to put object: {:?}", put_res.err());

    let get_res = store.get(&path).await.expect("Failed to get object");
    let meta_size = get_res.meta.size;

    let stream_bytes = get_res
        .into_stream()
        .map(|r| r.unwrap())
        .fold(Vec::new(), |mut acc, bytes| async move {
            acc.extend_from_slice(&bytes);
            acc
        })
        .await;

    assert_eq!(
        Bytes::from(stream_bytes),
        content,
        "Retrieved content does not match"
    );
    assert_eq!(meta_size, content.len() as u64, "Size metadata mismatch");
}

#[wasm_bindgen_test]
async fn test_indexeddb_object_store_range_reads() {
    let db_name = "test_db_ranges";
    let store = IndexedDbObjectStore::new(db_name.to_string());
    let path = Path::from("alphabet.txt");

    let content = Bytes::from("abcdefghijklmnopqrstuvwxyz");
    store
        .put(&path, PutPayload::from_bytes(content))
        .await
        .unwrap();

    let range_bytes = store
        .get_range(&path, 5..10)
        .await
        .expect("Failed range read");
    assert_eq!(range_bytes, Bytes::from("fghij"));
}

#[wasm_bindgen_test]
async fn test_indexeddb_object_store_head() {
    let db_name = "test_db_head";
    let store = IndexedDbObjectStore::new(db_name.to_string());
    let path = Path::from("metadata_test.bin");
    let content = Bytes::from("1234567890");

    store
        .put(&path, PutPayload::from_bytes(content.clone()))
        .await
        .unwrap();

    let meta = store.head(&path).await.expect("Failed to head object");
    assert_eq!(meta.size, content.len() as u64);
    assert_eq!(meta.location, path);
}

#[wasm_bindgen_test]
async fn test_indexeddb_object_store_not_found() {
    let db_name = "test_db_not_found";
    let store = IndexedDbObjectStore::new(db_name.to_string());
    let path = Path::from("non_existent_file.txt");

    let get_res = store.get(&path).await;
    assert!(
        matches!(get_res, Err(object_store::Error::NotFound { .. })),
        "Expected NotFound error, got {:?}",
        get_res
    );
}

#[wasm_bindgen_test]
async fn test_indexeddb_object_store_list() {
    let db_name = "test_db_list";
    let store = IndexedDbObjectStore::new(db_name.to_string());

    let file1 = Path::from("data/file1.txt");
    let file2 = Path::from("data/file2.txt");
    let file3 = Path::from("other/file3.txt");

    store
        .put(&file1, PutPayload::from_bytes(Bytes::from("1")))
        .await
        .unwrap();
    store
        .put(&file2, PutPayload::from_bytes(Bytes::from("22")))
        .await
        .unwrap();
    store
        .put(&file3, PutPayload::from_bytes(Bytes::from("333")))
        .await
        .unwrap();

    // List all
    let all_entries: Vec<_> = store.list(None).collect().await;
    let all_paths: Vec<Path> = all_entries
        .into_iter()
        .map(|r| r.unwrap().location)
        .collect();
    assert!(all_paths.contains(&file1));
    assert!(all_paths.contains(&file2));
    assert!(all_paths.contains(&file3));

    // List with prefix "data"
    let prefix = Path::from("data");
    let data_entries: Vec<_> = store.list(Some(&prefix)).collect().await;
    let data_paths: Vec<Path> = data_entries
        .into_iter()
        .map(|r| r.unwrap().location)
        .collect();
    assert_eq!(data_paths.len(), 2);
    assert!(data_paths.contains(&file1));
    assert!(data_paths.contains(&file2));
    assert!(!data_paths.contains(&file3));
}

#[wasm_bindgen_test]
async fn test_indexeddb_object_store_list_with_offset() {
    let db_name = "test_db_list_offset";
    let store = IndexedDbObjectStore::new(db_name.to_string());

    let file_a = Path::from("a.txt");
    let file_b = Path::from("b.txt");
    let file_c = Path::from("c.txt");

    store
        .put(&file_a, PutPayload::from_bytes(Bytes::from("a")))
        .await
        .unwrap();
    store
        .put(&file_b, PutPayload::from_bytes(Bytes::from("b")))
        .await
        .unwrap();
    store
        .put(&file_c, PutPayload::from_bytes(Bytes::from("c")))
        .await
        .unwrap();

    let offset = Path::from("a.txt");
    let offset_entries: Vec<_> = store.list_with_offset(None, &offset).collect().await;
    let offset_paths: Vec<Path> = offset_entries
        .into_iter()
        .map(|r| r.unwrap().location)
        .collect();
    assert_eq!(offset_paths.len(), 2);
    assert!(!offset_paths.contains(&file_a));
    assert!(offset_paths.contains(&file_b));
    assert!(offset_paths.contains(&file_c));
}
