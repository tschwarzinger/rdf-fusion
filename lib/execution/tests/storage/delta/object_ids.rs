use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermArrayElementBuilder};
use rdf_fusion_storage::local_object_ids::{
    LocalObjectIdDictionary, RedbObjectIdDictionaryBuilder, StaticObjectIdClaimer,
};
use std::sync::Arc;

#[tokio::test]
async fn test_transaction_without_claim_abort_persists_claim() {
    let dictionary = RedbObjectIdDictionaryBuilder::new_in_memory()
        .with_claimer(Some(Arc::new(StaticObjectIdClaimer)))
        .finish()
        .unwrap();
    let mut txn = dictionary.transaction().await.unwrap();

    let mut builder = PlainTermArrayElementBuilder::new();
    builder.append_raw(1, "http://example.org/A", None, None);
    let array = PlainTermArray::try_from(builder.finish().into_array_ref()).unwrap();

    txn.encode_array(&array).await.unwrap();
    txn.abort().await.unwrap();

    // Rollback should still store the claimed id space
    assert_eq!(
        dictionary
            .snapshot()
            .await
            .unwrap()
            .read_claimed_object_ids()
            .await
            .unwrap()
            .unwrap(),
        (0, 9223372036854775807)
    );
}

#[tokio::test]
async fn test_transaction_with_claim_does_not_decrease_claim() {
    let dictionary = RedbObjectIdDictionaryBuilder::new_in_memory()
        .with_claimer(Some(Arc::new(StaticObjectIdClaimer)))
        .finish()
        .unwrap();

    let mut txn = dictionary.transaction().await.unwrap();
    let mut builder = PlainTermArrayElementBuilder::new();
    builder.append_raw(1, "http://example.org/A", None, None);
    let array = PlainTermArray::try_from(builder.finish().into_array_ref()).unwrap();
    txn.encode_array(&array).await.unwrap();
    txn.commit(0).await.unwrap();

    let mut txn = dictionary.transaction().await.unwrap();
    let mut builder = PlainTermArrayElementBuilder::new();
    builder.append_raw(1, "http://example.org/B", None, None);
    let array = PlainTermArray::try_from(builder.finish().into_array_ref()).unwrap();
    txn.encode_array(&array).await.unwrap();
    txn.abort().await.unwrap();

    assert_eq!(
        dictionary
            .snapshot()
            .await
            .unwrap()
            .read_claimed_object_ids()
            .await
            .unwrap()
            .unwrap()
            .0,
        1 // Only a single id should have been used
    );
}
