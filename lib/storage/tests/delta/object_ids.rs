use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermArrayElementBuilder};
use rdf_fusion_storage::local_object_ids::{
    LocalObjectIdDictionary, StaticObjectIdClaimer,
};
use std::sync::Arc;

#[tokio::test]
async fn test_transaction_without_claim_abort_persists_claim() {
    let dictionary = LocalObjectIdDictionary::try_new(
        None,
        1_000_000,
        Arc::new(StaticObjectIdClaimer),
    )
    .unwrap();
    let mut txn = dictionary.transaction().await.unwrap();

    let mut builder = PlainTermArrayElementBuilder::new();
    builder.append_raw(1, "http://example.org/A", None, None);
    let array = PlainTermArray::try_from(builder.finish().into_array_ref()).unwrap();

    txn.encode_array(&array).await.unwrap();
    txn.abort().unwrap();

    // Rollback should still store the claimed id space
    assert_eq!(
        dictionary
            .snapshot()
            .unwrap()
            .read_claimed_object_ids()
            .unwrap()
            .unwrap(),
        (0, 9223372036854775807)
    );
}

#[tokio::test]
async fn test_transaction_with_claim_does_not_decrease_claim() {
    let dictionary = LocalObjectIdDictionary::try_new(
        None,
        1_000_000,
        Arc::new(StaticObjectIdClaimer),
    )
    .unwrap();

    let mut txn = dictionary.transaction().await.unwrap();
    let mut builder = PlainTermArrayElementBuilder::new();
    builder.append_raw(1, "http://example.org/A", None, None);
    let array = PlainTermArray::try_from(builder.finish().into_array_ref()).unwrap();
    txn.encode_array(&array).await.unwrap();
    txn.commit(0).unwrap();

    let mut txn = dictionary.transaction().await.unwrap();
    let mut builder = PlainTermArrayElementBuilder::new();
    builder.append_raw(1, "http://example.org/B", None, None);
    let array = PlainTermArray::try_from(builder.finish().into_array_ref()).unwrap();
    txn.encode_array(&array).await.unwrap();
    txn.abort().unwrap();

    assert_eq!(
        dictionary
            .snapshot()
            .unwrap()
            .read_claimed_object_ids()
            .unwrap()
            .unwrap()
            .0,
        1 // Only a single id should have been used
    );
}
