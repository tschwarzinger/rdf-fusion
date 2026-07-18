use anyhow::Result;
use datafusion::arrow::array::{Array, Int64Builder, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use rdf_fusion::encoding::EncodingArray;
use rdf_fusion::encoding::plain_term::{
    PlainTermArray, PlainTermArrayElementBuilder, PlainTermEncoding,
};
use rdf_fusion::storage::local_object_ids::LocalObjectIdDictionary;
use std::sync::Arc;

pub async fn encode_and_resolve_terms(
    mapping: Arc<dyn LocalObjectIdDictionary>,
) -> Result<()> {
    let mut txn = mapping.transaction().await?;

    let input_array = create_test_array(vec![
        ("http://example.org/Alice", None, None),
        ("b0", None, None),
        ("Hello", None, Some("en")),
        ("42", Some("http://www.w3.org/2001/XMLSchema#integer"), None),
    ]);

    let encoded_ids = txn.encode_array(&input_array).await?;
    txn.commit(0).await?;

    let snapshot = mapping.snapshot().await?;
    let resolved_array_ref = snapshot.resolve_plain_terms(&encoded_ids)?;

    assert_eq!(input_array.inner().as_ref(), resolved_array_ref.as_ref());
    Ok(())
}

pub async fn add_global_batch_non_contiguous(
    dictionary: Arc<dyn LocalObjectIdDictionary>,
) -> Result<()> {
    let mut txn = dictionary.transaction().await?;

    let mut id_builder = Int64Builder::new();
    id_builder.append_value(1);
    id_builder.append_value(3);
    let id_array = Arc::new(id_builder.finish()) as Arc<dyn Array>;

    let mut term_builder = PlainTermArrayElementBuilder::new();
    term_builder.append_raw(1, "A", None, None);
    term_builder.append_raw(1, "B", None, None);
    let term_array = term_builder.finish().into_array_ref();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("term", PlainTermEncoding::data_type().clone(), true),
    ]));

    let batch = RecordBatch::try_new(schema, vec![id_array, term_array])?;

    txn.add_global_batch(&batch).await?;
    txn.commit(0).await?;

    let snapshot = dictionary.snapshot().await?;
    assert_eq!(snapshot.len().unwrap(), 2);
    assert!(snapshot.read_claimed_object_ids()?.is_none());

    Ok(())
}

pub async fn synced_version(mapping: Arc<dyn LocalObjectIdDictionary>) -> Result<()> {
    assert_eq!(mapping.snapshot().await?.get_synced_version()?, None);

    let txn = mapping.transaction().await?;
    txn.commit(42).await?;

    assert_eq!(mapping.snapshot().await?.get_synced_version()?, Some(42));

    let txn2 = mapping.transaction().await?;
    txn2.commit(100).await?;

    assert_eq!(mapping.snapshot().await?.get_synced_version()?, Some(100));

    Ok(())
}

pub async fn transaction_commit(dict: Arc<dyn LocalObjectIdDictionary>) -> Result<()> {
    let mut txn = dict.transaction().await?;
    let array = create_test_array(vec![("test1", None, None), ("test2", None, None)]);

    let ids = txn.encode_array(&array).await?;
    assert_eq!(ids.len(), 2);

    txn.commit(0).await?;

    let snapshot = dict.snapshot().await?;
    let resolved = snapshot.resolve_plain_terms(&ids)?;
    assert_eq!(resolved.len(), 2);
    Ok(())
}

pub async fn transaction_abort(dict: Arc<dyn LocalObjectIdDictionary>) -> Result<()> {
    let mut txn = dict.transaction().await?;
    let array = create_test_array(vec![("test1", None, None)]);
    let ids = txn.encode_array(&array).await?;

    txn.abort().await?;

    let snapshot = dict.snapshot().await?;
    assert!(snapshot.resolve_plain_terms(&ids).is_err());
    assert_eq!(
        snapshot.read_claimed_object_ids()?.unwrap(),
        (0, 9223372036854775807)
    );
    Ok(())
}

fn create_test_array(terms: Vec<(&str, Option<&str>, Option<&str>)>) -> PlainTermArray {
    let mut builder = PlainTermArrayElementBuilder::new();
    for (value, datatype, lang) in terms {
        builder.append_raw(1, value, datatype, lang);
    }
    let array_ref = builder.finish().into_array_ref();
    PlainTermArray::try_from(array_ref).unwrap()
}
