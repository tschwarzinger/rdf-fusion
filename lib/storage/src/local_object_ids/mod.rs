mod claim;
mod dictionary;
mod error;
mod snapshot;
mod transaction;

pub use claim::{ObjectIdClaim, ObjectIdClaimer, StaticObjectIdClaimer};
pub use dictionary::LocalObjectIdDictionary;
pub use error::LocalObjectIdError;
use redb::TableDefinition;
pub use snapshot::LocalObjectIdDictionarySnapshot;
pub use transaction::LocalObjectIdTransaction;

type TermTuple<'a> = (i8, &'a str, Option<&'a str>, Option<&'a str>);

type OwnedTermTuple = (i8, String, Option<String>, Option<String>);

const TABLE_ID_TO_TERM: TableDefinition<i64, TermTuple<'static>> =
    TableDefinition::new("id_to_term");
const TABLE_TERM_TO_ID: TableDefinition<TermTuple<'static>, i64> =
    TableDefinition::new("term_to_id");
const TABLE_METADATA: TableDefinition<&str, &str> = TableDefinition::new("metadata");

const SYNCED_VERSION_KEY: &str = "synced_version";
const NEXT_FREE_ID_KEY: &str = "next_free_id";
const LAST_FREE_ID_KEY: &str = "last_free_id";

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Builder, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use rdf_fusion_common::DFResult;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::plain_term::{
        PlainTermArray, PlainTermArrayElementBuilder, PlainTermEncoding,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_encode_and_resolve_terms() -> DFResult<()> {
        let mapping = setup_dict().await;
        let mut txn = mapping.transaction().await?;

        let input_array = create_test_array(vec![
            ("http://example.org/Alice", None, None),
            ("b0", None, None),
            ("Hello", None, Some("en")),
            ("42", Some("http://www.w3.org/2001/XMLSchema#integer"), None),
        ]);

        let encoded_ids = txn.encode_array(&input_array).await?;
        txn.commit()?;

        let snapshot = mapping.snapshot()?;
        let resolved_array_ref = snapshot.resolve_plain_terms(&encoded_ids)?;

        assert_eq!(input_array.inner().as_ref(), resolved_array_ref.as_ref());
        Ok(())
    }

    #[tokio::test]
    async fn test_add_global_batch_non_contiguous() -> DFResult<()> {
        let dictionary = LocalObjectIdDictionary::try_new(
            None,
            1_000_000,
            Arc::new(StaticObjectIdClaimer),
        )?;
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
        txn.commit()?;

        let snapshot = dictionary.snapshot()?;
        assert_eq!(snapshot.len().unwrap(), 2);
        assert!(snapshot.read_claimed_object_ids()?.is_none()); // Doesn't change claim

        Ok(())
    }

    #[tokio::test]
    async fn test_synced_version() -> DFResult<()> {
        let mapping = setup_dict().await;
        assert_eq!(mapping.snapshot()?.get_synced_version()?, None);

        let mut txn = mapping.transaction().await?;
        txn.set_synced_version(42)?;
        txn.commit()?;

        assert_eq!(mapping.snapshot()?.get_synced_version()?, Some(42));

        let mut txn2 = mapping.transaction().await?;
        txn2.set_synced_version(100)?;
        txn2.commit()?;

        assert_eq!(mapping.snapshot()?.get_synced_version()?, Some(100));

        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_commit() {
        let dict = setup_dict().await;

        let mut txn = dict.transaction().await.unwrap();
        let array = create_test_array(vec![("test1", None, None), ("test2", None, None)]);

        let ids = txn.encode_array(&array).await.unwrap();
        assert_eq!(ids.len(), 2);

        txn.commit().unwrap();

        let snapshot = dict.snapshot().unwrap();
        let resolved = snapshot.resolve_plain_terms(&ids).unwrap();
        assert_eq!(resolved.len(), 2);
    }

    #[tokio::test]
    async fn test_transaction_abort() {
        let dict = setup_dict().await;

        let mut txn = dict.transaction().await.unwrap();
        let array = create_test_array(vec![("test1", None, None)]);
        let ids = txn.encode_array(&array).await.unwrap();

        txn.abort().unwrap();

        let snapshot = dict.snapshot().unwrap();
        assert!(snapshot.resolve_plain_terms(&ids).is_err());
        assert_eq!(
            snapshot.read_claimed_object_ids().unwrap().unwrap(),
            (0, 9223372036854775807) // New claims are also stored on aborts.
        );
    }

    async fn setup_dict() -> LocalObjectIdDictionary {
        LocalObjectIdDictionary::try_new(None, 1_000_000, Arc::new(StaticObjectIdClaimer))
            .unwrap()
    }

    fn create_test_array(
        terms: Vec<(&str, Option<&str>, Option<&str>)>,
    ) -> PlainTermArray {
        let mut builder = PlainTermArrayElementBuilder::new();
        for (value, datatype, lang) in terms {
            builder.append_raw(1, value, datatype, lang);
        }
        let array_ref = builder.finish().into_array_ref();
        PlainTermArray::try_from(array_ref).unwrap()
    }
}
