mod dictionary;
mod error;

pub use dictionary::LocalObjectIdDictionary;
pub use error::LocalObjectIdError;

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::TermEncoding;
    use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
    use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermArrayElementBuilder};
    use std::sync::Arc;

    type DFResult<T> = Result<T, Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn test_encode_and_resolve_terms() -> DFResult<()> {
        let mapping = LocalObjectIdDictionary::try_new_in_memory()?;

        let input_array = create_test_array();

        let encoded_ids = mapping.encode_array(&input_array).await?;
        let resolved_array_ref = mapping.resolve_plain_terms(&encoded_ids).await?;

        assert_eq!(input_array.inner().as_ref(), resolved_array_ref.as_ref());
        Ok(())
    }

    #[tokio::test]
    async fn test_add_batch_non_contiguous() -> DFResult<()> {
        let mapping = LocalObjectIdDictionary::try_new_in_memory()?;

        let id_array = Arc::new(Int64Array::from(vec![0, 5]));

        let mut builder = PlainTermArrayElementBuilder::new();
        builder.append_raw(1, "http://example.org/A", None, None);
        builder.append_raw(1, "http://example.org/B", None, None);
        let term_array = builder.finish().into_array_ref();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("term", PLAIN_TERM_ENCODING.data_type().clone(), true),
        ]));

        let batch = RecordBatch::try_new(schema, vec![id_array, term_array])?;

        mapping.add_batch(&batch).await?;
        assert_eq!(mapping.next_id(), 6);

        Ok(())
    }

    #[tokio::test]
    async fn test_synced_version() -> DFResult<()> {
        let mapping = LocalObjectIdDictionary::try_new_in_memory()?;
        assert_eq!(mapping.get_synced_version().await?, None);

        mapping.set_synced_version(42).await?;
        assert_eq!(mapping.get_synced_version().await?, Some(42));

        mapping.set_synced_version(100).await?;
        assert_eq!(mapping.get_synced_version().await?, Some(100));

        Ok(())
    }

    fn create_test_array() -> PlainTermArray {
        let mut builder = PlainTermArrayElementBuilder::new();
        builder.append_raw(1, "http://example.org/Alice", None, None);
        builder.append_raw(2, "b0", None, None);
        builder.append_raw(3, "Hello", None, Some("en"));
        builder.append_raw(
            3,
            "42",
            Some("http://www.w3.org/2001/XMLSchema#integer"),
            None,
        );
        builder.finish()
    }
}
