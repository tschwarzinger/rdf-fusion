mod encoding;
mod mapping;
mod mapping_in_memory;

pub use encoding::*;
pub use mapping::*;

#[cfg(test)]
mod tests {
    use crate::delta::objectids::DeltaObjectIdMapping;
    use datafusion::arrow::util::pretty::pretty_format_columns;
    use deltalake::logstore::{IORuntime, StorageConfig, logstore_with};
    use insta::assert_snapshot;
    use object_store::memory::InMemory;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::object_id::ObjectIdMapping;
    use rdf_fusion_encoding::plain_term::PlainTermArrayElementBuilder;
    use rdf_fusion_model::NamedNodeRef;
    use std::sync::Arc;
    use tokio::runtime::Handle;
    use url::Url;

    #[tokio::test]
    async fn test_encode_decode_roundtrip() {
        let mapping: DeltaObjectIdMapping = setup_mapping().await;

        let mut builder = PlainTermArrayElementBuilder::new();
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.org/a"));
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.org/b"));
        builder.append_null();
        let original_array = builder.finish();

        let encoded = mapping.encode_array(&original_array).unwrap();
        let decoded = mapping.decode_array(&encoded).unwrap();
        assert_eq!(decoded.inner().len(), 3);

        // Compare iterators
        for (o, d) in original_array.iter().zip(decoded.iter()) {
            let ot = o.as_term();
            let dt = d.as_term();
            assert_eq!(ot, dt);
        }
    }

    #[tokio::test]
    async fn test_encode_handles_null() {
        let mapping: DeltaObjectIdMapping = setup_mapping().await;

        let mut builder = PlainTermArrayElementBuilder::new();
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.org/a"));
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.org/b"));
        builder.append_null();
        let original_array = builder.finish();

        let encoded = mapping.encode_array(&original_array).unwrap();
        assert_eq!(encoded.len(), 3);
        assert!(encoded.is_null(2));
    }

    #[tokio::test]
    async fn test_encode_chooses_sensible_ids() {
        let mapping: DeltaObjectIdMapping = setup_mapping().await;

        let mut builder = PlainTermArrayElementBuilder::new();
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.org/a"));
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.org/b"));
        let original_array = builder.finish();

        let encoded = mapping.encode_array(&original_array).unwrap();
        assert_snapshot!(
            pretty_format_columns("encoded", &[encoded]).unwrap(),
            @r"
    +---------+
    | encoded |
    +---------+
    | 0       |
    | 1       |
    +---------+
    "
        )
    }

    #[tokio::test]
    async fn test_encode_reuses_ids() {
        let mapping: DeltaObjectIdMapping = setup_mapping().await;

        let mut builder = PlainTermArrayElementBuilder::new();
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.org/a"));
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.org/b"));
        let original_array = builder.finish();

        let encoded1 = mapping.encode_array(&original_array).unwrap();
        let encoded2 = mapping.encode_array(&original_array).unwrap();
        assert_eq!(encoded1.as_ref(), encoded2.as_ref());
    }

    async fn setup_mapping() -> DeltaObjectIdMapping {
        let memory_store = Arc::new(InMemory::new());
        let url = Url::parse("memory://").unwrap();
        let log_store = logstore_with(
            memory_store,
            &url,
            StorageConfig::default().with_io_runtime(IORuntime::RT(Handle::current())),
        )
        .unwrap();
        DeltaObjectIdMapping::try_new_at_location(log_store)
            .await
            .unwrap()
    }
}
