use crate::parquet::setup_test_store;
use bytes::Bytes;
use datafusion::parquet::file::reader::FileReader;
use datafusion::parquet::file::serialized_reader::SerializedFileReader;
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use object_store::ObjectStoreExt;
use prettytable::{Table, row};
use rdf_fusion::common::{QuadComponent, RdfDumpFormat, RdfSortOrder};
use rdf_fusion::store::{DumpEncoding, RdfDumpOptions, Store};
use url::Url;

struct ParquetSizeMetrics {
    total_file_size: i64,
    footer_size: i64,
    total_data_size: i64,
    page_index_size: i64,
    bloom_filter_size: i64,
}

#[tokio::test]
async fn test_parquet_file_and_bloom_filter_size() {
    let store = setup_test_store().await;

    let sort_orders = vec![
        RdfSortOrder::NativeOrder(vec![
            QuadComponent::Subject,
            QuadComponent::Predicate,
            QuadComponent::Object,
        ]),
        RdfSortOrder::NativeOrder(vec![
            QuadComponent::Predicate,
            QuadComponent::Object,
            QuadComponent::Subject,
        ]),
        RdfSortOrder::NativeOrder(vec![
            QuadComponent::Object,
            QuadComponent::Subject,
            QuadComponent::Predicate,
        ]),
    ];

    let mut table = Table::new();
    table.add_row(row![
        "Sort Order",
        "File Size (Bytes)",
        "Footer Size (Bytes)",
        "Total Data Size (Bytes)",
        "Page Index Size (Bytes)",
        "Bloom Filter Size (Bytes)"
    ]);

    for (i, order) in sort_orders.into_iter().enumerate() {
        let test_url = format!("memory:///test_{i}.parquet");
        store
            .dump(
                test_url.clone(),
                RdfDumpFormat::Parquet,
                RdfDumpOptions::default()
                    .with_encoding(DumpEncoding::String)
                    .with_sort_by(Some(order.clone())),
            )
            .await
            .unwrap();

        let bytes = get_dumped_bytes(&store, &test_url).await;
        let metrics = compute_parquet_size_metrics(&bytes);

        table.add_row(row![
            format!("{:?}", order),
            metrics.total_file_size,
            metrics.footer_size,
            metrics.total_data_size,
            metrics.page_index_size,
            metrics.bloom_filter_size
        ]);
    }

    let table_str = table.to_string();
    println!("\nParquet File Size DataFrame:\n{}", table_str);

    insta::assert_snapshot!(table_str, @"
    +-------------------------------------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | Sort Order                                | File Size (Bytes) | Footer Size (Bytes) | Total Data Size (Bytes) | Page Index Size (Bytes) | Bloom Filter Size (Bytes) |
    +-------------------------------------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | NativeOrder([Subject, Predicate, Object]) | 10166029          | 10330               | 9686095                 | 174068                  | 295524                    |
    +-------------------------------------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | NativeOrder([Predicate, Object, Subject]) | 10182572          | 11341               | 9684650                 | 191045                  | 295524                    |
    +-------------------------------------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | NativeOrder([Object, Subject, Predicate]) | 10444310          | 10919               | 9947424                 | 190431                  | 295524                    |
    +-------------------------------------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    ");
}

async fn get_dumped_bytes(store: &Store, url_str: &str) -> Bytes {
    let url = Url::parse(url_str).unwrap();
    let object_store = store
        .context()
        .session_context()
        .runtime_env()
        .object_store(&url.as_object_store_url())
        .unwrap();

    let path = object_store::path::Path::from(url.path());

    object_store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
}

fn compute_parquet_size_metrics(bytes: &Bytes) -> ParquetSizeMetrics {
    let reader: SerializedFileReader<Bytes> =
        SerializedFileReader::new(bytes.clone()).unwrap();
    let metadata = reader.metadata();

    let total_file_size = bytes.len() as i64;

    let footer_size =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
            as i64;

    let mut total_data_size = 0;
    let mut page_index_size = 0;
    let mut bloom_filter_size = 0;

    for rg in metadata.row_groups() {
        for col in rg.columns() {
            total_data_size += col.compressed_size();

            if let Some(len) = col.column_index_length() {
                page_index_size += len as i64;
            }
            if let Some(len) = col.offset_index_length() {
                page_index_size += len as i64;
            }
            if let Some(len) = col.bloom_filter_length() {
                bloom_filter_size += len as i64;
            }
        }
    }

    ParquetSizeMetrics {
        total_file_size,
        footer_size,
        total_data_size,
        page_index_size,
        bloom_filter_size,
    }
}
