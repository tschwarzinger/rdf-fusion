use crate::parquet::{
    ParquetTestConfig, format_bytes, get_dumped_bytes, setup_test_store,
};
use bytes::Bytes;
use datafusion::parquet::file::reader::FileReader;
use datafusion::parquet::file::serialized_reader::SerializedFileReader;
use prettytable::{Table, row};
use rdf_fusion::common::{QuadComponent, RdfDumpFormat, RdfSortOrder};
use rdf_fusion::store::{DumpEncoding, RdfDumpOptions};

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

    let configs = vec![
        ParquetTestConfig::new(
            "NativeOrder([Subject, Predicate, Object])",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::String)
                .with_sort_by(Some(RdfSortOrder::NativeOrder(vec![
                    QuadComponent::Subject,
                    QuadComponent::Predicate,
                    QuadComponent::Object,
                ]))),
        ),
        ParquetTestConfig::new(
            "NativeOrder([Predicate, Object, Subject])",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::String)
                .with_sort_by(Some(RdfSortOrder::NativeOrder(vec![
                    QuadComponent::Predicate,
                    QuadComponent::Object,
                    QuadComponent::Subject,
                ]))),
        ),
        ParquetTestConfig::new(
            "NativeOrder([Object, Subject, Predicate])",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::String)
                .with_sort_by(Some(RdfSortOrder::NativeOrder(vec![
                    QuadComponent::Object,
                    QuadComponent::Subject,
                    QuadComponent::Predicate,
                ]))),
        ),
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

    for (i, config) in configs.into_iter().enumerate() {
        let test_url = format!("memory:///test_{i}.parquet");
        store
            .dump(
                test_url.clone(),
                RdfDumpFormat::Parquet,
                config.config.clone(),
            )
            .await
            .unwrap();

        let bytes = get_dumped_bytes(&store, &test_url).await;
        let metrics = compute_parquet_size_metrics(&bytes);

        table.add_row(row![
            config.name,
            format_bytes(metrics.total_file_size as u64),
            format_bytes(metrics.footer_size as u64),
            format_bytes(metrics.total_data_size as u64),
            format_bytes(metrics.page_index_size as u64),
            format_bytes(metrics.bloom_filter_size as u64)
        ]);
    }

    let table_str = table.to_string();
    println!("\nParquet File Size DataFrame:\n{}", table_str);

    insta::assert_snapshot!(table_str, @"
    +-------------------------------------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | Sort Order                                | File Size (Bytes) | Footer Size (Bytes) | Total Data Size (Bytes) | Page Index Size (Bytes) | Bloom Filter Size (Bytes) |
    +-------------------------------------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | NativeOrder([Subject, Predicate, Object]) | 10 166 029        | 10 330              | 9 686 095               | 174 068                 | 295 524                   |
    +-------------------------------------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | NativeOrder([Predicate, Object, Subject]) | 10 182 572        | 11 341              | 9 684 650               | 191 045                 | 295 524                   |
    +-------------------------------------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | NativeOrder([Object, Subject, Predicate]) | 10 444 310        | 10 919              | 9 947 424               | 190 431                 | 295 524                   |
    +-------------------------------------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    ");
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
