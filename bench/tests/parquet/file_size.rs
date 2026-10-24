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
            "String(SPO)",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::String)
                .with_sort_by(Some(RdfSortOrder::NativeOrder(vec![
                    QuadComponent::Subject,
                    QuadComponent::Predicate,
                    QuadComponent::Object,
                ]))),
        ),
        ParquetTestConfig::new(
            "String(POS)",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::String)
                .with_sort_by(Some(RdfSortOrder::NativeOrder(vec![
                    QuadComponent::Predicate,
                    QuadComponent::Object,
                    QuadComponent::Subject,
                ]))),
        ),
        ParquetTestConfig::new(
            "String(OSP)",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::String)
                .with_sort_by(Some(RdfSortOrder::NativeOrder(vec![
                    QuadComponent::Object,
                    QuadComponent::Subject,
                    QuadComponent::Predicate,
                ]))),
        ),
        ParquetTestConfig::new(
            "PlainTerm(POS)",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::PlainTerm)
                .with_sort_by(Some(RdfSortOrder::NativeOrder(vec![
                    QuadComponent::Predicate,
                    QuadComponent::Object,
                    QuadComponent::Subject,
                ]))),
        ),
    ];

    let mut table = Table::new();
    table.add_row(row![
        "Configuration",
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

    insta::assert_snapshot!(table.to_string(), @"
    +----------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | Configuration  | File Size (Bytes) | Footer Size (Bytes) | Total Data Size (Bytes) | Page Index Size (Bytes) | Bloom Filter Size (Bytes) |
    +----------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | String(SPO)    | 10 144 523        | 10 080              | 9 864 762               | 174 130                 | 95 539                    |
    +----------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | String(POS)    | 8 631 157         | 11 050              | 8 275 644               | 191 550                 | 152 901                   |
    +----------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | String(OSP)    | 9 310 750         | 10 724              | 9 009 869               | 190 509                 | 99 636                    |
    +----------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
    | PlainTerm(POS) | 8 770 903         | 29 178              | 8 296 931               | 286 805                 | 157 977                   |
    +----------------+-------------------+---------------------+-------------------------+-------------------------+---------------------------+
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
