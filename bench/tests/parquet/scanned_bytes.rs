use crate::load::load_queries;
use crate::parquet::{
    ParquetTestConfig, format_bytes, get_dumped_bytes, setup_test_store,
};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use prettytable::{Row, Table, cell};
use rdf_fusion::common::{QuadComponent, RdfDumpFormat, RdfSortOrder};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion::store::{DumpEncoding, RdfDumpOptions, Store};
use rdf_fusion_storage::parquet::ParquetQuadStorage;
use std::sync::Arc;
use url::Url;

fn find_bytes_scanned(plan: &Arc<dyn ExecutionPlan>) -> u64 {
    let mut total = 0;
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            if let datafusion::physical_plan::metrics::MetricValue::Count {
                name,
                count,
            } = metric.value()
            {
                if name == "bytes_scanned" {
                    total += count.value() as u64;
                }
            }
        }
    }
    for child in plan.children() {
        total += find_bytes_scanned(child);
    }
    total
}

#[tokio::test]
async fn test_parquet_scanned_bytes() {
    let base_store = setup_test_store().await;

    let configs = vec![
        ParquetTestConfig::new(
            "Native(POS)",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::String)
                .with_sort_by(Some(RdfSortOrder::NativeOrder(vec![
                    QuadComponent::Predicate,
                    QuadComponent::Object,
                    QuadComponent::Subject,
                ]))),
        ),
        ParquetTestConfig::new(
            "ZOrder(POS)",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::String)
                .with_sort_by(Some(RdfSortOrder::ZOrder(vec![
                    QuadComponent::Predicate,
                    QuadComponent::Object,
                    QuadComponent::Subject,
                ]))),
        ),
        ParquetTestConfig::new(
            "Sparql(POS)",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::String)
                .with_sort_by(Some(RdfSortOrder::SparqlOrder(vec![
                    QuadComponent::Predicate,
                    QuadComponent::Object,
                    QuadComponent::Subject,
                ]))),
        ),
    ];

    let mut queries = Vec::new();

    queries.extend(load_queries("tests/test_queries/bsbm/micro").unwrap());
    queries.extend(load_queries("tests/test_queries/bsbm/explore").unwrap());
    queries.extend(load_queries("tests/test_queries/bsbm/bi").unwrap());

    // We will collect results to print a nice comparison table
    let mut table = Table::new();

    let mut header = Vec::new();
    header.push(cell!("Access Pattern"));
    header.extend(configs.iter().map(|c| cell!(c.name)));
    table.add_row(Row::new(header));

    // To keep it highly performant, we dump each configuration once
    let mut stores = Vec::new();
    for config in &configs {
        let clean_name = config.name.replace("(", "_").replace(")", "_");
        let test_url = format!("memory:///test_{clean_name}.parquet");

        base_store
            .dump(
                test_url.clone(),
                RdfDumpFormat::Parquet,
                config.config.clone(),
            )
            .await
            .unwrap();

        let bytes = get_dumped_bytes(&base_store, &test_url).await;
        let file_size = bytes.len() as u64;

        let registry = base_store
            .context()
            .session_context()
            .runtime_env()
            .object_store_registry
            .clone();
        let storage = ParquetQuadStorage::try_load(
            Url::parse(&test_url).unwrap(),
            QuadStorageEncodingName::String,
            Arc::as_ref(&registry),
        )
        .await
        .unwrap();

        let context = RdfFusionContextBuilder::new(Arc::new(storage))
            .with_single_partition_session_config()
            .with_runtime_env(Some(
                base_store.context().session_context().runtime_env().clone(),
            ))
            .build()
            .unwrap();

        let parquet_store = Store::new(context);
        stores.push((&config.name, parquet_store, file_size));
    }

    // Now execute all queries on all stores
    for (q_name, query_str) in &queries {
        let mut row_data = vec![q_name.clone()];

        for (_, store, file_size) in &stores {
            let (results, explanation) = store
                .explain_query_opt(query_str, QueryOptions::default())
                .await
                .unwrap();

            match results {
                QueryResults::Solutions(mut solutions) => {
                    while let Some(row) = solutions.next().await {
                        row.unwrap();
                    }
                }
                QueryResults::Graph(mut triples) => {
                    while let Some(triple) = triples.next().await {
                        triple.unwrap();
                    }
                }
                _ => panic!("Unexpected query results format"),
            }

            let bytes_scanned = find_bytes_scanned(&explanation.execution_plan);
            let percentage = (bytes_scanned as f64 / *file_size as f64) * 100.0;
            row_data.push(format!(
                "{} ({:.1}%)",
                format_bytes(bytes_scanned),
                percentage
            ));
        }

        table.add_row(Row::new(row_data.into_iter().map(|s| cell!(s)).collect()));
    }

    insta::assert_snapshot!(table.to_string(), @"
    +-------------------------------+---------------------+---------------------+---------------------+
    | Access Pattern                | Native(POS)         | ZOrder(POS)         | Sparql(POS)         |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bound-object                  | 167 245 (1.6%)      | 678 852 (6.6%)      | 167 245 (1.6%)      |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bound-predicate               | 1 000 472 (9.8%)    | 8 436 916 (81.6%)   | 1 000 472 (9.8%)    |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bound-subject                 | 2 241 394 (22.0%)   | 3 112 279 (30.1%)   | 2 241 394 (22.0%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | load-review                   | 5 135 590 (50.4%)   | 4 106 098 (39.7%)   | 5 135 590 (50.4%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | load-review-with-lang-filter  | 6 653 247 (65.3%)   | 4 106 098 (39.7%)   | 6 653 247 (65.3%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q1                    | 1 716 928 (16.9%)   | 2 125 189 (20.5%)   | 1 716 928 (16.9%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q10                   | 2 959 733 (29.1%)   | 2 349 412 (22.7%)   | 2 959 733 (29.1%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q11                   | 1 383 088 (13.6%)   | 724 916 (7.0%)      | 1 383 088 (13.6%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q12                   | 2 591 157 (25.4%)   | 5 867 793 (56.7%)   | 2 591 157 (25.4%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q2-empty-optional     | 4 895 143 (48.1%)   | 13 070 713 (126.4%) | 4 895 143 (48.1%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q2-non-empty-optional | 4 942 701 (48.5%)   | 13 587 719 (131.4%) | 4 942 701 (48.5%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q3                    | 2 713 475 (26.6%)   | 10 040 144 (97.1%)  | 2 713 475 (26.6%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q4                    | 4 065 646 (39.9%)   | 9 073 224 (87.7%)   | 4 065 646 (39.9%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q5                    | 1 420 772 (14.0%)   | 10 641 232 (102.9%) | 1 420 772 (14.0%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q7                    | 8 775 337 (86.2%)   | 16 001 916 (154.7%) | 8 775 337 (86.2%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q8                    | 12 476 544 (122.5%) | 11 682 289 (112.9%) | 12 476 544 (122.5%) |
    +-------------------------------+---------------------+---------------------+---------------------+
    | explore-q9                    | 1 516 588 (14.9%)   | 1 239 551 (12.0%)   | 1 516 588 (14.9%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bi-q1                         | 4 837 782 (47.5%)   | 950 871 (9.2%)      | 4 837 782 (47.5%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bi-q2                         | 996 475 (9.8%)      | 1 198 323 (11.6%)   | 996 475 (9.8%)      |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bi-q3                         | 548 580 (5.4%)      | 1 132 554 (10.9%)   | 548 580 (5.4%)      |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bi-q4                         | 5 576 737 (54.8%)   | 1 485 559 (14.4%)   | 5 576 737 (54.8%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bi-q5                         | 5 697 412 (56.0%)   | 934 768 (9.0%)      | 5 697 412 (56.0%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bi-q6                         | 2 996 072 (29.4%)   | 8 079 407 (78.1%)   | 2 996 072 (29.4%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bi-q7                         | 3 242 465 (31.8%)   | 940 192 (9.1%)      | 3 242 465 (31.8%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    | bi-q8                         | 5 944 689 (58.4%)   | 2 640 125 (25.5%)   | 5 944 689 (58.4%)   |
    +-------------------------------+---------------------+---------------------+---------------------+
    ");
}
