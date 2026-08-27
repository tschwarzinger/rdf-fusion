use crate::load_queries;
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

/// This test counts the bytes scanned when querying different Parquet configurations. Note that
/// this is done on a significantly smaller dataset (1000 products) and thus will have different
/// results compared to the benchmark with a larger number of products.
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

        let registry = Arc::clone(
            &base_store
                .context()
                .session_context()
                .runtime_env()
                .object_store_registry,
        );
        let storage = ParquetQuadStorage::try_load(
            Url::parse(&test_url).unwrap(),
            QuadStorageEncodingName::String,
            Arc::as_ref(&registry),
        )
        .await
        .unwrap();

        let context = RdfFusionContextBuilder::new(Arc::new(storage))
            .with_single_partition_session_config()
            .with_runtime_env(Some(Arc::clone(
                &base_store.context().session_context().runtime_env(),
            )))
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
    +-------------------------------+---------------------+---------------------+
    | Access Pattern                | Native(POS)         | Sparql(POS)         |
    +-------------------------------+---------------------+---------------------+
    | bound-object                  | 161 637 (1.6%)      | 161 637 (1.6%)      |
    +-------------------------------+---------------------+---------------------+
    | bound-predicate               | 812 875 (8.1%)      | 812 875 (8.1%)      |
    +-------------------------------+---------------------+---------------------+
    | bound-subject                 | 2 120 871 (21.2%)   | 2 120 871 (21.2%)   |
    +-------------------------------+---------------------+---------------------+
    | load-review                   | 5 006 342 (50.0%)   | 5 006 342 (50.0%)   |
    +-------------------------------+---------------------+---------------------+
    | load-review-with-lang-filter  | 5 006 342 (50.0%)   | 5 006 342 (50.0%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q1                    | 1 421 103 (14.2%)   | 1 421 103 (14.2%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q10                   | 2 478 451 (24.7%)   | 2 478 451 (24.7%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q11                   | 1 270 050 (12.7%)   | 1 270 050 (12.7%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q12                   | 2 204 579 (22.0%)   | 2 204 579 (22.0%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q2-empty-optional     | 4 139 067 (41.3%)   | 4 139 067 (41.3%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q2-non-empty-optional | 4 590 217 (45.8%)   | 4 590 217 (45.8%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q3                    | 1 408 680 (14.1%)   | 1 408 680 (14.1%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q4                    | 3 473 960 (34.7%)   | 3 473 960 (34.7%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q5                    | 1 422 641 (14.2%)   | 1 422 641 (14.2%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q7                    | 6 762 473 (67.5%)   | 6 762 473 (67.5%)   |
    +-------------------------------+---------------------+---------------------+
    | explore-q8                    | 10 794 394 (107.7%) | 10 794 394 (107.7%) |
    +-------------------------------+---------------------+---------------------+
    | explore-q9                    | 1 250 617 (12.5%)   | 1 250 617 (12.5%)   |
    +-------------------------------+---------------------+---------------------+
    | bi-q1                         | 4 075 815 (40.7%)   | 4 075 815 (40.7%)   |
    +-------------------------------+---------------------+---------------------+
    | bi-q2                         | 808 878 (8.1%)      | 808 878 (8.1%)      |
    +-------------------------------+---------------------+---------------------+
    | bi-q3                         | 516 794 (5.2%)      | 516 794 (5.2%)      |
    +-------------------------------+---------------------+---------------------+
    | bi-q4                         | 4 698 443 (46.9%)   | 4 698 443 (46.9%)   |
    +-------------------------------+---------------------+---------------------+
    | bi-q5                         | 4 831 505 (48.2%)   | 4 831 505 (48.2%)   |
    +-------------------------------+---------------------+---------------------+
    | bi-q6                         | 2 906 431 (29.0%)   | 2 906 431 (29.0%)   |
    +-------------------------------+---------------------+---------------------+
    | bi-q7                         | 2 757 060 (27.5%)   | 2 757 060 (27.5%)   |
    +-------------------------------+---------------------+---------------------+
    | bi-q8                         | 5 024 174 (50.1%)   | 5 024 174 (50.1%)   |
    +-------------------------------+---------------------+---------------------+
    ");
}
