use crate::load_queries;
use crate::parquet::{
    ParquetTestConfig, format_bytes, get_dumped_bytes, setup_test_store,
};
use datafusion::physical_expr_common::metrics::MetricValue;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use prettytable::{Row, Table, cell};
use rdf_fusion::common::{QuadComponent, RdfDumpFormat, RdfSortOrder};
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
            if let MetricValue::Count { name, count } = metric.value()
                && name == "bytes_scanned"
            {
                total += count.value() as u64;
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
            "String(Sparql(POS))",
            RdfDumpOptions::default()
                .with_encoding(DumpEncoding::String)
                .with_sort_by(Some(RdfSortOrder::SparqlOrder(vec![
                    QuadComponent::Predicate,
                    QuadComponent::Object,
                    QuadComponent::Subject,
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

        let state = base_store.context().session_context().state();
        let storage = ParquetQuadStorage::builder(Url::parse(&test_url).unwrap())
            .with_encoding(config.config.encoding().into())
            .build(&state)
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
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | Access Pattern                | String(POS)         | String(SPO)         | String(Sparql(POS)) | PlainTerm(POS)      |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bound-object                  | 210 314 (2.1%)      | 8 465 892 (84.9%)   | 210 314 (2.1%)      | 2 365 520 (23.5%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bound-predicate               | 812 875 (8.1%)      | 2 240 642 (22.5%)   | 812 875 (8.1%)      | 818 913 (8.1%)      |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bound-subject                 | 2 186 543 (21.8%)   | 571 210 (5.7%)      | 2 186 543 (21.8%)   | 2 951 638 (29.3%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | load-review                   | 5 024 824 (50.2%)   | 10 872 411 (109.1%) | 5 024 824 (50.2%)   | 6 737 818 (66.8%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | load-review-with-lang-filter  | 5 024 824 (50.2%)   | 15 098 382 (151.5%) | 5 024 824 (50.2%)   | 6 737 818 (66.8%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q1                    | 1 482 998 (14.8%)   | 6 065 904 (60.8%)   | 1 482 998 (14.8%)   | 2 662 538 (26.4%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q10                   | 2 553 039 (25.5%)   | 4 203 767 (42.2%)   | 2 553 039 (25.5%)   | 2 766 756 (27.4%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q11                   | 1 390 544 (13.9%)   | 1 337 366 (13.4%)   | 1 390 544 (13.9%)   | 4 056 974 (40.2%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q12                   | 2 294 567 (22.9%)   | 2 181 418 (21.9%)   | 2 294 567 (22.9%)   | 8 989 504 (89.1%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q2-empty-optional     | 4 268 927 (42.6%)   | 7 160 947 (71.8%)   | 4 268 927 (42.6%)   | 19 239 013 (190.8%) |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q2-non-empty-optional | 4 720 039 (47.1%)   | 7 643 653 (76.7%)   | 4 720 039 (47.1%)   | 19 690 757 (195.3%) |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q3                    | 1 481 844 (14.8%)   | 4 018 011 (40.3%)   | 1 481 844 (14.8%)   | 2 729 896 (27.1%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q4                    | 3 450 883 (34.4%)   | 12 864 257 (129.0%) | 3 450 883 (34.4%)   | 5 960 609 (59.1%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q5                    | 1 287 159 (12.8%)   | 7 399 625 (74.2%)   | 1 287 159 (12.8%)   | 4 881 979 (48.4%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q7                    | 6 516 469 (65.0%)   | 21 307 272 (213.7%) | 6 516 469 (65.0%)   | 5 781 300 (57.3%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q8                    | 10 870 704 (108.5%) | 32 003 621 (321.0%) | 10 870 704 (108.5%) | 9 500 613 (94.2%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | explore-q9                    | 1 275 244 (12.7%)   | 1 051 620 (10.5%)   | 1 275 244 (12.7%)   | 3 375 608 (33.5%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bi-q1                         | 3 266 641 (32.6%)   | 27 778 092 (278.6%) | 3 266 641 (32.6%)   | 5 125 610 (50.8%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bi-q2                         | 841 714 (8.4%)      | 3 929 390 (39.4%)   | 841 714 (8.4%)      | 2 873 008 (28.5%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bi-q3                         | 545 646 (5.4%)      | 34 180 063 (342.9%) | 545 646 (5.4%)      | 613 408 (6.1%)      |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bi-q4                         | 4 808 047 (48.0%)   | 13 687 856 (137.3%) | 4 808 047 (48.0%)   | 6 163 854 (61.1%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bi-q5                         | 4 964 356 (49.5%)   | 33 071 131 (331.7%) | 4 964 356 (49.5%)   | 7 771 954 (77.1%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bi-q6                         | 3 027 145 (30.2%)   | 83 666 631 (839.3%) | 3 027 145 (30.2%)   | 3 206 340 (31.8%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bi-q7                         | 2 765 730 (27.6%)   | 6 654 866 (66.8%)   | 2 765 730 (27.6%)   | 2 812 177 (27.9%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    | bi-q8                         | 5 031 173 (50.2%)   | 12 692 890 (127.3%) | 5 031 173 (50.2%)   | 6 439 173 (63.9%)   |
    +-------------------------------+---------------------+---------------------+---------------------+---------------------+
    ");
}
