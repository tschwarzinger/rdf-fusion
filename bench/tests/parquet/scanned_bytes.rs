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
use rdf_fusion_storage_parquet::ParquetQuadStorage;
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
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | Access Pattern                | String(POS)        | String(SPO)         | String(Sparql(POS)) | PlainTerm(POS)      |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bound-object                  | 121 977 (1.4%)     | 8 677 411 (85.5%)   | 121 977 (1.4%)      | 1 975 610 (22.5%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bound-predicate               | 693 488 (8.0%)     | 2 006 068 (19.8%)   | 693 488 (8.0%)      | 714 812 (8.1%)      |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bound-subject                 | 1 791 452 (20.8%)  | 29 523 (0.3%)       | 1 791 452 (20.8%)   | 2 410 432 (27.5%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | load-review                   | 4 281 373 (49.6%)  | 8 467 091 (83.5%)   | 4 281 373 (49.6%)   | 5 705 534 (65.1%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | load-review-with-lang-filter  | 4 281 373 (49.6%)  | 14 466 242 (142.6%) | 4 281 373 (49.6%)   | 5 705 534 (65.1%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q1                    | 1 212 989 (14.1%)  | 2 204 415 (21.7%)   | 1 212 989 (14.1%)   | 2 216 431 (25.3%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q10                   | 1 939 552 (22.5%)  | 2 977 434 (29.4%)   | 1 939 552 (22.5%)   | 2 146 435 (24.5%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q11                   | 1 077 055 (12.5%)  | 825 193 (8.1%)      | 1 077 055 (12.5%)   | 3 380 547 (38.5%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q12                   | 1 734 851 (20.1%)  | 663 103 (6.5%)      | 1 734 851 (20.1%)   | 6 934 385 (79.1%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q2-empty-optional     | 3 399 568 (39.4%)  | 2 328 109 (22.9%)   | 3 399 568 (39.4%)   | 14 810 038 (168.9%) |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q2-non-empty-optional | 3 792 302 (43.9%)  | 2 371 977 (23.4%)   | 3 792 302 (43.9%)   | 15 197 239 (173.3%) |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q3                    | 1 207 517 (14.0%)  | 1 274 427 (12.6%)   | 1 207 517 (14.0%)   | 2 282 968 (26.0%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q4                    | 2 808 714 (32.5%)  | 4 788 190 (47.2%)   | 2 808 714 (32.5%)   | 4 886 048 (55.7%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q5                    | 1 128 527 (13.1%)  | 4 063 019 (40.1%)   | 1 128 527 (13.1%)   | 3 794 423 (43.3%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q7                    | 2 698 354 (31.3%)  | 15 435 368 (152.2%) | 2 698 354 (31.3%)   | 4 616 580 (52.6%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q8                    | 9 222 742 (106.9%) | 19 956 873 (196.7%) | 9 222 742 (106.9%)  | 7 923 434 (90.3%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | explore-q9                    | 1 028 820 (11.9%)  | 86 059 (0.8%)       | 1 028 820 (11.9%)   | 2 709 304 (30.9%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bi-q1                         | 2 651 210 (30.7%)  | 25 315 069 (249.5%) | 2 651 210 (30.7%)   | 4 251 463 (48.5%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bi-q2                         | 795 645 (9.2%)     | 2 950 119 (29.1%)   | 795 645 (9.2%)      | 2 351 583 (26.8%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bi-q3                         | 398 434 (4.6%)     | 33 597 558 (331.2%) | 398 434 (4.6%)      | 454 552 (5.2%)      |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bi-q4                         | 4 044 469 (46.9%)  | 8 858 723 (87.3%)   | 4 044 469 (46.9%)   | 5 199 255 (59.3%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bi-q5                         | 4 009 489 (46.5%)  | 28 225 705 (278.2%) | 4 009 489 (46.5%)   | 6 468 849 (73.8%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bi-q6                         | 2 367 383 (27.4%)  | 80 255 425 (791.1%) | 2 367 383 (27.4%)   | 2 481 039 (28.3%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bi-q7                         | 2 216 263 (25.7%)  | 4 834 216 (47.7%)   | 2 216 263 (25.7%)   | 2 257 193 (25.7%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    | bi-q8                         | 3 918 629 (45.4%)  | 8 461 804 (83.4%)   | 3 918 629 (45.4%)   | 5 145 024 (58.7%)   |
    +-------------------------------+--------------------+---------------------+---------------------+---------------------+
    ");
}
