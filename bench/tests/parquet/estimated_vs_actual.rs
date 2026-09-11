use crate::parquet::{ParquetTestConfig, setup_test_store};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs, StatisticsContext};
use futures::StreamExt;
use prettytable::{Row, Table, cell};
use rdf_fusion::common::sparql::{QueryVariant, RdfFusionQuery};
use rdf_fusion::common::{
    NamedNode, NamedNodePattern, QuadComponent, RdfDumpFormat, RdfSortOrder, TermPattern,
    TriplePattern, Variable,
};
use rdf_fusion::encoding::QuadStorageEncoding;
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion::execution::{RdfFusionContext, RdfFusionContextBuilder};
use rdf_fusion::logical::ActiveGraph;
use rdf_fusion::logical::quad_pattern::QuadPatternNode;
use rdf_fusion::store::{DumpEncoding, RdfDumpOptions};
use rdf_fusion_storage::parquet::ParquetQuadStorage;
use std::sync::Arc;
use url::Url;

/// Asserts that the estimated scanned bytes produced by the estimator are consistent with the
/// bytes actually scanned during execution for a selection of triple patterns.
#[tokio::test]
async fn test_estimated_bytes_match_actual_scanned_bytes() {
    let base_store = setup_test_store().await;

    let config = ParquetTestConfig::new(
        "String(POS)",
        RdfDumpOptions::default()
            .with_encoding(DumpEncoding::String)
            .with_sort_by(Some(RdfSortOrder::NativeOrder(vec![
                QuadComponent::Predicate,
                QuadComponent::Object,
                QuadComponent::Subject,
            ]))),
    );

    let queries = vec![
        (
            "bound-subject",
            TriplePattern {
                subject: TermPattern::NamedNode(NamedNode::new("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9747").unwrap()),
                predicate: NamedNodePattern::Variable(Variable::new("p").unwrap()),
                object: TermPattern::Variable(Variable::new("o").unwrap()),
            }
        ),
        (
            "bound-predicate",
            TriplePattern {
                subject: TermPattern::Variable(Variable::new("s").unwrap()),
                predicate: NamedNodePattern::NamedNode(NamedNode::new("http://www.w3.org/2000/01/rdf-schema#label").unwrap()),
                object: TermPattern::Variable(Variable::new("o").unwrap()),
            }
        ),
        (
            "bound-object",
            TriplePattern {
                subject: TermPattern::Variable(Variable::new("s").unwrap()),
                predicate: NamedNodePattern::Variable(Variable::new("p").unwrap()),
                object: TermPattern::NamedNode(NamedNode::new("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer12/Product578").unwrap()),
            }
        ),
        (
            "subject-and-predicate",
            TriplePattern {
                subject: TermPattern::NamedNode(NamedNode::new("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9747").unwrap()),
                predicate: NamedNodePattern::NamedNode(NamedNode::new("http://www.w3.org/2000/01/rdf-schema#label").unwrap()),
                object: TermPattern::Variable(Variable::new("o").unwrap()),
            }
        ),
        (
            "free",
            TriplePattern {
                subject: TermPattern::Variable(Variable::new("s").unwrap()),
                predicate: NamedNodePattern::Variable(Variable::new("p").unwrap()),
                object: TermPattern::Variable(Variable::new("o").unwrap()),
            }
        ),
    ];

    let mut table = Table::new();
    table.add_row(Row::new(vec![
        cell!("Pattern"),
        cell!("Row Count"),
        cell!("Estimated"),
        cell!("Actual"),
        cell!("Ratio"),
    ]));

    let test_url = "memory:///test.parquet";
    base_store
        .dump(
            test_url.to_string(),
            RdfDumpFormat::Parquet,
            config.config.clone(),
        )
        .await
        .unwrap();

    let registry = Arc::clone(
        &base_store
            .context()
            .session_context()
            .runtime_env()
            .object_store_registry,
    );
    let storage = ParquetQuadStorage::try_load(
        Url::parse(test_url).unwrap(),
        config.config.encoding().into(),
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

    for (name, pattern) in &queries {
        let (row_count, estimated, actual) = run_query(&context, pattern).await;
        let ratio = if actual == 0 {
            "-".to_string()
        } else {
            format!("{:.2}%", (estimated as f64 / actual as f64) * 100.0)
        };
        table.add_row(Row::new(vec![
            cell!(name),
            cell!(row_count),
            cell!(estimated),
            cell!(actual),
            cell!(ratio),
        ]));
    }

    insta::assert_snapshot!(table.to_string(), @"
    +-----------------------+-----------+-----------+---------+---------+
    | Pattern               | Row Count | Estimated | Actual  | Ratio   |
    +-----------------------+-----------+-----------+---------+---------+
    | bound-subject         | 10        | 1969894   | 1907839 | 103.25% |
    +-----------------------+-----------+-----------+---------+---------+
    | bound-predicate       | 5930      | 812875    | 812875  | 100.00% |
    +-----------------------+-----------+-----------+---------+---------+
    | bound-object          | 47        | 171556    | 161637  | 106.14% |
    +-----------------------+-----------+-----------+---------+---------+
    | subject-and-predicate | 0         | 0         | 0       | -       |
    +-----------------------+-----------+-----------+---------+---------+
    | free                  | 374911    | 9663754   | 9663754 | 100.00% |
    +-----------------------+-----------+-----------+---------+---------+
    ");
}

/// Returns the estimated number of bytes scanned, obtained from the statistics.
fn find_estimated_bytes(plan: &Arc<dyn ExecutionPlan>) -> usize {
    assert_eq!(plan.children().len(), 0, "Scan should be a leaf node.");
    let stats = StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())
        .unwrap();
    *stats.total_byte_size.get_value().unwrap()
}

/// Returns the actual number of bytes scanned, obtained from the `bytes_scanned` execution metric.
fn find_bytes_scanned(plan: &Arc<dyn ExecutionPlan>) -> usize {
    assert_eq!(plan.children().len(), 0, "Scan should be a leaf node.");

    let metrics = plan.metrics().unwrap();
    let mut total = 0;
    for metric in metrics.iter() {
        if let MetricValue::Count { name, count } = metric.value() {
            if name == "bytes_scanned" {
                total += count.value();
            }
        }
    }

    total
}

async fn run_query(
    context: &RdfFusionContext,
    query: &TriplePattern,
) -> (u64, usize, usize) {
    let query = RdfFusionQuery::new(
        LogicalPlan::Extension(Extension {
            node: Arc::new(QuadPatternNode::new(
                QuadStorageEncoding::String,
                ActiveGraph::DefaultGraph,
                None,
                query.clone(),
            )),
        }),
        QueryVariant::Select,
    );

    let (results, explanation) = context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();

    let mut row_count = 0;
    match results {
        QueryResults::Solutions(mut solutions) => {
            while let Some(row) = solutions.next().await {
                row.unwrap();
                row_count += 1;
            }
        }
        QueryResults::Graph(mut triples) => {
            while let Some(triple) = triples.next().await {
                triple.unwrap();
                row_count += 1;
            }
        }
        _ => panic!("Unexpected query results format"),
    }

    let estimated = find_estimated_bytes(&explanation.execution_plan);
    let actual = find_bytes_scanned(&explanation.execution_plan);
    (row_count, estimated, actual)
}
