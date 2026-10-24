use crate::parquet::{ParquetTestConfig, setup_test_store};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs, StatisticsContext};
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use prettytable::{Row, Table, cell};
use rdf_fusion::common::config::{PlanningBloomFilters, RdfFusionOptions};
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

/// Compares the estimated scanned bytes with the bytes actually scanned during execution for a
/// selection of triple patterns, without using Bloom filters during planning.
///
/// Bound columns that are not part of the sort order prefix (here, the subject and the object) are
/// barely pruned by the row group statistics. Hence, their scans are heavily overestimated, while
/// the Bloom filters prune most row groups during execution. The actual bytes also contain the
/// Bloom filters that are read during execution.
#[tokio::test]
async fn test_estimated_bytes_without_planning_bloom_filters() {
    let table = estimated_vs_actual_table(PlanningBloomFilters::Never).await;
    insta::assert_snapshot!(table, @"
    +-----------------------+-----------+-----------+---------+----------+
    | Pattern               | Row Count | Estimated | Actual  | Ratio    |
    +-----------------------+-----------+-----------+---------+----------+
    | bound-subject         | 10        | 2882052   | 1569922 | 183.58%  |
    +-----------------------+-----------+-----------+---------+----------+
    | bound-predicate       | 5930      | 693756    | 693756  | 100.00%  |
    +-----------------------+-----------+-----------+---------+----------+
    | bound-object          | 47        | 1590121   | 122233  | 1300.89% |
    +-----------------------+-----------+-----------+---------+----------+
    | subject-and-predicate | 0         | 677878    | 8209    | 8257.74% |
    +-----------------------+-----------+-----------+---------+----------+
    | free                  | 374911    | 8275644   | 8275644 | 100.00%  |
    +-----------------------+-----------+-----------+---------+----------+
    ");
}

/// Same as [`test_estimated_bytes_without_planning_bloom_filters`] but with Bloom filters during
/// planning. The row groups that are pruned by the Bloom filters are not part of the estimate,
/// which brings the estimate close to the actual bytes.
#[tokio::test]
async fn test_estimated_bytes_with_planning_bloom_filters() {
    let table = estimated_vs_actual_table(PlanningBloomFilters::Always).await;
    insta::assert_snapshot!(table, @"
    +-----------------------+-----------+-----------+---------+---------+
    | Pattern               | Row Count | Estimated | Actual  | Ratio   |
    +-----------------------+-----------+-----------+---------+---------+
    | bound-subject         | 10        | 1545129   | 1553504 | 99.46%  |
    +-----------------------+-----------+-----------+---------+---------+
    | bound-predicate       | 5930      | 693756    | 693756  | 100.00% |
    +-----------------------+-----------+-----------+---------+---------+
    | bound-object          | 47        | 84060     | 83829   | 100.28% |
    +-----------------------+-----------+-----------+---------+---------+
    | subject-and-predicate | 0         | 0         | 0       | -       |
    +-----------------------+-----------+-----------+---------+---------+
    | free                  | 374911    | 8275644   | 8275644 | 100.00% |
    +-----------------------+-----------+-----------+---------+---------+
    ");
}

/// Returns a table that compares the estimated with the actual scanned bytes for a selection of
/// triple patterns on a POS-sorted file.
async fn estimated_vs_actual_table(
    planning_bloom_filters: PlanningBloomFilters,
) -> String {
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

    let state = base_store.context().session_context().state();
    let storage = ParquetQuadStorage::builder(Url::parse(test_url).unwrap())
        .with_encoding(config.config.encoding().into())
        .build(&state)
        .await
        .unwrap();

    let mut session_config = SessionConfig::new()
        .with_batch_size(8192)
        .with_target_partitions(1);
    session_config
        .options_mut()
        .execution
        .parquet
        .pushdown_filters = true;
    let mut rdf_fusion_options = RdfFusionOptions::default();
    rdf_fusion_options.execution.planning_bloom_filters = planning_bloom_filters;
    session_config
        .options_mut()
        .extensions
        .insert(rdf_fusion_options);

    let context = RdfFusionContextBuilder::new(Arc::new(storage))
        .with_session_config(Some(session_config))
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

    table.to_string()
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
        if let MetricValue::Count { name, count } = metric.value()
            && name == "bytes_scanned"
        {
            total += count.value();
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
