//! Shared setup for the tests that switch a quad pattern scan to another quad table at execution
//! time, once the dynamic filter of a hash join restricts a column to a few values.
//!
//! The tests themselves live in the `parquet` and `delta` modules.

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use rdf_fusion_common::config::RdfFusionOptions;
use rdf_fusion_common::{GraphName, Literal, NamedNode, Quad};
use rdf_fusion_execution::results::QueryResults;
use rdf_fusion_execution::sparql::QueryOptions;
use rdf_fusion_execution::{RdfFusionContext, RdfFusionContextBuilder};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_sparql_parser::ParserOptions;
use std::sync::Arc;
use url::Url;

/// The number of authors, each with a name.
const NUM_AUTHORS: usize = 20_000;
/// The number of distinct names. Hence, multiple authors share a name.
const NUM_NAMES: usize = 1_000;
/// The number of rows per page. The pages are small, such that a lookup only reads a few of them.
const ROWS_PER_PAGE: usize = 100;

pub(super) const PREFIX: &str = "PREFIX ex: <http://example.org/>";

pub(super) const DISABLE_SWITCHING: (&str, &str) =
    ("rdf_fusion.execution.quad_table_switching", "false");

/// Runs queries against a storage with the quad tables GPOS, GSPO, and GOSP (like dblp).
///
/// The storage contains `NUM_AUTHORS` authors with a name, and publications `pub<k>` that are
/// authored by `k` authors. The names are not ordered like the authors. The Parquet and Delta
/// tests create the storage and use this type to run the queries.
pub(super) struct SwitchTest {
    context: SessionContext,
    storage: Arc<dyn QuadStorage>,
}

/// The result of a query.
pub(super) struct QueryResult {
    /// The sorted solutions.
    pub(super) solutions: Vec<String>,
    /// The executed plan, with metrics.
    pub(super) plan: String,
    /// The executed plan, without metrics.
    pub(super) plan_without_metrics: String,
    /// The executed plan in the default (non-verbose) format, without metrics.
    pub(super) plan_default: String,
    /// The bytes scanned by the scans during execution.
    pub(super) bytes_scanned: usize,
    /// The bytes scanned by the scans during planning.
    pub(super) bytes_scanned_planning: usize,
    /// The bytes scanned while planning the alternative scans of quad table switches.
    pub(super) switch_bytes_scanned_planning: usize,
    /// The rows decoded by the scans (i.e., not pruned by statistics or the page index).
    pub(super) rows_decoded: usize,
    /// The verbose display of each `ParquetQuadScanExec` after execution.
    scans: Vec<String>,
}

impl QueryResult {
    /// Returns how the probe scan (`?author ex:name ?name`) decided.
    pub(super) fn probe_scan_switch(&self) -> String {
        self.scans
            .iter()
            .find(|scan| scan.contains("http://example.org/name"))
            .and_then(|scan| scan.split_once("quad_table_switch="))
            .map(|(_, decision)| decision.to_owned())
            .unwrap_or_default()
    }
}

impl SwitchTest {
    /// Creates a session context that writes small files and pages.
    pub(super) fn session_context() -> SessionContext {
        SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(1024)
                .with_target_partitions(1)
                .set_usize("datafusion.execution.soft_max_rows_per_output_file", 5_000)
                .set_usize("datafusion.execution.parquet.max_row_group_size", 2_000)
                .set_usize(
                    "datafusion.execution.parquet.data_page_row_count_limit",
                    ROWS_PER_PAGE,
                )
                .set_usize(
                    "datafusion.execution.parquet.write_batch_size",
                    ROWS_PER_PAGE,
                ),
        )
    }

    /// Creates a test for the `storage`, which must contain the [`test_quads`]. The `context` must
    /// have been created with [`SwitchTest::session_context`].
    pub(super) fn new(context: SessionContext, storage: Arc<dyn QuadStorage>) -> Self {
        Self { context, storage }
    }

    /// Returns a session that can write into the storage.
    pub(super) fn writing_session(&self) -> SessionContext {
        rdf_fusion_context(&self.context, Arc::clone(&self.storage), &[])
            .session_context()
            .clone()
    }

    /// Runs the query in a new context (with an empty metadata cache) with the given options.
    pub(super) async fn run(&self, query: &str, options: &[(&str, &str)]) -> QueryResult {
        let runtime_env = SessionContext::new().runtime_env();
        runtime_env.object_store_registry.register_store(
            &Url::parse("memory:///").unwrap(),
            self.context
                .runtime_env()
                .object_store(
                    datafusion::datasource::object_store::ObjectStoreUrl::parse(
                        "memory://",
                    )
                    .unwrap(),
                )
                .unwrap(),
        );
        let context =
            SessionContext::new_with_config_rt(self.context.copied_config(), runtime_env);
        let rdf_context =
            rdf_fusion_context(&context, Arc::clone(&self.storage), options);

        let query = rdf_fusion_sparql_parser::parse_query(
            &rdf_context.create_view(),
            query,
            &ParserOptions::default(),
        )
        .unwrap();
        let (results, explanation) = rdf_context
            .execute_query(&query, QueryOptions::default())
            .await
            .unwrap();
        let QueryResults::Solutions(mut stream) = results else {
            panic!("Expected solutions");
        };
        let mut solutions = Vec::new();
        while let Some(solution) = stream.next().await {
            let solution = solution.unwrap();
            solutions.push(
                solution
                    .iter()
                    .map(|(variable, term)| format!("{variable}={term}"))
                    .collect::<Vec<_>>()
                    .join(" "),
            );
        }
        solutions.sort();

        let plan = &explanation.execution_plan;
        let mut scans = Vec::new();
        collect_scans(plan, &mut scans);
        QueryResult {
            solutions,
            plan: DisplayableExecutionPlan::with_metrics(plan.as_ref())
                .indent(true)
                .to_string(),
            plan_without_metrics: DisplayableExecutionPlan::new(plan.as_ref())
                .indent(true)
                .to_string(),
            plan_default: DisplayableExecutionPlan::new(plan.as_ref())
                .indent(false)
                .to_string(),
            bytes_scanned: sum_metric(plan, "bytes_scanned"),
            bytes_scanned_planning: sum_metric(plan, "bytes_scanned_planning"),
            switch_bytes_scanned_planning: sum_metric(
                plan,
                "switch_bytes_scanned_planning",
            ),
            rows_decoded: sum_metric(plan, "pushdown_rows_matched")
                + sum_metric(plan, "pushdown_rows_pruned"),
            scans,
        }
    }
}

/// Creates an [`RdfFusionContext`] that shares the runtime environment of the `context` and uses
/// its configuration with the given `options`.
pub(super) fn rdf_fusion_context(
    context: &SessionContext,
    storage: Arc<dyn QuadStorage>,
    options: &[(&str, &str)],
) -> RdfFusionContext {
    let mut config = context
        .copied_config()
        .with_option_extension(RdfFusionOptions::default());
    config.options_mut().execution.parquet.pushdown_filters = true;
    for (key, value) in options {
        config.options_mut().set(key, value).unwrap();
    }
    RdfFusionContextBuilder::new(storage)
        .with_session_config(Some(config))
        .with_runtime_env(Some(context.runtime_env()))
        .build()
        .unwrap()
}

/// Returns the test data (see [`SwitchTest`]).
pub(super) fn test_quads() -> Vec<Quad> {
    let mut quads = Vec::new();
    for author in 0..NUM_AUTHORS {
        // A permutation of the authors, such that the names are not ordered like the authors.
        let name = (author * 7919) % NUM_NAMES;
        quads.push(Quad::new(
            NamedNode::new_unchecked(format!("http://example.org/author{author:05}")),
            NamedNode::new_unchecked("http://example.org/name"),
            Literal::new_simple_literal(format!("Name {name:04}")),
            GraphName::DefaultGraph,
        ));
    }
    for num_authors in [3, 10, 40, 100] {
        for i in 0..num_authors {
            // Spread the authors across the subjects. Two consecutive authors share a name.
            let author =
                ((i / 2) * 4999 + (i % 2) * NUM_NAMES + num_authors) % NUM_AUTHORS;
            quads.push(quad(
                &format!("http://example.org/pub{num_authors}"),
                "http://example.org/authoredBy",
                &format!("http://example.org/author{author:05}"),
            ));
        }
    }
    quads
}

/// Returns the test data as N-Triples.
pub(super) fn test_ntriples() -> String {
    test_quads()
        .into_iter()
        .map(|quad| format!("{quad} .\n"))
        .collect()
}

/// Creates a quad in the default graph from IRIs.
pub(super) fn quad(subject: &str, predicate: &str, object: &str) -> Quad {
    Quad::new(
        NamedNode::new_unchecked(subject),
        NamedNode::new_unchecked(predicate),
        NamedNode::new_unchecked(object),
        GraphName::DefaultGraph,
    )
}

fn collect_scans(plan: &Arc<dyn ExecutionPlan>, scans: &mut Vec<String>) {
    if plan.name() == "ParquetQuadScanExec" {
        // The verbose format explains why a scan kept the planned quad table.
        scans.push(
            DisplayableExecutionPlan::new(plan.as_ref())
                .indent(true)
                .to_string()
                .trim_end()
                .to_owned(),
        );
    }
    for child in plan.children() {
        collect_scans(child, scans);
    }
}

/// Sums the metric with the given name over all nodes of the plan.
fn sum_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let own = plan
        .metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map(|value| value.as_usize())
        .unwrap_or(0);
    own + plan
        .children()
        .into_iter()
        .map(|child| sum_metric(child, name))
        .sum::<usize>()
}
