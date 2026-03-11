//! Contains tests that assert that the query plans for the benchmark queries are correct.
//!
//! This should be used for the following purposes:
//! - To ensure that the query plans do not change unexpectedly.
//! - Given a new optimization, verify that the "end to end" query plans are indeed changed.

use futures::StreamExt;
use insta::Settings;
use rdf_fusion::execution::results::QueryResults;

mod bsbm_business_intelligence;
mod bsbm_explore;
mod wind_farm;

fn run_plan_assertions(assertions: impl FnOnce()) {
    let mut settings = Settings::default();

    // This is a bit hacky. Oxigraph does not print leading zeroes, and therefore we must replace
    // also shorter uuids. We assume that more than 12 leading zeroes are very unlikely for random
    // uuids and that, on the other hand, 20 characters long hex numbers are also unlikely in LPs.
    settings.add_filter(r"\b[0-9a-fA-F]{20,32}\b", "<uuid>");

    // This is also a bit hacky. This searches for usages of object ids in the query plans.
    settings.add_filter(
        r"\[([0-9a-fA-F]+,?\s?)+\]\.\.\[([0-9a-fA-F]+,?\s?)+\]",
        "<object id range>",
    );
    settings.add_filter(r"= \[([0-9a-fA-F]{1,2},?\s?){4}\]", "= <object id>");
    settings.add_filter(r"= [0-9a-fA-F]{2,}", "= <object id>");
    settings.add_filter(
        r#"FixedSizeBinary\(4,\s"[0-9a-fA-F]+,[0-9a-fA-F]+,[0-9a-fA-F]+,[0-9a-fA-F]+"\)"#,
        "FixedSizeBinary(<object id>)",
    );

    settings.bind(|| assertions());
}

/// Consume the entire result.
async fn consume_result(query: QueryResults) -> () {
    match query {
        QueryResults::Solutions(solutions) => {
            let mut solutions = solutions.into_record_batch_stream().unwrap();
            while let Some(result) = solutions.next().await {
                result.unwrap();
            }
        }
        QueryResults::Boolean(_) => {}
        QueryResults::Graph(mut triples) => {
            while let Some(result) = triples.next().await {
                result.unwrap();
            }
        }
    }
}
