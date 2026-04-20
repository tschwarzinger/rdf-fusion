//! Contains tests that assert that the query plans for the benchmark queries are correct.
//!
//! This should be used for the following purposes:
//! - To ensure that the query plans do not change unexpectedly.
//! - Given a new optimization, verify that the "end to end" query plans are indeed changed.

use insta::Settings;

mod bsbm_business_intelligence;
mod bsbm_explore;
mod wind_farm;

fn run_plan_assertions(assertions: impl FnOnce()) {
    let mut settings = Settings::default();

    // This is a bit hacky. Oxigraph does not print leading zeroes, and therefore we must replace
    // also shorter uuids. We assume that more than 12 leading zeroes are very unlikely for random
    // uuids and that, on the other hand, 20 characters long hex numbers are also unlikely in LPs.
    settings.add_filter(r"\b[0-9a-fA-F]{20,32}\b", "<uuid>");

    settings.bind(|| assertions());
}
