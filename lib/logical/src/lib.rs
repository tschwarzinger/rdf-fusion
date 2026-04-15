#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! SPARQL logical query plan.
//!
//! This crate contains the building blocks for creating a SPARQL logical query plan
//! that can be optimized and executed by Apache DataFusion. It provides builders
//! for constructing the plan programmatically and defines custom logical nodes
//! for SPARQL-specific operations.

extern crate core;

mod active_graph;
pub mod expr;
mod expr_builder;
mod expr_builder_context;
pub mod extend;
pub mod join;
mod logical_plan_builder;
mod logical_plan_builder_context;
pub mod minus;
pub mod object_id;
pub mod paths;
pub mod patterns;
pub mod quad_pattern;

pub use active_graph::{ActiveGraph, EnumeratedActiveGraph};
use datafusion::common::{DFSchema, plan_err};
pub use expr_builder::RdfFusionExprBuilder;
pub use expr_builder_context::RdfFusionExprBuilderContext;
pub use logical_plan_builder::RdfFusionLogicalPlanBuilder;
pub use logical_plan_builder_context::RdfFusionLogicalPlanBuilderContext;
use rdf_fusion_model::DFResult;

/// Checks if two schemas are logically equivalent in terms of names and types.
pub(crate) fn check_same_schema(
    old_schema: &DFSchema,
    new_schema: &DFSchema,
) -> DFResult<()> {
    if !old_schema.logically_equivalent_names_and_types(new_schema) {
        return plan_err!(
            "Schema of the new plan is not compatible with the old one. Old Schema: {:?}. New Schema: {:?}",
            old_schema,
            new_schema
        );
    }
    Ok(())
}
