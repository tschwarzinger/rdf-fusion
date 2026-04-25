//! [SPARQL](https://www.w3.org/TR/sparql11-overview/) implementation.

mod algebra;
pub mod error;
mod evaluate_query;
mod explanation;
mod optimizer;
mod rewriting;
mod update;

pub use crate::sparql::algebra::{QueryDataset, RdfFusionQuery, RdfFusionUpdate};
pub use crate::sparql::explanation::QueryExplanation;
pub use evaluate_query::{evaluate_query, evaluate_query_with_snapshot};
pub use optimizer::{create_optimizer_rules, create_pyhsical_optimizer_rules};
pub use rdf_fusion_model::{Variable, VariableNameParseError};
pub use update::evaluate_update;

/// Defines how many optimizations the query optimizer should apply.
///
/// Currently, the default value is [OptimizationLevel::Full], as we are still searching for a
/// subset that performs well on many queries. Once this subset has been identified, the default
/// value will be [OptimizationLevel::Default].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OptimizationLevel {
    /// No optimizations, except rewrites that are necessary for a working query.
    None,
    /// A balanced default optimization level. Suitable for simple queries or those handling modest
    /// data volumes.
    Default,
    /// Runs all optimizations. Ideal for complex queries or those processing large datasets.
    #[default]
    Full,
}

/// Options for SPARQL query evaluation.
#[derive(Clone, Default)]
pub struct QueryOptions {
    /// The defined optimization level
    pub optimization_level: OptimizationLevel,
}

/// Options for SPARQL update evaluation.
#[derive(Clone, Default)]
pub struct UpdateOptions;

impl From<QueryOptions> for UpdateOptions {
    #[inline]
    fn from(_query_options: QueryOptions) -> Self {
        Self {}
    }
}
