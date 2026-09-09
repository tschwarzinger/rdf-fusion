//! [SPARQL](https://www.w3.org/TR/sparql11-overview/) implementation.

mod error;
mod evaluate_query;
mod explanation;
mod optimizer;
mod options;
mod update;

pub use error::*;
pub use evaluate_query::{evaluate_query, evaluate_query_with_snapshot};
pub use explanation::QueryExplanation;
pub use optimizer::{create_optimizer_rules, create_pyhsical_optimizer_rules};
pub use options::*;
pub use rdf_fusion_common::sparql::{
    QueryDataset, QueryVariant, RdfFusionQuery, RdfFusionUpdate, UpdateOperation,
};
pub use update::evaluate_update;
