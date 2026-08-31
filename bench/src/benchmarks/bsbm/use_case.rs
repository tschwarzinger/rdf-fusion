use crate::benchmarks::BenchmarkName;
use crate::benchmarks::bsbm::NumProducts;
use std::fmt::Display;
use std::hash::Hash;
use std::path::PathBuf;

/// The name of the supported BSBM use cases.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BsbmUseCaseName {
    /// The explore use case
    Explore,
    /// The business intelligence use case
    BusinessIntelligence,
}

impl BsbmUseCaseName {
    /// Creates a [BenchmarkName] with the given arguments.
    pub fn into_benchmark_name(
        self,
        num_products: NumProducts,
        max_query_count: Option<u64>,
    ) -> BenchmarkName {
        match self {
            BsbmUseCaseName::Explore => BenchmarkName::BsbmExplore {
                num_products,
                max_query_count,
            },
            BsbmUseCaseName::BusinessIntelligence => {
                BenchmarkName::BsbmBusinessIntelligence {
                    num_products,
                    max_query_count,
                }
            }
        }
    }
}

impl Display for BsbmUseCaseName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BsbmUseCaseName::Explore => write!(f, "explore"),
            BsbmUseCaseName::BusinessIntelligence => write!(f, "business-intelligence"),
        }
    }
}

/// Implements the use-case-specific logic for BSBM benchmarks.
pub trait BsbmUseCase: Send + Sync {
    /// Returns the name of the use case.
    fn name() -> BsbmUseCaseName;

    /// Lists all queries in the use case.
    fn list_queries() -> Vec<String>;

    /// The file path to the queries.
    fn queries_file_path() -> PathBuf;
}
