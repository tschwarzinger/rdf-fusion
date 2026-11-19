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

    /// Selects one instance with a non-empty result for each query in the use case.
    ///
    /// BSBM generates query instances randomly and some of them have an empty result. If there are
    /// no pre-selected instances for `num_products`, this falls back to the first instance of each
    /// query (which may have an empty result).
    fn queries(num_products: NumProducts) -> Vec<BsbmQuerySelection> {
        let _ = num_products;
        first_instances(Self::list_queries())
    }

    /// Selects instances with an empty result (see [EmptyQueryKind]).
    ///
    /// Returns an error if there are no pre-selected instances for `num_products`.
    fn empty_queries(
        num_products: NumProducts,
    ) -> anyhow::Result<Vec<BsbmQuerySelection>> {
        anyhow::bail!(
            "No pre-selected empty queries for BSBM {} with {num_products} products",
            Self::name()
        )
    }
}

/// Selects the first instance of each query in `queries`.
pub(crate) fn first_instances(queries: Vec<String>) -> Vec<BsbmQuerySelection> {
    queries.into_iter().map(BsbmQuerySelection::first).collect()
}

/// Why a BSBM query instance has an empty result.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum EmptyQueryKind {
    /// A triple pattern that must match has no match in the data.
    Pattern,
    /// Every triple pattern has a match, but joining them yields no result.
    Join,
}

impl Display for EmptyQueryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmptyQueryKind::Pattern => write!(f, "empty pattern"),
            EmptyQueryKind::Join => write!(f, "empty join"),
        }
    }
}

/// Selects an instance of a BSBM query from the pre-generated queries file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BsbmQuerySelection {
    /// The name under which the instance is reported (e.g., `Q1` or `Q1 (empty join)`).
    pub name: String,
    /// The BSBM query (e.g., `Q1`) that the instance must be an instance of.
    pub query: String,
    /// The line of the instance in the queries file (1-based, the header is line 1). `None`
    /// selects the first instance of `query`.
    pub line: Option<usize>,
}

impl BsbmQuerySelection {
    /// Selects the first instance of `query`.
    pub fn first(query: String) -> Self {
        Self {
            name: query.clone(),
            query,
            line: None,
        }
    }

    /// Selects the instance of `query` at `line`.
    pub fn at_line(query: impl Display, line: usize) -> Self {
        Self {
            name: query.to_string(),
            query: query.to_string(),
            line: Some(line),
        }
    }

    /// Selects the empty instance of `query` at `line`.
    pub fn empty_at_line(query: impl Display, kind: EmptyQueryKind, line: usize) -> Self {
        Self {
            name: format!("{query} ({kind})"),
            query: query.to_string(),
            line: Some(line),
        }
    }
}
