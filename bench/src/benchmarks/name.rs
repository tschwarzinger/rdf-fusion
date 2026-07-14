use crate::benchmarks::bsbm::NumProducts;
use clap::Subcommand;
use std::fmt::{Display, Formatter};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Subcommand)]
pub enum BenchmarkName {
    /// Represents the BSBM explore benchmark.
    BsbmExplore {
        /// Indicates the scaling of the dataset.
        #[arg(short, long, default_value = "1000")]
        num_products: NumProducts,
        /// Provides an upper bound on the number of queries to be executed.
        #[arg(short, long)]
        max_query_count: Option<u64>,
    },
    /// Represents the BSBM business intelligence benchmark.
    BsbmBusinessIntelligence {
        /// Indicates the scaling of the dataset.
        #[arg(short, long, default_value = "1000")]
        num_products: NumProducts,
        /// Provides an upper bound on the number of queries to be executed.
        #[arg(short, long)]
        max_query_count: Option<u64>,
    },
}

impl BenchmarkName {
    /// Returns a directory name for the benchmark.
    pub fn data_dir_name(&self) -> String {
        match self {
            BenchmarkName::BsbmExplore { num_products, .. }
            | BenchmarkName::BsbmBusinessIntelligence { num_products, .. } => {
                format!("bsbm-{num_products}")
            }
        }
    }

    /// Returns a directory name for the benchmark.
    pub fn results_dir_name(&self) -> String {
        match self {
            BenchmarkName::BsbmExplore {
                num_products: dataset_size,
                max_query_count,
            } => match max_query_count {
                Some(max_query_count) => {
                    format!("bsbm-explore-{dataset_size}-{max_query_count}")
                }
                None => format!("bsbm-explore-{dataset_size}"),
            },
            BenchmarkName::BsbmBusinessIntelligence {
                num_products: dataset_size,
                max_query_count,
            } => match max_query_count {
                Some(max_query_count) => {
                    format!("bsbm-bi-{dataset_size}-{max_query_count}")
                }
                None => format!("bsbm-bi-{dataset_size}"),
            },
        }
    }
}

impl Display for BenchmarkName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchmarkName::BsbmExplore {
                num_products: dataset_size,
                max_query_count,
            } => match max_query_count {
                Some(max_query_count) => write!(
                    f,
                    "BSBM Explore: dataset_size={dataset_size}, max_query_count={max_query_count}"
                ),
                None => write!(f, "BSBM Explore: dataset_size={dataset_size}"),
            },
            BenchmarkName::BsbmBusinessIntelligence {
                num_products: dataset_size,
                max_query_count,
            } => match max_query_count {
                Some(max_query_count) => write!(
                    f,
                    "BSBM Business Intelligence: dataset_size={dataset_size}, max_query_count={max_query_count}"
                ),
                None => {
                    write!(f, "BSBM Business Intelligence: dataset_size={dataset_size}")
                }
            },
        }
    }
}
