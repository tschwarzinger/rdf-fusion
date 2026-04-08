use crate::prepare::PrepRequirement;
use async_trait::async_trait;
use std::path::Path;

pub mod bsbm;
mod name;
pub mod windfarm;

use crate::environment::BenchmarkContext;
use crate::report::BenchmarkReport;
pub use name::BenchmarkName;
use rdf_fusion::store::Store;

/// Represents a benchmark.
#[async_trait]
pub trait Benchmark {
    /// Returns the id of the benchmark.
    ///
    /// This must be a valid folder name and will be used to store files / results on the file
    /// system.
    fn name(&self) -> BenchmarkName;

    /// Returns a list of preparation requirements.
    fn requirements(&self, bench_files_path: &Path) -> Vec<PrepRequirement>;

    /// Prepares a [`Store`] but does not execute the benchmark.
    async fn prepare_store(
        &self,
        ctx: &BenchmarkContext<'_>,
        print_info: bool,
    ) -> anyhow::Result<Store>;

    /// Executes the benchmark using the given `bencher`.
    async fn execute(
        &self,
        ctx: &BenchmarkContext<'_>,
    ) -> anyhow::Result<Box<dyn BenchmarkReport>>;
}
