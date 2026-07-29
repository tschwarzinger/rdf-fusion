use clap::ValueEnum;
use std::fmt::{Display, Formatter};

mod benchmark;
mod business_intelligence;
mod explore;
mod operation;
mod report;
mod use_case;
pub use benchmark::BsbmBenchmark;
pub use business_intelligence::*;
pub use explore::*;
pub use use_case::BsbmUseCase;

/// Indicates the size of the dataset.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum NumProducts {
    #[value(name = "1000")]
    N1_000,
    #[value(name = "2500")]
    N2_500,
    #[value(name = "5000")]
    N5_000,
    #[value(name = "7500")]
    N7_500,
    #[value(name = "10000")]
    N10_000,
    #[value(name = "25000")]
    N25_000,
    #[value(name = "50000")]
    N50_000,
    #[value(name = "75000")]
    N75_000,
    #[value(name = "250000")]
    N250_000,
    #[value(name = "500000")]
    N500_000,
}

impl Display for NumProducts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            NumProducts::N1_000 => "1000",
            NumProducts::N2_500 => "2500",
            NumProducts::N5_000 => "5000",
            NumProducts::N7_500 => "7500",
            NumProducts::N10_000 => "10000",
            NumProducts::N25_000 => "25000",
            NumProducts::N50_000 => "50000",
            NumProducts::N75_000 => "75000",
            NumProducts::N250_000 => "250000",
            NumProducts::N500_000 => "500000",
        };
        write!(f, "{string}")
    }
}
