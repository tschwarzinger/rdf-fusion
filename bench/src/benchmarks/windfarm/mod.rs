mod benchmark;
mod generate;
mod queries;
mod report;

pub use benchmark::WindFarmBenchmark;
use clap::ValueEnum;
pub use queries::WindFarmQueryName;
use std::fmt::{Display, Formatter};

/// Indicates the size of the dataset.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum NumTurbines {
    #[value(name = "4")]
    N4,
    #[value(name = "16")]
    N16,
    #[value(name = "100")]
    N100,
    #[value(name = "400")]
    N400,
}

impl NumTurbines {
    /// Returns the number of turbines as usize.
    pub fn into_usize(self) -> usize {
        match self {
            NumTurbines::N4 => 4,
            NumTurbines::N16 => 16,
            NumTurbines::N100 => 100,
            NumTurbines::N400 => 400,
        }
    }
}

impl Display for NumTurbines {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            NumTurbines::N4 => "4",
            NumTurbines::N16 => "16",
            NumTurbines::N100 => "100",
            NumTurbines::N400 => "400",
        };
        write!(f, "{string}")
    }
}
