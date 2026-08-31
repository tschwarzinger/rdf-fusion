use crate::benchmarks::bsbm::use_case::{BsbmUseCase, BsbmUseCaseName};
use clap::ValueEnum;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

/// The BSBM Explore Use Case.
#[derive(Clone, Copy, Debug)]
pub struct ExploreUseCase;

impl BsbmUseCase for ExploreUseCase {
    fn name() -> BsbmUseCaseName {
        BsbmUseCaseName::Explore
    }

    fn list_queries() -> Vec<String> {
        BsbmExploreQueryName::list_queries()
            .into_iter()
            .map(|q| q.to_string())
            .collect()
    }

    fn queries_file_path() -> PathBuf {
        PathBuf::from("./queries-explore.csv")
    }
}

/// The BSBM explore query names.
///
/// Q6 is no longer part of the benchmark and is thus missing.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum BsbmExploreQueryName {
    Q1,
    Q2,
    Q3,
    Q4,
    Q5,
    Q7,
    Q8,
    Q9,
    Q10,
    Q11,
    Q12,
}

impl BsbmExploreQueryName {
    pub fn list_queries() -> Vec<Self> {
        vec![
            BsbmExploreQueryName::Q1,
            BsbmExploreQueryName::Q2,
            BsbmExploreQueryName::Q3,
            BsbmExploreQueryName::Q4,
            BsbmExploreQueryName::Q5,
            BsbmExploreQueryName::Q7,
            BsbmExploreQueryName::Q8,
            BsbmExploreQueryName::Q9,
            BsbmExploreQueryName::Q10,
            BsbmExploreQueryName::Q11,
            BsbmExploreQueryName::Q12,
        ]
    }
}

impl TryFrom<u8> for BsbmExploreQueryName {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(BsbmExploreQueryName::Q1),
            2 => Ok(BsbmExploreQueryName::Q2),
            3 => Ok(BsbmExploreQueryName::Q3),
            4 => Ok(BsbmExploreQueryName::Q4),
            5 => Ok(BsbmExploreQueryName::Q5),
            7 => Ok(BsbmExploreQueryName::Q7),
            8 => Ok(BsbmExploreQueryName::Q8),
            9 => Ok(BsbmExploreQueryName::Q9),
            10 => Ok(BsbmExploreQueryName::Q10),
            11 => Ok(BsbmExploreQueryName::Q11),
            12 => Ok(BsbmExploreQueryName::Q12),
            _ => Err(anyhow::anyhow!("Invalid BSBM explore query name: {value}")),
        }
    }
}

impl Display for BsbmExploreQueryName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            BsbmExploreQueryName::Q1 => "Q1",
            BsbmExploreQueryName::Q2 => "Q2",
            BsbmExploreQueryName::Q3 => "Q3",
            BsbmExploreQueryName::Q4 => "Q4",
            BsbmExploreQueryName::Q5 => "Q5",
            BsbmExploreQueryName::Q7 => "Q7",
            BsbmExploreQueryName::Q8 => "Q8",
            BsbmExploreQueryName::Q9 => "Q9",
            BsbmExploreQueryName::Q10 => "Q10",
            BsbmExploreQueryName::Q11 => "Q11",
            BsbmExploreQueryName::Q12 => "Q12",
        };
        write!(f, "{string}")
    }
}
