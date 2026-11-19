use crate::benchmarks::bsbm::NumProducts;
use crate::benchmarks::bsbm::use_case::{
    BsbmQuerySelection, BsbmUseCase, BsbmUseCaseName, EmptyQueryKind, first_instances,
};
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

    /// For each query, the instance with the median result count among the non-empty instances.
    fn queries(num_products: NumProducts) -> Vec<BsbmQuerySelection> {
        use BsbmExploreQueryName::*;
        let lines = match num_products {
            NumProducts::N1_000 => [
                (Q1, 27),
                (Q2, 34),
                (Q3, 30),
                (Q4, 933),
                (Q5, 14),
                (Q7, 62),
                (Q8, 20),
                (Q9, 18),
                (Q10, 23),
                (Q11, 25),
                (Q12, 26),
            ],
            NumProducts::N10_000 => [
                (Q1, 2),
                (Q2, 9),
                (Q3, 5),
                (Q4, 383),
                (Q5, 11),
                (Q7, 138),
                (Q8, 367),
                (Q9, 18),
                (Q10, 99),
                (Q11, 25),
                (Q12, 26),
            ],
            _ => return first_instances(Self::list_queries()),
        };
        lines
            .into_iter()
            .map(|(query, line)| BsbmQuerySelection::at_line(query, line))
            .collect()
    }

    fn empty_queries(
        num_products: NumProducts,
    ) -> anyhow::Result<Vec<BsbmQuerySelection>> {
        use BsbmExploreQueryName::*;
        use EmptyQueryKind::*;
        let lines = match num_products {
            NumProducts::N1_000 => [
                (Q1, Pattern, 52),
                (Q1, Join, 2),
                (Q3, Pattern, 155),
                (Q3, Join, 5),
                (Q4, Pattern, 33),
                (Q4, Join, 8),
                (Q5, Join, 236),
                (Q10, Join, 49),
            ],
            _ => anyhow::bail!(
                "No pre-selected empty queries for BSBM {} with {num_products} products",
                Self::name()
            ),
        };
        Ok(lines
            .into_iter()
            .map(|(query, kind, line)| {
                BsbmQuerySelection::empty_at_line(query, kind, line)
            })
            .collect())
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
