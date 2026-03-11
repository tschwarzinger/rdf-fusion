use crate::index::{IndexComponents, ScanInstructions};
use crate::memory::encoding::{EncodedActiveGraph, EncodedTermPattern};
use crate::memory::object_id::{DEFAULT_GRAPH_ID, EncodedObjectId, FIRST_OBJECT_ID};
use crate::memory::storage::predicate_pushdown::MemStoragePredicateExpr;
use datafusion::common::{exec_datafusion_err, plan_datafusion_err};
use rdf_fusion_model::{DFResult, Variable};
use std::collections::{BTreeSet, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

/// A list of [MemIndexScanInstruction]s for querying a quad index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemIndexScanInstructions(IndexComponents, [MemIndexScanInstruction; 4]);

impl MemIndexScanInstructions {
    /// Creates a new [MemIndexScanInstructions] from the given [MemIndexScanInstruction]s.
    ///
    /// If more than one scans bind to a given variable, equality checks are handled automatically.
    pub fn new(
        components: IndexComponents,
        instructions: [MemIndexScanInstruction; 4],
    ) -> Self {
        let mut new_instructions = Vec::new();
        let mut seen = HashSet::new();

        for instruction in instructions {
            match instruction {
                MemIndexScanInstruction::Scan(var, predicate) => {
                    let inserted = seen.insert(Arc::clone(&var));
                    if inserted {
                        new_instructions
                            .push(MemIndexScanInstruction::Scan(var, predicate))
                    } else {
                        new_instructions.push(MemIndexScanInstruction::Traverse(Some(
                            MemIndexScanPredicate::EqualTo(Arc::clone(&var)),
                        )));
                    }
                }
                instruction => {
                    new_instructions.push(instruction);
                }
            }
        }

        Self(components, new_instructions.try_into().unwrap())
    }

    /// Calls [Self::new] with [IndexComponents::GSPO].
    pub fn new_gspo(instructions: [MemIndexScanInstruction; 4]) -> Self {
        Self::new(IndexComponents::GSPO, instructions)
    }

    /// Returns the current [IndexComponents] for the order of the scan instructions.
    pub fn index_components(&self) -> IndexComponents {
        self.0
    }

    /// Returns the inner [MemIndexScanInstruction]s.
    pub fn inner(&self) -> &[MemIndexScanInstruction; 4] {
        &self.1
    }

    /// Returns the inner [MemIndexScanInstruction]s by consuming self.
    pub fn into_inner(self) -> [MemIndexScanInstruction; 4] {
        self.1
    }

    /// Tries to find the [MemIndexScanInstruction] for a given column name.
    ///
    /// It should not be possible for two instructions to have the same name, as the second
    /// instruction should have been turned into a predicate.
    pub fn instructions_for_column(
        &self,
        column: &str,
    ) -> Option<(usize, &MemIndexScanInstruction)> {
        self.1
            .iter()
            .enumerate()
            .find(|(_, i)| i.scan_variable() == Some(column))
    }

    /// Returns new [MemIndexScanInstructions] with the given `instruction` at the given `index`.
    ///
    /// # Panics
    ///
    /// Will panic if the index is out-of-range.
    pub fn with_new_instruction_at(
        self,
        index: usize,
        instruction: MemIndexScanInstruction,
    ) -> Self {
        let mut new_instructions = self.1;
        new_instructions[index] = instruction;
        Self(self.0, new_instructions)
    }

    /// Applies a new `predicate` expression to the instructions.
    ///
    /// This will find the corresponding "scan" instruction that scans the column of the `predicate` and logically
    /// combine `predicate` with any existing predicate on that column.
    pub fn apply_filter(self, predicate: &MemStoragePredicateExpr) -> DFResult<Self> {
        if *predicate == MemStoragePredicateExpr::True {
            return Ok(self);
        }

        let column = predicate.column().ok_or_else(|| {
            plan_datafusion_err!("Invalid Predicate: Filter must have a column")
        })?;

        // If the filter is not a predicate, we can simply return the current scan.
        let Some(predicate) = predicate.to_scan_predicate()? else {
            return Ok(self);
        };

        let (idx, scan_instruction) =
            self.instructions_for_column(column).ok_or_else(|| {
                exec_datafusion_err!(
                    "Could not find scan instruction for column: {}",
                    column
                )
            })?;

        let new_predicate = scan_instruction
            .predicate()
            .map(|existing_predicate| existing_predicate.try_and_with(&predicate))
            .unwrap_or(Some(predicate))
            .ok_or(plan_datafusion_err!(
                "Could not apply predicate to scan instruction."
            ))?;
        let new_instruction = scan_instruction.clone().with_predicate(new_predicate);

        Ok(self.with_new_instruction_at(idx, new_instruction))
    }
}

impl ScanInstructions for MemIndexScanInstructions {
    fn reorder(&self, components: IndexComponents) -> Self {
        let mut reordered = Vec::new();
        for component in components.inner() {
            let index = self
                .0
                .inner()
                .iter()
                .position(|i| i == component)
                .expect("All components mut exist");
            reordered.push(self.1[index].clone());
        }
        Self(
            components,
            reordered.try_into().expect("Should yield 4 instructions"),
        )
    }
}

/// A predicate for filtering object ids.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum MemIndexScanPredicate {
    /// Always returns false.
    False,
    /// Checks whether the object id is in the given set.
    In(BTreeSet<EncodedObjectId>),
    /// Checks whether the object id is between the given object ids (end is inclusive).
    Between(EncodedObjectId, EncodedObjectId),
    /// Checks whether the object id is equal to the scan instruction with the given variable.
    EqualTo(Arc<String>),
}

impl MemIndexScanPredicate {
    /// Combines this predicate with `other` using a logical and.
    pub fn try_and_with(
        &self,
        other: &MemIndexScanPredicate,
    ) -> Option<MemIndexScanPredicate> {
        use MemIndexScanPredicate::*;
        let result = match (self, other) {
            // False with any predicate is false.
            (_, False) | (False, _) => False,

            // Intersect the sets.
            (In(a), In(b)) => {
                let inter: BTreeSet<_> = a.intersection(b).cloned().collect();
                if inter.is_empty() { False } else { In(inter) }
            }

            // For In, we can simply filter based on the other predicate.
            (In(a), Between(f, t)) | (Between(f, t), In(a)) => {
                let filtered: BTreeSet<_> = a
                    .iter()
                    .filter(|v| **v >= *f && **v <= *t)
                    .cloned()
                    .collect();
                if filtered.is_empty() {
                    False
                } else {
                    In(filtered)
                }
            }

            // Intersect between ranges
            (Between(from_1, to_1), Between(from_2, to_2)) => {
                let from = (*from_1).max(*from_2);
                let to = (*to_1).min(*to_2);
                if from > to { False } else { Between(from, to) }
            }

            // Otherwise, return None to indicate that the predicates cannot be combined.
            _ => return None,
        };
        Some(result)
    }
}

impl Display for MemIndexScanPredicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MemIndexScanPredicate::False => f.write_str("false"),
            MemIndexScanPredicate::In(set) => {
                if set.len() == 1 {
                    write!(f, "== {}", set.iter().next().unwrap())
                } else {
                    write!(
                        f,
                        "in ({})",
                        set.iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            }
            MemIndexScanPredicate::Between(from, to) => {
                write!(f, "in ({from}..{to})")
            }
            MemIndexScanPredicate::EqualTo(column) => write!(f, "== {column}"),
        }
    }
}

/// A trait for obtaining a [MemStoragePredicateExpr] which is still unknown at planning time (dynamic filter).
pub trait MemIndexScanPredicateSource: Debug + Send + Sync + Display {
    /// Returns the current predicate.
    fn current_predicate_expr(&self) -> DFResult<MemStoragePredicateExpr>;
}

/// An encoded version of a triple pattern.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MemIndexScanInstruction {
    /// Traverses the index level, not binding the elements at this level.
    Traverse(Option<MemIndexScanPredicate>),
    /// Scans the index level, binding the elements at this level.
    Scan(Arc<String>, Option<MemIndexScanPredicate>),
}

impl MemIndexScanInstruction {
    /// Creates a new [MemIndexScanInstruction::Traverse].
    pub fn traverse() -> Self {
        MemIndexScanInstruction::Traverse(None)
    }

    /// Creates a new [MemIndexScanInstruction::Traverse] with the given predicate.
    pub fn traverse_with_predicate(predicate: impl Into<MemIndexScanPredicate>) -> Self {
        MemIndexScanInstruction::Traverse(Some(predicate.into()))
    }

    /// Creates a new [MemIndexScanInstruction::Scan].
    pub fn scan(variable: impl Into<Arc<String>>) -> Self {
        MemIndexScanInstruction::Scan(variable.into(), None)
    }

    /// Creates a new [MemIndexScanInstruction::Scan] with the given predicate.
    pub fn scan_with_predicate(
        variable: impl Into<Arc<String>>,
        predicate: impl Into<MemIndexScanPredicate>,
    ) -> Self {
        MemIndexScanInstruction::Scan(variable.into(), Some(predicate.into()))
    }

    /// Returns the scan variable (i.e., the variable to bind the results to) for this instruction.
    pub fn scan_variable(&self) -> Option<&str> {
        match self {
            MemIndexScanInstruction::Traverse(_) => None,
            MemIndexScanInstruction::Scan(variable, _) => Some(variable.as_str()),
        }
    }

    /// Returns the predicate for this instruction.
    pub fn predicate(&self) -> Option<&MemIndexScanPredicate> {
        match self {
            MemIndexScanInstruction::Traverse(predicate) => predicate.as_ref(),
            MemIndexScanInstruction::Scan(_, predicate) => predicate.as_ref(),
        }
    }

    /// Creates a new [MemIndexScanInstruction] that has no predicate, even if the original instruction
    /// contained a predicate.
    pub fn without_predicate(self) -> Self {
        match self {
            MemIndexScanInstruction::Traverse(_) => {
                MemIndexScanInstruction::Traverse(None)
            }
            MemIndexScanInstruction::Scan(variable, _) => {
                MemIndexScanInstruction::Scan(variable, None)
            }
        }
    }

    /// Creates a new [MemIndexScanInstruction] with the given new predicate.
    pub fn with_predicate(self, predicate: MemIndexScanPredicate) -> Self {
        match self {
            MemIndexScanInstruction::Traverse(_) => {
                MemIndexScanInstruction::Traverse(Some(predicate))
            }
            MemIndexScanInstruction::Scan(variable, _) => {
                MemIndexScanInstruction::Scan(variable, Some(predicate))
            }
        }
    }
}

impl MemIndexScanInstruction {
    /// Returns the [MemIndexScanInstruction] for reading the given [EncodedActiveGraph], also
    /// considering whether the graph name is bound to a `variable`.
    pub fn from_active_graph(
        active_graph: &EncodedActiveGraph,
        variable: Option<&Variable>,
    ) -> MemIndexScanInstruction {
        let instruction_with_predicate = |predicate: Option<MemIndexScanPredicate>| {
            if let Some(variable) = variable {
                MemIndexScanInstruction::Scan(
                    Arc::new(variable.as_str().to_owned()),
                    predicate,
                )
            } else {
                MemIndexScanInstruction::Traverse(predicate)
            }
        };

        match active_graph {
            EncodedActiveGraph::DefaultGraph => {
                let object_ids = BTreeSet::from([DEFAULT_GRAPH_ID]);
                instruction_with_predicate(Some(MemIndexScanPredicate::In(object_ids)))
            }
            EncodedActiveGraph::AllGraphs => instruction_with_predicate(None),
            EncodedActiveGraph::Union(graphs) => {
                let object_ids = BTreeSet::from_iter(graphs.iter().copied());
                if object_ids.is_empty() {
                    instruction_with_predicate(Some(MemIndexScanPredicate::False))
                } else {
                    instruction_with_predicate(Some(MemIndexScanPredicate::In(
                        object_ids,
                    )))
                }
            }
            EncodedActiveGraph::AnyNamedGraph => instruction_with_predicate(Some(
                MemIndexScanPredicate::Between(FIRST_OBJECT_ID, EncodedObjectId::MAX),
            )),
        }
    }
}

impl From<EncodedTermPattern> for MemIndexScanInstruction {
    fn from(value: EncodedTermPattern) -> Self {
        match value {
            EncodedTermPattern::ObjectId(object_id) => MemIndexScanInstruction::Traverse(
                Some(MemIndexScanPredicate::In(BTreeSet::from([object_id]))),
            ),
            EncodedTermPattern::Variable(var) => {
                MemIndexScanInstruction::Scan(Arc::new(var), None)
            }
        }
    }
}

/// A list of [MemIndexPruningPredicate], one for each element of a quad index.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemIndexPruningPredicates(pub [Option<MemIndexPruningPredicate>; 4]);

impl From<&MemIndexScanInstructions> for MemIndexPruningPredicates {
    fn from(value: &MemIndexScanInstructions) -> Self {
        let predicates = value
            .1
            .iter()
            .map(|i| {
                i.predicate()
                    .and_then(Option::<MemIndexPruningPredicate>::from)
            })
            .collect::<Vec<_>>();
        Self(predicates.try_into().expect("Should yield 4 predicates"))
    }
}

/// A pruning predicate is a simpler version of [MemIndexScanPredicate] that can be used for quickly pruning relevant
/// row groups.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MemIndexPruningPredicate {
    /// Always returns false.
    False,
    /// Checks whether the object id is in the given set.
    EqualTo(EncodedObjectId),
    /// Checks whether the object id is between the given object ids (end is inclusive).
    Between(EncodedObjectId, EncodedObjectId),
}

impl From<&MemIndexScanPredicate> for Option<MemIndexPruningPredicate> {
    fn from(value: &MemIndexScanPredicate) -> Self {
        match value {
            MemIndexScanPredicate::False => Some(MemIndexPruningPredicate::False),
            MemIndexScanPredicate::In(ids) => {
                let predicate = match ids.len() {
                    0 => MemIndexPruningPredicate::False,
                    1 => MemIndexPruningPredicate::EqualTo(*ids.first().unwrap()),
                    _ => MemIndexPruningPredicate::Between(
                        *ids.first().unwrap(),
                        *ids.last().unwrap(),
                    ),
                };
                Some(predicate)
            }
            MemIndexScanPredicate::Between(from, to) => {
                if from == to {
                    Some(MemIndexPruningPredicate::EqualTo(*from))
                } else {
                    Some(MemIndexPruningPredicate::Between(*from, *to))
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::index::{IndexComponents, ScanInstructions};
    use crate::memory::storage::scan_instructions::{
        MemIndexScanInstruction, MemIndexScanInstructions,
    };
    use std::sync::Arc;

    #[test]
    fn test_reorder_pattern_gspo() {
        let pattern = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Scan(Arc::new("G".to_string()), None),
            MemIndexScanInstruction::Scan(Arc::new("S".to_string()), None),
            MemIndexScanInstruction::Scan(Arc::new("P".to_string()), None),
            MemIndexScanInstruction::Scan(Arc::new("O".to_string()), None),
        ]);
        let reordered = pattern.reorder(IndexComponents::GSPO);
        assert_eq!(reordered, pattern);
    }

    #[test]
    fn test_reorder_pattern_gpos() {
        let pattern = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Scan(Arc::new("G".to_string()), None),
            MemIndexScanInstruction::Scan(Arc::new("S".to_string()), None),
            MemIndexScanInstruction::Scan(Arc::new("P".to_string()), None),
            MemIndexScanInstruction::Scan(Arc::new("O".to_string()), None),
        ]);

        assert_eq!(
            pattern.reorder(IndexComponents::GPOS).into_inner(),
            [
                MemIndexScanInstruction::Scan(Arc::new("G".to_string()), None),
                MemIndexScanInstruction::Scan(Arc::new("P".to_string()), None),
                MemIndexScanInstruction::Scan(Arc::new("O".to_string()), None),
                MemIndexScanInstruction::Scan(Arc::new("S".to_string()), None),
            ]
        );
    }

    #[test]
    fn test_reorder_pattern_gpos_to_gosp() {
        let pattern = MemIndexScanInstructions::new(
            IndexComponents::GPOS,
            [
                MemIndexScanInstruction::Scan(Arc::new("G".to_string()), None),
                MemIndexScanInstruction::Scan(Arc::new("P".to_string()), None),
                MemIndexScanInstruction::Scan(Arc::new("O".to_string()), None),
                MemIndexScanInstruction::Scan(Arc::new("S".to_string()), None),
            ],
        );

        assert_eq!(
            pattern.reorder(IndexComponents::GOSP).into_inner(),
            [
                MemIndexScanInstruction::Scan(Arc::new("G".to_string()), None),
                MemIndexScanInstruction::Scan(Arc::new("O".to_string()), None),
                MemIndexScanInstruction::Scan(Arc::new("S".to_string()), None),
                MemIndexScanInstruction::Scan(Arc::new("P".to_string()), None),
            ]
        );
    }
}
