use crate::index::{IndexComponents, IndexQuad, NamedGraphStorage, QuadIndex};
use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::quad_index_data::MemIndexData;
use crate::memory::storage::scan::{DirectIndexRef, MemQuadIndexScanIterator};
use crate::memory::storage::scan_instructions::{
    MemIndexPruningPredicate, MemIndexPruningPredicates, MemIndexScanInstructions,
};
use rdf_fusion_encoding::object_id::ObjectIdEncodingRef;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{Display, Formatter};

/// Holds the configuration for the index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemIndexConfiguration {
    /// The object id encoding.
    pub object_id_encoding: ObjectIdEncodingRef,
    /// The desired batch size. This iterator only provides a best-effort service for adhering to
    /// the batch size.
    pub batch_size: usize,
    /// Differentiates between multiple configurations (e.g., SPO, PSO).
    pub components: IndexComponents,
}

impl Display for MemIndexConfiguration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.components)
    }
}

/// Represents a single permutation of a quad index held in-memory. The index is sorted from left
/// to right.
///
/// Given the [MemIndexConfiguration] GPOS, the index could look like this:
/// ```text
/// ?graph   ?predicate  ?object  ?subject
/// ┌─────┐    ┌─────┐   ┌─────┐   ┌─────┐
/// │   0 │    │   1 │   │   4 │   │   4 │
/// ├─────┤    ├─────┤   ├─────┤   ├─────┤
/// │   0 │    │   1 │   │   7 │   │   7 │
/// ├─────┤    ├─────┤   ├─────┤   ├─────┤
/// │   0 │    │   2 │   │   1 │   │   1 │
/// ├─────┤    ├─────┤   ├─────┤   ├─────┤
/// │ ... │    │ ... │   │ ... │   │ ... │
/// └─────┘    └─────┘   └─────┘   └─────┘
/// ```
///
/// The physical representation of the index in detaield in [MemIndexData].
#[derive(Debug)]
pub struct MemQuadIndex {
    /// The index content.
    data: MemIndexData,
    /// The configuration of the index.
    configuration: MemIndexConfiguration,
}

impl MemQuadIndex {
    /// Creates a new [MemQuadIndex].
    pub fn new(configuration: MemIndexConfiguration) -> Self {
        let nullable_position = configuration
            .components
            .inner()
            .iter()
            .position(|c| c.gspo_index() == 0)
            .expect("There has to be a graph name");
        Self {
            data: MemIndexData::new(configuration.batch_size, nullable_position),
            configuration,
        }
    }

    /// Returns a reference to the content of the index.
    pub(super) fn data(&self) -> &MemIndexData {
        &self.data
    }

    /// Creates a new iterator give the given scan `instructions`.
    pub fn scan_quads(
        &self,
        instructions: MemIndexScanInstructions,
    ) -> MemQuadIndexScanIterator<DirectIndexRef<'_>> {
        MemQuadIndexScanIterator::new(self, instructions)
    }
}

impl QuadIndex for MemQuadIndex {
    type Term = EncodedObjectId;
    type NamedGraphStorage = HashSet<EncodedObjectId>;
    type ScanInstructions = MemIndexScanInstructions;

    fn components(&self) -> IndexComponents {
        self.configuration.components
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn compute_scan_score(&self, instructions: &Self::ScanInstructions) -> usize {
        let pruning_predicates = MemIndexPruningPredicates::from(instructions);
        let mut score = 0;

        for (i, predicate) in pruning_predicates.0.iter().enumerate() {
            let Some(predicate) = predicate else {
                break;
            };

            let potent = (instructions.inner().len() - i) * 2;
            let reward = match predicate {
                MemIndexPruningPredicate::EqualTo(_) => 2,
                MemIndexPruningPredicate::Between(left, right) => {
                    if left == right {
                        2
                    } else {
                        1
                    }
                }
                MemIndexPruningPredicate::False => return usize::MAX,
            };

            score += reward << potent;

            // While we can prune at the Between level, the inner results cannot be pruned yet.
            if matches!(predicate, MemIndexPruningPredicate::Between(_, _)) {
                break;
            }
        }

        score
    }

    fn insert(
        &mut self,
        quads: impl IntoIterator<Item = IndexQuad<EncodedObjectId>>,
    ) -> usize {
        let mut to_insert: Vec<_> = quads.into_iter().collect();
        if to_insert.is_empty() {
            return 0;
        }

        to_insert.sort_unstable();
        to_insert.dedup();

        let to_insert_set: BTreeSet<_> = to_insert.into_iter().collect();

        self.data.insert(&to_insert_set)
    }

    fn remove(
        &mut self,
        quads: impl IntoIterator<Item = IndexQuad<EncodedObjectId>>,
    ) -> usize {
        let mut to_insert = BTreeSet::new();

        for quad in quads {
            to_insert.insert(quad);
        }

        self.data.remove(&to_insert)
    }

    fn clear(&mut self) {
        self.data = MemIndexData::new(
            self.configuration.batch_size,
            self.data.nullable_position(),
        );
    }

    fn clear_graph(&mut self, graph_name: Self::Term) {
        let index = self.data.nullable_position();
        self.data.clear_all_with_value_in_column(graph_name, index);
    }
}

impl NamedGraphStorage for HashSet<EncodedObjectId> {
    type Term = EncodedObjectId;

    fn contains(&self, graph_name: &Self::Term) -> bool {
        self.contains(graph_name)
    }

    fn iter(&self) -> impl Iterator<Item = Self::Term> {
        self.iter().copied()
    }

    fn insert(&mut self, graph_name: Self::Term) -> bool {
        if graph_name.is_default_graph() {
            return false;
        }
        self.insert(graph_name)
    }

    fn remove(&mut self, graph_name: &Self::Term) -> bool {
        self.remove(graph_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemObjectIdMapping;
    use crate::memory::object_id::EncodedObjectId;
    use crate::memory::storage::scan_instructions::{
        MemIndexScanInstruction, MemIndexScanInstructions, MemIndexScanPredicate,
    };
    use rdf_fusion_encoding::object_id::{ObjectIdEncoding, ObjectIdMapping};
    use std::sync::Arc;

    #[test]
    fn test_in_predicate_better_than_nothing() {
        let idx = make_index();

        let eq = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Scan(
                Arc::new("g".to_string()),
                Some(
                    MemIndexScanPredicate::In([EncodedObjectId::from(10)].into()).into(),
                ),
            ),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let nothing = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let eq_score = idx.compute_scan_score(&eq);
        let nothing_score = idx.compute_scan_score(&nothing);

        assert!(
            eq_score > nothing_score,
            "EqualTo should provide a better scan score than nothing"
        );
    }

    #[test]
    fn test_in_predicate_following_none_equal_to_nothing() {
        let idx = make_index();

        let eq = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Scan(
                Arc::new("g".to_string()),
                Some(
                    MemIndexScanPredicate::In([EncodedObjectId::from(10)].into()).into(),
                ),
            ),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let nothing = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let eq_score = idx.compute_scan_score(&eq);
        let nothing_score = idx.compute_scan_score(&nothing);

        assert_eq!(
            eq_score, nothing_score,
            "In should provide a better scan score than nothing"
        );
    }

    #[test]
    fn test_in_predicate_better_than_between() {
        let idx = make_index();
        let instructions_eq = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Scan(
                Arc::new("g".to_string()),
                Some(
                    MemIndexScanPredicate::In([EncodedObjectId::from(10)].into()).into(),
                ),
            ),
            MemIndexScanInstruction::Scan(
                Arc::new("s".to_string()),
                Some(
                    MemIndexScanPredicate::In([EncodedObjectId::from(10)].into()).into(),
                ),
            ),
            MemIndexScanInstruction::Scan(Arc::new("p".to_string()), None),
            MemIndexScanInstruction::Scan(Arc::new("o".to_string()), None),
        ]);

        let instructions_mixed = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Scan(
                Arc::new("g".to_string()),
                Some(MemIndexScanPredicate::EqualTo(Arc::new("x".to_string())).into()),
            ),
            MemIndexScanInstruction::Scan(
                Arc::new("s".to_string()),
                Some(
                    MemIndexScanPredicate::Between(
                        EncodedObjectId::from(1),
                        EncodedObjectId::from(10),
                    )
                    .into(),
                ),
            ),
            MemIndexScanInstruction::Scan(Arc::new("p".to_string()), None),
            MemIndexScanInstruction::Scan(Arc::new("o".to_string()), None),
        ]);

        let eq_score = idx.compute_scan_score(&instructions_eq);
        let mixed_score = idx.compute_scan_score(&instructions_mixed);

        assert!(
            eq_score > mixed_score,
            "Full EqualTo score should be higher than mixed"
        );
    }

    fn make_index() -> MemQuadIndex {
        let mapping = Arc::new(MemObjectIdMapping::new());
        let object_id_encoding = Arc::new(ObjectIdEncoding::new(
            Arc::clone(&mapping) as Arc<dyn ObjectIdMapping>
        ));
        let config = MemIndexConfiguration {
            object_id_encoding,
            batch_size: 128,
            components: IndexComponents::GSPO,
        };
        MemQuadIndex::new(config)
    }
}
