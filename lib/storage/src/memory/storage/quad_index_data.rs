use crate::index::IndexQuad;
use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::scan_instructions::{
    MemIndexPruningPredicate, MemIndexPruningPredicates, MemIndexScanInstructions,
    MemIndexScanPredicate,
};
use datafusion::arrow::array::{Array, ArrayDataBuilder, FixedSizeBinaryArray};
use datafusion::arrow::buffer::{Buffer, NullBuffer};
use datafusion::arrow::datatypes::DataType;
use itertools::Itertools;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Contains the data of a [MemQuadIndex](super::MemQuadIndex). This is the physical layout of the
/// index. Analogous to the [MemQuadIndex](super::MemQuadIndex), a single [MemIndexData] represents
/// exactly one permutation of the quad components. Furthermore, all RDF terms are represented as
/// [EncodedObjectId]s.
///
/// The physical layout of the [MemIndexData] is inspired by [Apache Parquet](https://parquet.apache.org/).
/// Therefore, we also adopt its terminology for row groups and column chunks.
///
/// The physical layout consists of four columns, one for each quad component. The entire index is
/// sorted from the left to the right. Furthermore, the index is partitioned into N row groups, each
/// of which should hold ideal [Self::row_group_size] elements. The batch size is usually set to the
/// default batch size of DataFusion. The part of a column within a row group is called a column
/// chunk.
///
/// The following illustration shows the physical layout of the index. Here we assume that the index
/// represents the GPOS permutation (any other permutation would be fine as well).
///
/// ```text
///              ?graph   ?predicate  ?object  ?subject
///               ┌─────┐    ┌─────┐   ┌─────┐   ┌─────┐
///               │   0 │    │   1 │   │   4 │   │  10 │
///               ├─────┤    ├─────┤   ├─────┤   ├─────┤
///               │   0 │    │   1 │   │   7 │   │  10 │
/// Row Group 0   ├─────┤    ├─────┤   ├─────┤   ├─────┤
///               │   0 │    │   2 │   │   1 │   │  10 │
///               ├─────┤    ├─────┤   ├─────┤   ├─────┤
///               │   0 │    │   2 │   │   3 │   │  10 │
///               └─────┘    └─────┘   └─────┘   └─────┘
///
///               ┌─────┐    ┌─────┐   ┌─────┐   ┌─────┐
///               │   0 │    │   2 │   │   4 │   │  10 │
///               ├─────┤    ├─────┤   ├─────┤   ├─────┤
///               │   0 │    │   2 │   │   4 │   │  20 │
/// Row Group 1   ├─────┤    ├─────┤   ├─────┤   ├─────┤
///               │   1 │    │   4 │   │   1 │   │  20 │
///               ├─────┤    ├─────┤   ├─────┤   ├─────┤
///               │ ... │    │ ... │   │ ... │   │ ... │
///               └─────┘    └─────┘   └─────┘   └─────┘
/// ```
///
/// The sorted nature of the index allows us to:
/// - relatively quickly check whether a quad is contained in the index
/// - efficiently scan the index for predicates that select a slice of the index (see [MemQuadIndexScanIterator](super::MemQuadIndexScanIterator)
///   for further details)
#[derive(Debug)]
pub(super) struct MemIndexData {
    /// Indicates which column is allowed to contain nullable data (i.e., the graph name)
    nullable_position: usize,
    /// The target row group size
    row_group_size: usize,
    /// The vector of [MemRowGroup].
    row_groups: Vec<MemRowGroup>,
}

/// The result of [MemIndexData::prune_relevant_row_groups].
///
/// During this task, some filters are already completely applied, and re-applying them would be
/// unnecessary overhead. Therefore, the result includes [Self::new_instructions] to signal
/// that some filters have become obsolete. Note that applying the old instructions will still be
/// correct.
pub(super) struct RowGroupPruningResult {
    /// The row groups that may contain quads that match the given instructions.
    ///
    /// Represents a *logically* contiguous slice of the index.
    pub row_groups: Vec<MemRowGroup>,
    /// The new instructions that should be applied to the index.
    ///
    /// It could be possible that the pruning step can already guarantee that a filter will match
    /// every row in the result. These instructions are then removed from the result to avoid
    /// redundant applications of the filter.
    pub new_instructions: Option<MemIndexScanInstructions>,
}

impl MemIndexData {
    /// Creates a new [IndexColumn].
    pub fn new(batch_size: usize, nullable_position: usize) -> Self {
        Self {
            nullable_position,
            row_group_size: batch_size,
            row_groups: Vec::new(),
        }
    }

    /// Returns which column of this index is nullable
    pub fn nullable_position(&self) -> usize {
        self.nullable_position
    }

    /// Returns the number of elements in the index.
    pub fn len(&self) -> usize {
        self.row_groups
            .iter()
            .map(|row_group| row_group.len())
            .sum()
    }

    /// Finds the range of this index that these instructions could match.
    ///
    /// This handles two tasks.
    /// 1. Search all row groups that may contain quads that match the given instructions
    /// 2. If necessary, slice the first and last row group such that some filters must not be
    ///    evaluated during the actual scan.
    ///
    /// You can think of this approach as narrowing in two pointers that point to the start and
    /// end of interesting quads. Here is an example:
    ///
    /// Suppose that the predicates `[In(0), In(2), In(4), In(10)]` are part of the scan. The
    /// figure below shows how the pointers are narrowing. It starts by having the initial pointer
    /// point to the first and the last pointer point to the last quad. Then, after checking the
    /// first predicate against the data, the last pointer is narrowed to the last quad that has the
    /// value `0`. This is possible as the quads are sorted. This step is then applied for the
    /// remaining scan instructions. Once a column is encountered that does not have a predicate,
    /// the pruning stops. This is fine, as the filters will be applied in another step.
    ///
    /// ```text
    ///               ?graph   ?predicate  ?object  ?subject    Instructions: In(0), In(2), In(4), In(10)
    ///
    ///               ┌─────┐    ┌─────┐   ┌─────┐   ┌─────┐
    ///               │   0 │    │   1 │   │   4 │   │  10 │ ◄───── initial ─ first
    ///               ├─────┤    ├─────┤   ├─────┤   ├─────┤
    ///               │   0 │    │   1 │   │   7 │   │  10 │
    /// Row Group 0   ├─────┤    ├─────┤   ├─────┤   ├─────┤
    ///               │   0 │    │   2 │   │   1 │   │  10 │                   ◄───── second
    ///               ├─────┤    ├─────┤   ├─────┤   ├─────┤
    ///               │   0 │    │   2 │   │   3 │   │  10 │
    ///               └─────┘    └─────┘   └─────┘   └─────┘
    ///
    ///
    ///
    ///               ┌─────┐    ┌─────┐   ┌─────┐   ┌─────┐
    ///               │   0 │    │   2 │   │   4 │   │  10 │                            ◄───── third ─ fourth
    ///               ├─────┤    ├─────┤   ├─────┤   ├─────┤
    ///               │   0 │    │   2 │   │   4 │   │  20 │           ◄───── first ─ second ─ third
    /// Row Group 1   ├─────┤    ├─────┤   ├─────┤   ├─────┤
    ///               │   1 │    │   4 │   │   1 │   │  20 │
    ///               ├─────┤    ├─────┤   ├─────┤   ├─────┤
    ///               │ ... │    │ ... │   │ ... │   │ ... │ ◄───── initial
    ///               └─────┘    └─────┘   └─────┘   └─────┘
    /// ```
    ///
    /// The result also contains the new list of scan instructions. In the example above, we can
    /// already guarantee from the pruning steps that the predicates will match all the quads
    /// that remain between the from and to pointer. Therefore, we can eliminate them.
    pub fn prune_relevant_row_groups(
        &self,
        instructions: &MemIndexScanInstructions,
    ) -> RowGroupPruningResult {
        let pruning_predicates = MemIndexPruningPredicates::from(instructions);

        if pruning_predicates
            .0
            .contains(&Some(MemIndexPruningPredicate::False))
        {
            return RowGroupPruningResult {
                row_groups: Vec::new(),
                new_instructions: {
                    let new_instructions =
                        instructions.inner().clone().map(|i| i.without_predicate());
                    Some(MemIndexScanInstructions::new(
                        instructions.index_components(),
                        new_instructions,
                    ))
                },
            };
        }

        let mut relevant_row_groups = self.row_groups.clone();

        for (column_idx, predicate) in pruning_predicates.0.iter().enumerate() {
            // If there is no filter we abort and do the scan over the current row group set.
            let Some(predicate) = predicate else {
                break;
            };

            let (from_oid, to_oid) = match predicate {
                MemIndexPruningPredicate::EqualTo(oid) => (*oid, *oid),
                MemIndexPruningPredicate::Between(from, to) => (*from, *to),
                MemIndexPruningPredicate::False => unreachable!("Handled above"),
            };

            // Find the first row group for which the given id is not before the first value of the
            // row group.
            let first_relevant = relevant_row_groups
                .iter()
                .enumerate()
                .filter_map(|(row_group_idx, row_group)| {
                    let column_chunk = &row_group.column_chunks[column_idx];
                    match column_chunk.find_range_between(from_oid, to_oid) {
                        FindRangeResult::After => None,
                        result => Some((row_group_idx, result)),
                    }
                })
                .next();

            // No batch contains any relevant data
            let Some((first_row_group, first_range_result)) = first_relevant else {
                return RowGroupPruningResult {
                    row_groups: Vec::new(),
                    new_instructions: None,
                };
            };

            // All other results (e.g., NotContained) indicate that the given id is not contained
            // in the first batch. As a result, it won't be contained in any other batch.
            let FindRangeResult::Contained(from_idx, to_idx) = first_range_result else {
                return RowGroupPruningResult {
                    row_groups: Vec::new(),
                    new_instructions: None,
                };
            };

            let mut new_relevant_row_groups =
                vec![relevant_row_groups[first_row_group].slice(from_idx, to_idx)];

            // Find the end of relevant row groups. Can be skipped if the end of the first check
            // was before the end of the row group.
            if to_idx == relevant_row_groups[first_row_group].len() {
                for row_group in &relevant_row_groups[first_row_group + 1..] {
                    let column_chunk = &row_group.column_chunks[column_idx];
                    match column_chunk.find_range_between(from_oid, to_oid) {
                        FindRangeResult::Before => {
                            break;
                        }
                        FindRangeResult::Contained(from, to) => {
                            assert_eq!(
                                from, 0,
                                "From must be 0, otherwise early terminated"
                            );

                            // If the end of the range is before the end of the row group, slice and
                            // abort
                            if to < row_group.len() {
                                new_relevant_row_groups.push(row_group.slice(from, to));
                                break;
                            } else {
                                new_relevant_row_groups.push(row_group.clone());
                            }
                        }
                        FindRangeResult::After | FindRangeResult::NotContained(_) => {
                            unreachable!("Column is sorted")
                        }
                    }
                }
            }

            relevant_row_groups = new_relevant_row_groups;

            if from_oid != to_oid {
                break;
            }
        }

        let mut new_instructions = Vec::new();
        for instruction in instructions.inner() {
            match &instruction.predicate() {
                Some(MemIndexScanPredicate::In(ids)) => {
                    if ids.len() == 1 {
                        new_instructions.push(instruction.clone().without_predicate());
                    } else {
                        break;
                    }
                }
                Some(MemIndexScanPredicate::Between(from, to)) => {
                    new_instructions.push(instruction.clone().without_predicate());
                    if from != to {
                        // The between can be fully applied, but not the following predicates as
                        // they are multiple sorted regions.
                        break;
                    }
                }
                _ => break,
            }
        }

        let new_instructions = if new_instructions.is_empty() {
            None
        } else {
            let missing_instructions = &instructions.inner()[new_instructions.len()..];
            new_instructions.extend_from_slice(missing_instructions);
            Some(MemIndexScanInstructions::new(
                instructions.index_components(),
                new_instructions
                    .try_into()
                    .expect("Should yield 4 instructions"),
            ))
        };

        RowGroupPruningResult {
            row_groups: relevant_row_groups,
            new_instructions,
        }
    }

    /// Insert `to_insert` into the index.
    pub fn insert(&mut self, to_insert: &BTreeSet<IndexQuad<EncodedObjectId>>) -> usize {
        let mut count = 0;
        let mut row_group_idx = 0;
        let mut to_insert = to_insert.iter().peekable();

        while row_group_idx < self.row_groups.len() {
            let current_row_group = &mut self.row_groups[row_group_idx];

            let mut to_insert_row_group = BTreeSet::new();
            while let Some(current_quad) = to_insert.peek() {
                match current_row_group.find(current_quad) {
                    QuadFindResult::Before => {
                        to_insert_row_group.insert(to_insert.next().unwrap().clone());
                    }
                    QuadFindResult::Contained => {
                        // Skip to the next quad if already contained.
                        to_insert.next();
                    }
                    QuadFindResult::NotContained => {
                        to_insert_row_group.insert(to_insert.next().unwrap().clone());
                    }
                    QuadFindResult::After => {
                        // Stop collecting for this row group.
                        break;
                    }
                }
            }

            count += to_insert_row_group.len();

            if !to_insert_row_group.is_empty() {
                current_row_group.insert(to_insert_row_group);
            }

            row_group_idx += 1;
        }

        for chunk in to_insert.chunks(self.row_group_size).into_iter() {
            let chunk = chunk.collect::<Vec<_>>();
            let new_row_group = MemRowGroup::new(chunk);
            count += new_row_group.len();
            self.row_groups.push(new_row_group);
        }

        count
    }

    /// Removes the `to_remove` set of quads from the index.
    ///
    /// The method assumes that the [IndexQuad<EncodedObjectId>](IndexQuad<EncodedObjectId>) are ordered according to the
    /// components of the index.
    pub fn remove(&mut self, to_remove: &BTreeSet<IndexQuad<EncodedObjectId>>) -> usize {
        let mut count = 0;
        let mut row_group_idx = 0;
        let mut to_insert = to_remove.iter().peekable();

        while row_group_idx < self.row_groups.len() {
            let current_row_group = &mut self.row_groups[row_group_idx];

            let mut to_remove_row_group = BTreeSet::new();
            while let Some(current_quad) = to_insert.peek() {
                match current_row_group.find(current_quad) {
                    QuadFindResult::Before | QuadFindResult::NotContained => {
                        // Do nothing, the quad is not present.
                        to_insert.next();
                    }
                    QuadFindResult::Contained => {
                        to_remove_row_group.insert(to_insert.next().unwrap().clone());
                    }
                    QuadFindResult::After => {
                        // Stop collecting for this row group.
                        break;
                    }
                }
            }

            count += to_remove_row_group.len();
            current_row_group.remove(to_remove_row_group);

            if current_row_group.len() == 0 {
                self.row_groups.remove(row_group_idx);
            } else {
                row_group_idx += 1;
            }
        }

        // Remaining quads are not contained in any row group.

        count
    }

    /// Clears all quads that have the given `value` in the column `column_idx`.
    pub(crate) fn clear_all_with_value_in_column(
        &mut self,
        value: EncodedObjectId,
        column_idx: usize,
    ) {
        let mut row_group_idx = 0;
        while row_group_idx < self.row_groups.len() {
            let current_row_group = &mut self.row_groups[row_group_idx];

            current_row_group.remove(
                current_row_group
                    .quads()
                    .into_iter()
                    .filter(|q| q.0[column_idx] == value)
                    .collect(),
            );

            if current_row_group.len() == 0 {
                self.row_groups.remove(row_group_idx);
            } else {
                row_group_idx += 1;
            }
        }
    }
}

/// The result of finding a quad in a [MemRowGroup].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QuadFindResult {
    /// The quad is not part of the [MemRowGroup]. If this quad was inserted, its position
    /// would be before the row group.
    Before,
    /// The quad is contained in the [MemRowGroup].
    Contained,
    /// The quad is not contained in the [MemRowGroup]. If this quad was inserted, its position
    /// would be within the row group.
    NotContained,
    /// The quad is not part of the [MemRowGroup]. If this quad was inserted, its position
    /// would be after the row group.
    After,
}

/// In the following, we borrow terminology from [Apache Parquet](https://parquet.apache.org/), as
/// the data is organized similarly to their approach.
#[derive(Debug, Clone)]
pub(super) struct MemRowGroup {
    column_chunks: [MemColumnChunk; 4],
}

impl MemRowGroup {
    /// Creates a new [MemRowGroup] with the provided `quads`.
    ///
    /// Assumes that `quads` is sorted.
    pub fn new(quads: Vec<&IndexQuad<EncodedObjectId>>) -> Self {
        let column_chunks: [MemColumnChunk; 4] = (0..4)
            .map(|idx| {
                quads
                    .iter()
                    .map(|quad| quad.0[idx].as_bytes())
                    .collect::<Vec<_>>()
            })
            .map(MemColumnChunk::new)
            .collect::<Vec<_>>()
            .try_into()
            .expect("Should yield 4 columns");

        Self { column_chunks }
    }

    /// The length of this row group.
    pub fn len(&self) -> usize {
        self.column_chunks[3].len()
    }

    /// Inserts the given quads into this [MemRowGroup].
    ///
    /// This method may assume the following:
    /// - No quad is already contained in this row group
    pub fn insert(&mut self, mut quads: BTreeSet<IndexQuad<EncodedObjectId>>) {
        let mut new_quads = self.quads();
        new_quads.append(&mut quads);

        let new_data = Self::new(new_quads.iter().collect());
        self.column_chunks = new_data.column_chunks;
    }

    /// Removes the given quads from this [MemRowGroup].
    ///
    /// This method may assume the following:
    /// - All quads are contained in this row group
    pub fn remove(&mut self, quads: BTreeSet<IndexQuad<EncodedObjectId>>) {
        let new_quads = self.quads();
        let difference = new_quads.difference(&quads);
        let new_data = Self::new(difference.collect());
        self.column_chunks = new_data.column_chunks;
    }

    /// Tries to find the given quad in this [MemRowGroup].
    ///
    /// See [QuadFindResult] for a list of possible outcomes.
    pub fn find(&self, quads: &IndexQuad<EncodedObjectId>) -> QuadFindResult {
        let mut from = 0;
        let mut to = self.len();

        for (chunk, id) in self.column_chunks.iter().zip(quads.0.iter()) {
            let chunk = chunk.slice(from, to);
            let (new_from, new_to) = match chunk.find_range(*id) {
                FindRangeResult::Before => {
                    return if from == 0 {
                        QuadFindResult::Before
                    } else {
                        QuadFindResult::NotContained
                    };
                }
                FindRangeResult::NotContained(_) => {
                    return QuadFindResult::NotContained;
                }
                FindRangeResult::Contained(found_from, found_to) => {
                    (from + found_from, from + found_to)
                }
                FindRangeResult::After => {
                    return if to == self.len() {
                        QuadFindResult::After
                    } else {
                        QuadFindResult::NotContained
                    };
                }
            };

            from = new_from;
            to = new_to;
        }

        debug_assert_eq!(from, to - 1, "Could not identify a single quad."); // to is exclusive
        QuadFindResult::Contained
    }

    /// Returns a [BTreeSet] of all quads in this [MemRowGroup].
    fn quads(&self) -> BTreeSet<IndexQuad<EncodedObjectId>> {
        let n = self.len();
        (0..n)
            .map(|i| {
                IndexQuad([
                    self.column_chunks[0].data.value(i).try_into().unwrap(),
                    self.column_chunks[1].data.value(i).try_into().unwrap(),
                    self.column_chunks[2].data.value(i).try_into().unwrap(),
                    self.column_chunks[3].data.value(i).try_into().unwrap(),
                ])
            })
            .collect()
    }

    /// Returns a new [MemRowGroup] that is a slice of this row group. The `to` index is exclusive.
    ///
    /// This is a low-cost operation. The buffers for the individual column chunks will not be
    /// copied.
    fn slice(&self, from: usize, to: usize) -> MemRowGroup {
        MemRowGroup {
            column_chunks: [
                self.column_chunks[0].slice(from, to),
                self.column_chunks[1].slice(from, to),
                self.column_chunks[2].slice(from, to),
                self.column_chunks[3].slice(from, to),
            ],
        }
    }

    /// Returns the [MemColumnChunk] arrays of this row group.
    pub fn into_arrays(self) -> [Arc<FixedSizeBinaryArray>; 4] {
        self.column_chunks.map(|c| c.data)
    }
}

/// Returns a range of indices that have the same value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum FindRangeResult {
    /// No element with the given value was found. If there had been an element with this value,
    /// it would be before this [ColumnChunk].
    Before,
    /// No element with the given value was found. If there had been an element with this value,
    /// it would be at the given index.
    NotContained(usize),
    /// There was at least one element with the given value. The entire range between the two
    /// indices has this value.
    Contained(usize, usize),
    /// No element with the given value was found. If there had been an element with this value,
    /// it would be after this [ColumnChunk].
    After,
}

#[derive(Debug, Clone)]
pub(super) struct MemColumnChunk {
    data: Arc<FixedSizeBinaryArray>,
}

impl MemColumnChunk {
    /// Creates a new [MemColumnChunk].
    pub fn new(data: Vec<[u8; 4]>) -> Self {
        let null = data
            .iter()
            .map(|slice| *slice != [0; 4])
            .collect::<NullBuffer>();

        let array_data = ArrayDataBuilder::new(DataType::FixedSizeBinary(4))
            .len(data.len())
            .add_buffer(Buffer::from(data.into_flattened()))
            .nulls((null.null_count() > 0).then_some(null))
            .build()
            .expect("Should yield a valid array");

        Self {
            data: Arc::new(FixedSizeBinaryArray::from(array_data)),
        }
    }

    /// The length of this column chunk.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Checks whether the given `value` is part of this [MemColumnChunk].
    ///
    /// See [FindRangeResult] for a list of possible results.
    pub fn find_range(&self, object_id: EncodedObjectId) -> FindRangeResult {
        self.find_range_between(object_id, object_id)
    }

    /// Checks whether any values in this [MemColumnChunk] fall between `from` and `to`.
    ///
    /// See [FindRangeResult] for a list of possible results.
    pub fn find_range_between(
        &self,
        from: EncodedObjectId,
        to: EncodedObjectId,
    ) -> FindRangeResult {
        // Fast path for before check
        let first = EncodedObjectId::from_4_byte_slice(self.data.value(0));
        if to < first {
            return FindRangeResult::Before;
        }

        // Fast path for after check
        let last =
            EncodedObjectId::from_4_byte_slice(self.data.value(self.data.len() - 1));
        if last < from {
            return FindRangeResult::After;
        }

        // Fast path for null handling
        let null_count = self.data.null_count();
        if from.is_default_graph() && to.is_default_graph() {
            if null_count == 0 {
                return FindRangeResult::Before;
            }
            return FindRangeResult::Contained(0, null_count);
        }

        let find_result = self
            .data
            .values()
            .chunks_exact(4)
            .map(EncodedObjectId::from_4_byte_slice)
            .position(|v| v >= from);
        let count_first_larger_value = match find_result {
            None => unreachable!("Should have been caught by fast path"),
            Some(position) => {
                let found_value = &self.data.values()[position * 4..position * 4 + 4];

                if position == 0 && first > to {
                    unreachable!("Should have been caught by fast path");
                } else if EncodedObjectId::from_4_byte_slice(found_value) > to {
                    return FindRangeResult::NotContained(position);
                } else {
                    position
                }
            }
        };

        let contained_count = self.data.values()[count_first_larger_value * 4..]
            .chunks_exact(4)
            .map(EncodedObjectId::from_4_byte_slice)
            .take_while(|v| *v <= to)
            .count();

        FindRangeResult::Contained(
            count_first_larger_value,
            count_first_larger_value + contained_count,
        )
    }

    /// Returns a new [MemColumnChunk] starting at index `from` and ending at index `to`
    /// (exclusive).
    ///
    /// This is an inexpensive operation. The buffer is not copied.
    fn slice(&self, from: usize, to: usize) -> MemColumnChunk {
        debug_assert!(from < to, "From must be smaller than to");
        let len = to - from;
        Self {
            data: Arc::new(self.data.slice(from, len)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::IndexQuad;
    use crate::memory::object_id::{DEFAULT_GRAPH_ID, EncodedObjectId};
    use crate::memory::storage::scan_instructions::{
        MemIndexScanInstruction, MemIndexScanPredicate,
    };
    use insta::assert_debug_snapshot;

    #[test]
    fn test_memcolumnchunk_slice_simple() {
        let chunk = create_mem_column_chunk(vec![Some(10), Some(20), Some(30), Some(40)]);
        assert_eq!(
            chunk.slice(1, 3).data.values().as_slice(),
            &[20u32.to_be_bytes(), 30u32.to_be_bytes()].concat()
        );
    }

    #[test]
    fn test_memcolumnchunk_slice_of_slice() {
        let chunk = create_mem_column_chunk(vec![
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
        ]);
        let mid_slice = chunk.slice(1, 5); // [6, 7, 8, 9]
        assert_eq!(
            mid_slice.slice(1, 3).data.values().as_slice(),
            &[7u32.to_be_bytes(), 8u32.to_be_bytes()].concat()
        );
    }

    #[test]
    fn test_empty_indexdata() {
        let index = MemIndexData::new(2, 0);
        assert_eq!(index.len(), 0);
        assert_eq!(index.row_groups.len(), 0);
    }

    #[test]
    fn test_insert_and_len_single_row_group() {
        let mut index = MemIndexData::new(4, 0);
        let items = quad_set([1, 2, 3]);
        index.insert(&items);

        assert_eq!(index.len(), 3);
        assert_eq!(index.row_groups.len(), 1);
        assert_eq!(index.row_groups[0].len(), 3);
    }

    #[test]
    fn test_insert_and_len_multiple_row_groups() {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([10, 20, 30, 40, 50]);
        index.insert(&items);

        assert_eq!(index.len(), 5);
        assert_eq!(index.row_groups.len(), 3);
        assert_eq!(index.row_groups[0].len(), 2);
        assert_eq!(index.row_groups[1].len(), 2);
        assert_eq!(index.row_groups[2].len(), 1);
    }

    #[test]
    fn test_insert_empty_set_no_effect() {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([]);
        index.insert(&items);

        assert_eq!(index.len(), 0);
        assert_eq!(index.row_groups.len(), 0);
    }

    #[test]
    fn test_inserting_multiple_batches_and_content() {
        let mut index = MemIndexData::new(3, 0);
        let items = quad_set([11, 12, 13, 14, 15, 16]);
        index.insert(&items);

        assert_eq!(index.row_groups.len(), 2);
        assert_eq!(index.row_groups[0].len(), 3);
        assert_eq!(index.row_groups[1].len(), 3);
    }

    #[test]
    fn test_inserting_duplicate_quads() {
        let mut index = MemIndexData::new(3, 0);
        let mut items = quad_set([1, 2, 3]);
        index.insert(&items);
        assert_eq!(index.len(), 3);

        // Insert overlapping items again
        items = quad_set([2, 3, 4]);
        index.insert(&items);

        assert_eq!(index.len(), 4);
    }

    #[test]
    fn test_nullable_indexdata_insert() {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([0, 1, 2]);
        index.insert(&items);
    }

    #[test]
    fn test_memrowgroup_insert_to_empty() {
        let quads: Vec<IndexQuad<EncodedObjectId>> =
            [10, 20, 30].into_iter().map(|i| quad(i)).collect();
        let mut group = MemRowGroup::new(vec![]);

        group.insert(quads.into_iter().collect());
        let arrays = group.clone().into_arrays();
        assert_eq!(
            &arrays[0].iter().collect_vec(),
            &[
                Some(10u32.to_be_bytes().as_slice()),
                Some(20u32.to_be_bytes().as_slice()),
                Some(30u32.to_be_bytes().as_slice())
            ]
        );
    }

    #[test]
    fn test_memrowgroup_insert_appends_to_existing() {
        // Insert after initial values, nothing overlaps
        let initial: Vec<IndexQuad<EncodedObjectId>> =
            [10, 20].into_iter().map(|i| quad(i)).collect();
        let mut group = MemRowGroup::new(initial.iter().collect());
        let new_quads: Vec<IndexQuad<EncodedObjectId>> =
            [30, 40].into_iter().map(|i| quad(i)).collect();

        group.insert(new_quads.into_iter().collect());
        let arrays = group.clone().into_arrays();
        assert_eq!(
            &arrays[0].iter().collect_vec(),
            &[
                Some(10u32.to_be_bytes().as_slice()),
                Some(20u32.to_be_bytes().as_slice()),
                Some(30u32.to_be_bytes().as_slice()),
                Some(40u32.to_be_bytes().as_slice())
            ]
        )
    }

    #[test]
    fn test_memrowgroup_insert_inserts_in_middle() {
        // Insert in the middle
        let initial: Vec<IndexQuad<EncodedObjectId>> =
            [10, 30].into_iter().map(|i| quad(i)).collect();
        let mut group = MemRowGroup::new(initial.iter().collect());
        let new_quads: Vec<IndexQuad<EncodedObjectId>> =
            [20].into_iter().map(|i| quad(i)).collect();

        group.insert(new_quads.into_iter().collect());
        let arrays = group.clone().into_arrays();
        assert_eq!(
            &arrays[0].iter().collect_vec(),
            &[
                Some(10u32.to_be_bytes().as_slice()),
                Some(20u32.to_be_bytes().as_slice()),
                Some(30u32.to_be_bytes().as_slice()),
            ]
        )
    }

    #[test]
    fn test_memrowgroup_insert_with_nulls() {
        let initial: Vec<IndexQuad<EncodedObjectId>> =
            [0, 2].into_iter().map(|i| quad(i)).collect();
        let mut group = MemRowGroup::new(initial.iter().collect());

        let new_quads: Vec<IndexQuad<EncodedObjectId>> =
            [1].into_iter().map(|i| quad(i)).collect();
        group.insert(new_quads.into_iter().collect());

        let arrays = group.clone().into_arrays();
        assert_eq!(
            &arrays[0].iter().collect_vec(),
            &[
                None,
                Some(1u32.to_be_bytes().as_slice()),
                Some(2u32.to_be_bytes().as_slice()),
            ]
        )
    }

    #[test]
    fn test_prune_empty_index() {
        let index = MemIndexData::new(2, 0);
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);

        assert!(relevant.row_groups.is_empty());
    }

    #[test]
    fn test_prune_no_filter_returns_all_groups() {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([1, 2, 3, 4]);
        index.insert(&items);

        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);
        assert_eq!(result.row_groups.len(), index.row_groups.len());
    }

    #[test]
    fn test_prune_filter_single_quad_present() {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([10, 20, 30, 40]);
        index.insert(&items);

        // Only filter first column, look for value 30 which should be in second row group
        let predicate = MemIndexScanPredicate::In([EncodedObjectId::from(30u32)].into());
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(predicate)),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), 1);
        assert!(
            result.new_instructions.unwrap().inner()[0]
                .predicate()
                .is_none()
        );
        assert_debug_snapshot!(result.row_groups[0], @r"
        MemRowGroup {
            column_chunks: [
                MemColumnChunk {
                    data: FixedSizeBinaryArray<4>
                    [
                      [
                        0,
                        0,
                        0,
                        30,
                    ],
                    ],
                },
                MemColumnChunk {
                    data: FixedSizeBinaryArray<4>
                    [
                      [
                        0,
                        0,
                        0,
                        30,
                    ],
                    ],
                },
                MemColumnChunk {
                    data: FixedSizeBinaryArray<4>
                    [
                      [
                        0,
                        0,
                        0,
                        30,
                    ],
                    ],
                },
                MemColumnChunk {
                    data: FixedSizeBinaryArray<4>
                    [
                      [
                        0,
                        0,
                        0,
                        30,
                    ],
                    ],
                },
            ],
        }
        ");
    }

    /// This test aims to test the following scenario:
    /// - 1st row group matches completely
    /// - 2nd row group matches partly
    /// - 3rd row group doesn't match
    ///
    /// It is important that the algorithm stops after the 2nd group (the partial match).
    #[test]
    fn test_prune_filter_partial_match_breaks_early() {
        let mut index = MemIndexData::new(5, 0);

        index.insert(
            &[
                // 1st row group
                quad_from_values(10, 10, 10, 10),
                quad_from_values(10, 10, 10, 11),
                quad_from_values(10, 10, 10, 12),
                quad_from_values(10, 10, 10, 13),
                quad_from_values(10, 10, 10, 14),
                // 2nd row group
                quad_from_values(10, 10, 10, 15),
                quad_from_values(10, 10, 10, 16),
                quad_from_values(10, 10, 10, 17),
                quad_from_values(10, 10, 10, 18),
                quad_from_values(20, 5, 5, 5),
                // 3rd row group
                quad_from_values(20, 5, 5, 6),
                quad_from_values(20, 5, 5, 7),
            ]
            .into_iter()
            .collect(),
        );

        let predicate = MemIndexScanPredicate::In([EncodedObjectId::from(10u32)].into());
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), 2);
        assert_eq!(result.row_groups[0].len(), 5);
        assert_eq!(result.row_groups[1].len(), 4);
        assert!(
            result.new_instructions.as_ref().unwrap().inner()[0]
                .predicate()
                .is_none()
        );
        assert!(
            result.new_instructions.as_ref().unwrap().inner()[1]
                .predicate()
                .is_none()
        );
    }

    #[test]
    fn test_prune_filter_breaks_early_on_last_row() {
        let mut index = MemIndexData::new(5, 0);

        index.insert(
            &[
                // 1st row group
                quad_from_values(10, 10, 10, 10),
                quad_from_values(10, 10, 10, 11),
                quad_from_values(10, 10, 10, 12),
                quad_from_values(10, 10, 10, 13),
                quad_from_values(11, 10, 10, 14),
                // 2nd row group
                quad_from_values(11, 10, 10, 15),
            ]
            .into_iter()
            .collect(),
        );

        let predicate = MemIndexScanPredicate::In([EncodedObjectId::from(10u32)].into());
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), 1);
        assert_eq!(result.row_groups[0].len(), 4);
        assert!(
            result.new_instructions.unwrap().inner()[0]
                .predicate()
                .is_none()
        );
    }

    #[test]
    fn test_prune_filter_multi_row_groups() {
        let mut index = MemIndexData::new(5, 0);

        index.insert(
            &[
                // 1st row group
                quad_from_values(0, 10, 10, 10),
                quad_from_values(0, 11, 10, 11),
                quad_from_values(0, 11, 10, 12),
                quad_from_values(0, 11, 10, 13),
                quad_from_values(0, 11, 10, 14),
                // 2nd row group
                quad_from_values(0, 11, 10, 21),
                quad_from_values(0, 11, 10, 22),
                quad_from_values(0, 11, 10, 23),
                quad_from_values(0, 11, 10, 24),
                quad_from_values(0, 11, 10, 25),
                // 3rd row group
                quad_from_values(0, 11, 10, 31),
                quad_from_values(0, 11, 12, 32),
            ]
            .into_iter()
            .collect(),
        );

        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(MemIndexScanPredicate::In(
                [EncodedObjectId::from(0)].into(),
            ))),
            MemIndexScanInstruction::Traverse(Some(MemIndexScanPredicate::In(
                [EncodedObjectId::from(11)].into(),
            ))),
            MemIndexScanInstruction::Traverse(Some(MemIndexScanPredicate::In(
                [EncodedObjectId::from(10)].into(),
            ))),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), 3);
        assert_eq!(result.row_groups[0].len(), 4);
        assert_eq!(result.row_groups[1].len(), 5);
        assert_eq!(result.row_groups[2].len(), 1);
    }

    #[test]
    fn test_prune_multiple_filters_start_fixed() {
        let mut index = MemIndexData::new(5, 0);

        index.insert(
            &[
                quad_from_values(10, 10, 10, 10),
                quad_from_values(10, 10, 12, 11),
                quad_from_values(10, 11, 12, 12),
                quad_from_values(20, 20, 20, 20),
            ]
            .into_iter()
            .collect(),
        );

        let predicate = MemIndexScanPredicate::In([EncodedObjectId::from(10u32)].into());
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), 1);
        assert_eq!(result.row_groups[0].len(), 1);
    }

    #[test]
    fn test_prune_multiple_filters_end_fixed() {
        let mut index = MemIndexData::new(5, 0);

        index.insert(
            &[
                quad_from_values(10, 9, 9, 10),
                quad_from_values(10, 10, 9, 10),
                quad_from_values(10, 10, 10, 10),
                quad_from_values(20, 20, 20, 20),
            ]
            .into_iter()
            .collect(),
        );

        let predicate = MemIndexScanPredicate::In([EncodedObjectId::from(10u32)].into());
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), 1);
        assert_eq!(result.row_groups[0].len(), 1);
    }

    #[test]
    fn test_prune_filter_single_quad_absent() {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([1, 2, 3, 4]);
        index.insert(&items);

        let predicate = MemIndexScanPredicate::In([EncodedObjectId::from(99u32)].into());
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(predicate)),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert!(result.row_groups.is_empty());
    }

    #[test]
    fn test_prune_filter_between_removes_current_instructions_but_retains_rest() {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([1, 2, 3, 4]);
        index.insert(&items);

        let predicate = MemIndexScanPredicate::Between(
            EncodedObjectId::from(1),
            EncodedObjectId::from(2),
        );
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(Some(predicate)),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), 1);
        let new_instructions = result.new_instructions.unwrap();
        assert!(new_instructions.inner()[0].predicate().is_none());
        assert!(new_instructions.inner()[1].predicate().is_some());
    }

    #[test]
    fn test_prune_filter_between_with_single_element_also_prunes_following_instructions()
    {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([1, 2, 3, 4]);
        index.insert(&items);

        let predicate = MemIndexScanPredicate::Between(
            EncodedObjectId::from(1),
            EncodedObjectId::from(1),
        );
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(predicate.clone())),
            MemIndexScanInstruction::Traverse(Some(predicate)),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), 1);
        let new_instructions = result.new_instructions.unwrap();
        assert!(new_instructions.inner()[0].predicate().is_none());
        assert!(new_instructions.inner()[1].predicate().is_none());
    }

    #[test]
    fn test_prune_relevant_row_groups_in_predicate_multiple_values_no_pruning() {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([1, 2, 3, 4]);
        index.insert(&items);

        let set = [EncodedObjectId::from(2u32), EncodedObjectId::from(3u32)];
        let predicate = MemIndexScanPredicate::In(set.into());
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(predicate)),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), index.row_groups.len());
    }

    #[test]
    fn test_prune_relevant_row_groups_in_empty_set_returns_empty_result() {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([1, 2, 3, 4]);
        index.insert(&items);

        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(MemIndexScanPredicate::In(
                BTreeSet::new(),
            ))),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), 0);
    }

    #[test]
    fn test_prune_relevant_row_groups_false_predicate_returns_empty_result() {
        let mut index = MemIndexData::new(2, 0);
        let items = quad_set([1, 2, 3, 4]);
        index.insert(&items);

        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::Traverse(Some(MemIndexScanPredicate::False)),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::Traverse(None),
        ]);

        let result = index.prune_relevant_row_groups(&instructions);

        assert_eq!(result.row_groups.len(), 0);
    }

    #[test]
    fn test_find_range_all_nulls() {
        let chunk = create_mem_column_chunk(vec![None, None, None]);
        let value = EncodedObjectId::from(0u32);
        let result = chunk.find_range(value);
        assert_eq!(result, FindRangeResult::Contained(0, 3));
    }

    #[test]
    fn test_find_range_nulls_before_data() {
        let chunk = create_mem_column_chunk(vec![None, None, Some(3), Some(5), Some(7)]);
        let result_null = chunk.find_range(EncodedObjectId::from(0u32));
        assert_eq!(result_null, FindRangeResult::Contained(0, 2));

        let result_val = chunk.find_range(EncodedObjectId::from(5u32));
        assert_eq!(result_val, FindRangeResult::Contained(3, 4));
    }

    #[test]
    fn test_find_range_value_present_single() {
        let chunk = create_mem_column_chunk(vec![Some(2), Some(4), Some(6)]);
        let result = chunk.find_range(EncodedObjectId::from(4u32));
        assert_eq!(result, FindRangeResult::Contained(1, 2));
    }

    #[test]
    fn test_find_range_value_present_multiple() {
        let chunk = create_mem_column_chunk(vec![Some(4), Some(4), Some(4), Some(5)]);
        let result = chunk.find_range(EncodedObjectId::from(4u32));
        assert_eq!(result, FindRangeResult::Contained(0, 3));
    }

    #[test]
    fn test_find_range_value_not_present_between() {
        let chunk = create_mem_column_chunk(vec![Some(1), Some(3), Some(5), Some(7)]);
        let result = chunk.find_range(EncodedObjectId::from(4u32));
        // 4 is not present, but would fall between 3 (idx 1) and 5 (idx 2)
        assert_eq!(result, FindRangeResult::NotContained(2));
    }

    #[test]
    fn test_find_range_value_too_small_and_too_big() {
        let chunk = create_mem_column_chunk(vec![Some(10), Some(20), Some(30)]);
        // Value smaller than any element
        let result_before = chunk.find_range(EncodedObjectId::from(2u32));
        assert_eq!(result_before, FindRangeResult::Before);

        // Value greater than any element
        let result_after = chunk.find_range(EncodedObjectId::from(50u32));
        assert_eq!(result_after, FindRangeResult::After);
    }

    #[test]
    fn test_find_quad_contained_complex() {
        let mut index = MemIndexData::new(5, 0);

        index.insert(
            &[
                quad_from_values(1, 2, 3, 4),
                quad_from_values(1, 5, 3, 4),
                quad_from_values(1, 5, 3, 6),
                quad_from_values(1, 7, 3, 8),
            ]
            .into_iter()
            .collect(),
        );

        let result = index.row_groups[0].find(&IndexQuad([
            EncodedObjectId::from(1),
            EncodedObjectId::from(5),
            EncodedObjectId::from(3),
            EncodedObjectId::from(4),
        ]));

        assert_eq!(QuadFindResult::Contained, result);
    }

    #[test]
    fn test_clear_all_with_value_in_column_removes_only_matching() {
        let mut index = MemIndexData::new(5, 0);

        index.insert(
            &[
                quad_from_values(1, 100, 3, 4),
                quad_from_values(2, 100, 3, 4),
                quad_from_values(3, 200, 3, 4),
                quad_from_values(4, 200, 3, 4),
            ]
            .into_iter()
            .collect(),
        );

        index.clear_all_with_value_in_column(EncodedObjectId::from(100), 1);

        assert_eq!(index.len(), 2);
        for value in index.row_groups[0].column_chunks[1].data.iter() {
            assert_ne!(value, Some(100u32.to_be_bytes().as_slice()));
        }
    }

    #[test]
    fn test_clear_all_with_value_in_column_no_match_leaves_index_unchanged() {
        let mut index = MemIndexData::new(3, 0);

        index.insert(
            &[
                quad_from_values(10, 20, 30, 40),
                quad_from_values(11, 22, 33, 44),
            ]
            .into_iter()
            .collect(),
        );

        index.clear_all_with_value_in_column(EncodedObjectId::from(99), 0);

        assert_eq!(index.len(), 2);
    }

    /// Creates a new [`MemColumnChunk`] from `u32`.
    fn create_mem_column_chunk(values: Vec<Option<u32>>) -> MemColumnChunk {
        MemColumnChunk::new(
            values
                .into_iter()
                .map(|opt| {
                    opt.map(|v| v.to_be_bytes())
                        .unwrap_or(DEFAULT_GRAPH_ID.as_bytes())
                })
                .collect(),
        )
    }

    /// Creates a quad where all four terms have the same u32 value
    fn quad(val: u32) -> IndexQuad<EncodedObjectId> {
        IndexQuad([
            EncodedObjectId::from(val),
            EncodedObjectId::from(val),
            EncodedObjectId::from(val),
            EncodedObjectId::from(val),
        ])
    }

    /// Creates a quad where all four terms have the same u32 value
    fn quad_from_values(
        val1: u32,
        val2: u32,
        val3: u32,
        val4: u32,
    ) -> IndexQuad<EncodedObjectId> {
        IndexQuad([
            EncodedObjectId::from(val1),
            EncodedObjectId::from(val2),
            EncodedObjectId::from(val3),
            EncodedObjectId::from(val4),
        ])
    }

    /// Creates a quad set from a set of u32 values. Each element in the quad will have the same
    /// value.
    fn quad_set<I: IntoIterator<Item = u32>>(
        vals: I,
    ) -> BTreeSet<IndexQuad<EncodedObjectId>> {
        vals.into_iter().map(quad).collect()
    }
}
