use crate::index::{IndexComponents, IndexPermutations, QuadIndex, ScanInstructions};
use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::predicate_pushdown::{
    DynamicFilterScanPredicateSource, MemStoragePredicateExpr,
};
use crate::memory::storage::quad_index::MemQuadIndex;
use crate::memory::storage::quad_index_data::{MemRowGroup, RowGroupPruningResult};
use crate::memory::storage::scan_instructions::{
    MemIndexScanInstruction, MemIndexScanInstructions, MemIndexScanPredicate,
    MemIndexScanPredicateSource,
};
use crate::memory::storage::stream::MemIndexScanStream;
use datafusion::arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, RecordBatch, RecordBatchOptions,
};
use datafusion::arrow::compute::kernels::cmp::{eq, gt_eq, lt_eq};
use datafusion::arrow::compute::{and, filter, or};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::metrics::BaselineMetrics;
use itertools::{Itertools, repeat_n};
use rdf_fusion_model::{DFResult, TriplePattern, Variable};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;

/// The results emitted by the [MemQuadIndexScanIterator].
pub struct QuadIndexBatch {
    /// The number of rows in the batch.
    pub num_rows: usize,
    /// A mapping from column name to column data.
    pub columns: HashMap<String, Arc<dyn Array>>,
}

/// Matches a given pattern against a [MemQuadIndex]. The matches are returned as [QuadIndexBatch].
pub struct MemQuadIndexScanIterator<TIndexRef: IndexRef> {
    /// A reference to the index.
    state: ScanState<TIndexRef>,
}

impl<'index> MemQuadIndexScanIterator<DirectIndexRef<'index>> {
    /// Creates a new [MemQuadIndexScanIterator].
    pub fn new(
        index: &'index MemQuadIndex,
        instructions: MemIndexScanInstructions,
    ) -> Self {
        Self {
            state: ScanState::CollectRelevantRowGroups(
                DirectIndexRef(index),
                instructions,
                Vec::new(),
            ),
        }
    }
}

impl MemQuadIndexScanIterator<IndexRefInSet> {
    /// Creates a new [MemQuadIndexScanIterator].
    pub fn new_from_index_set(
        index_set: Arc<OwnedRwLockReadGuard<IndexPermutations<MemQuadIndex>>>,
        index: IndexComponents,
        instructions: MemIndexScanInstructions,
        dynamic_filters: Vec<Arc<dyn MemIndexScanPredicateSource>>,
    ) -> Self {
        Self {
            state: ScanState::CollectRelevantRowGroups(
                IndexRefInSet(index_set, index),
                instructions,
                dynamic_filters,
            ),
        }
    }
}

/// The state of the [MemQuadIndexScanIterator].
enum ScanState<TIndexRef: IndexRef> {
    /// Collecting all relevant [MemRowGroup]s in the index. This will copy a reference to all
    /// arrays and can thus drop the [TIndexRef] once this step is done.
    CollectRelevantRowGroups(
        TIndexRef,
        MemIndexScanInstructions,
        Vec<Arc<dyn MemIndexScanPredicateSource>>,
    ),
    /// Applying the filters and projections to every identified
    Scanning {
        /// The data to scan.
        data: Vec<MemRowGroup>,
        /// Contains the instructions to scan the index. These may differ from the instructions
        /// in [Self::CollectRelevantBatches] if the collecting process already evaluated parts of
        /// the filters. These filters become [None] and ideally, the index should only be scanned
        /// after identifying the batches (we prune the first and last batch if necessary). As a
        /// result, the iterator can simply copy the batches without any more filtering.
        instructions: [Option<MemIndexScanInstruction>; 4],
    },
    /// The scan is fined.
    Finished,
}

impl<TIndexRef: IndexRef> Iterator for MemQuadIndexScanIterator<TIndexRef> {
    type Item = DFResult<QuadIndexBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                ScanState::CollectRelevantRowGroups(
                    index_ref,
                    instructions,
                    dynamic_filters,
                ) => {
                    let pruning_result = match collect_relevant_row_groups(
                        index_ref,
                        instructions,
                        dynamic_filters,
                    ) {
                        Ok(result) => result,
                        Err(err) => return Some(Err(err)),
                    };

                    let instructions = match pruning_result.new_instructions {
                        None => instructions.inner().clone(),
                        Some(new_instructions) => new_instructions.into_inner(),
                    };

                    if pruning_result.row_groups.is_empty() {
                        self.state = ScanState::Finished;
                    } else {
                        self.state = ScanState::Scanning {
                            data: pruning_result.row_groups,
                            instructions: instructions.map(Some),
                        };
                    }
                }
                ScanState::Scanning { data, instructions } => {
                    assert!(!data.is_empty());

                    let next_row_group = data.remove(0);
                    let batch_size = next_row_group.len();
                    let batch = next_row_group.into_arrays();

                    let selection_vector =
                        Self::compute_selection_vector(&batch, instructions);

                    match selection_vector {
                        None => {
                            let columns = batch
                                .iter()
                                .zip(instructions.iter())
                                .filter_map(|(data, instruction)| match instruction {
                                    Some(MemIndexScanInstruction::Scan(name, _)) => {
                                        Some((
                                            name.as_str().to_owned(),
                                            Arc::clone(data) as Arc<dyn Array>,
                                        ))
                                    }
                                    _ => None,
                                })
                                .collect();

                            if data.is_empty() {
                                // This is the last iteration.
                                self.state = ScanState::Finished;
                            }

                            return Some(Ok(QuadIndexBatch {
                                num_rows: batch_size,
                                columns,
                            }));
                        }
                        Some(selection_vector) => {
                            let columns = batch
                                .iter()
                                .zip(instructions.iter())
                                .filter_map(|(data, instruction)| match instruction {
                                    Some(MemIndexScanInstruction::Scan(name, _)) => {
                                        Some((name.as_str().to_owned(), data))
                                    }
                                    _ => None,
                                })
                                .map(|(name, data)| {
                                    (
                                        name,
                                        filter(data.as_ref(), &selection_vector)
                                            .expect("Array length must match"),
                                    )
                                })
                                .collect::<HashMap<_, _>>();

                            if data.is_empty() {
                                // This is the last iteration.
                                self.state = ScanState::Finished;
                            }

                            // Don't return empty batches.
                            if selection_vector.true_count() == 0 {
                                continue;
                            }

                            return Some(Ok(QuadIndexBatch {
                                num_rows: selection_vector.true_count(),
                                columns,
                            }));
                        }
                    }
                }
                ScanState::Finished => {
                    return None;
                }
            }
        }
    }
}

/// Collects all relevant [`MemRowGroup`]s in the index, returning the
/// [`RowGroupPruningResult`].
fn collect_relevant_row_groups(
    index_ref: &dyn IndexRef,
    instructions: &MemIndexScanInstructions,
    dynamic_filters: &[Arc<dyn MemIndexScanPredicateSource>],
) -> DFResult<RowGroupPruningResult> {
    let instructions =
        combine_instructions_with_dynamic_filters(instructions, dynamic_filters)?;

    match index_ref.try_choose_better_index(&instructions) {
        None => {
            let index = index_ref.get_index();
            let index_data = index.data();
            Ok(index_data.prune_relevant_row_groups(&instructions))
        }
        Some(new_index) => {
            let components = new_index.get_index().components();
            let instructions = instructions.reorder(components);
            let index = new_index.get_index();
            let index_data = index.data();
            Ok(index_data.prune_relevant_row_groups(&instructions))
        }
    }
}

fn combine_instructions_with_dynamic_filters(
    instructions: &MemIndexScanInstructions,
    dynamic_filters: &[Arc<dyn MemIndexScanPredicateSource>],
) -> DFResult<MemIndexScanInstructions> {
    // Filter out any dynamic filters that are not supported by the index.
    let supported_filters = dynamic_filters
        .iter()
        .flat_map(|s| s.current_predicate_expr().ok())
        .collect_vec();

    if supported_filters.is_empty() {
        return Ok(instructions.clone());
    }

    let mut new_instructions = instructions.clone();
    for predicate in supported_filters {
        new_instructions = new_instructions.apply_filter(&predicate)?;
    }

    Ok(new_instructions)
}

impl<TIndexRef: IndexRef> MemQuadIndexScanIterator<TIndexRef> {
    fn compute_selection_vector(
        data: &[Arc<FixedSizeBinaryArray>; 4],
        instructions: &[Option<MemIndexScanInstruction>; 4],
    ) -> Option<BooleanArray> {
        data.iter()
            .zip(instructions.iter())
            .filter_map(|(array, instruction)| {
                instruction
                    .as_ref()
                    .and_then(|i| i.predicate())
                    .map(|p| (array, p))
            })
            .flat_map(|(array, predicate)| {
                Self::apply_predicate(data, instructions, array, predicate)
            })
            .reduce(|lhs, rhs| and(&lhs, &rhs).expect("Array length must match"))
    }

    /// Applies the `predicate` to the `data`, returning a boolean array that indicates which
    /// elements match the predicate. This array can then be passed into a filter function.
    ///
    /// If [None] is returned, no predicates need to be applied and the entire data array can be
    /// considered as matching.
    ///
    /// We assume that the number of elements in the sets is relatively small. Therefore, doing
    /// vectorized comparisons and merging the resulting arrays is more performant as iterating
    /// over the array and consulting the set. In the future, one could check the size of the
    /// set and switch to the different strategy for large predicates.
    fn apply_predicate(
        all_data: &[Arc<FixedSizeBinaryArray>; 4],
        instructions: &[Option<MemIndexScanInstruction>; 4],
        data: &FixedSizeBinaryArray,
        predicate: &MemIndexScanPredicate,
    ) -> Option<BooleanArray> {
        match predicate {
            MemIndexScanPredicate::In(ids) => ids
                .iter()
                .map(|id| {
                    eq(
                        data,
                        &ScalarValue::FixedSizeBinary(
                            EncodedObjectId::SIZE_I32,
                            Some(id.as_bytes().to_vec()),
                        )
                        .to_scalar()
                        .unwrap(),
                    )
                    .expect("Array length must match, Data Types match")
                })
                .reduce(|lhs, rhs| or(&lhs, &rhs).expect("Array length must match")),
            MemIndexScanPredicate::EqualTo(name) => {
                let index = instructions.iter().position(|i| match i {
                    Some(MemIndexScanInstruction::Scan(var, _)) => var == name,
                    _ => false,
                })?;
                Some(
                    eq(all_data[index].as_ref(), data)
                        .expect("Array length must match, Data Types match"),
                )
            }
            MemIndexScanPredicate::Between(from, to) => {
                let ge = gt_eq(
                    data,
                    &ScalarValue::FixedSizeBinary(
                        EncodedObjectId::SIZE_I32,
                        Some(from.as_bytes().to_vec()),
                    )
                    .to_scalar()
                    .expect("UInt32 can be converted to a Scalar"),
                )
                .expect("gt_eq supports UInt32");
                let le = lt_eq(
                    data,
                    &ScalarValue::FixedSizeBinary(
                        EncodedObjectId::SIZE_I32,
                        Some(to.as_bytes().to_vec()),
                    )
                    .to_scalar()
                    .expect("UInt32 can be converted to a Scalar"),
                )
                .expect("lt_eq supports UInt32");
                Some(and(&ge, &le).expect("Inputs are bools and of same length"))
            }
            MemIndexScanPredicate::False => {
                Some(repeat_n(Some(false), data.len()).collect())
            }
        }
    }
}

/// A [MemQuadIndexScanIterator] that uses an [IndexPermutations] to choose the index for scanning.
pub struct MemQuadIndexScanRecordBatchIterator {
    /// The schema of the result.
    schema: SchemaRef,
    /// The inner iterator.
    inner: MemQuadIndexScanIterator<IndexRefInSet>,
}

impl MemQuadIndexScanRecordBatchIterator {
    /// Creates a new [MemQuadIndexScanRecordBatchIterator].
    pub fn new(
        schema: SchemaRef,
        index_set: Arc<OwnedRwLockReadGuard<IndexPermutations<MemQuadIndex>>>,
        index: IndexComponents,
        instructions: MemIndexScanInstructions,
        dynamic_filters: Vec<Arc<dyn MemIndexScanPredicateSource>>,
    ) -> Self {
        let instructions = instructions.reorder(index);
        let iterator = MemQuadIndexScanIterator::new_from_index_set(
            index_set,
            index,
            instructions.clone(),
            dynamic_filters,
        );
        MemQuadIndexScanRecordBatchIterator {
            schema,
            inner: iterator,
        }
    }
}

impl Iterator for MemQuadIndexScanRecordBatchIterator {
    type Item = DFResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = match self.inner.next()? {
            Ok(next) => next,
            Err(err) => return Some(Err(err)),
        };

        let reordered = reorder_result(&self.schema, next.columns);
        Some(Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            reordered,
            &RecordBatchOptions::new().with_row_count(Some(next.num_rows)),
        )
        .expect("Creates valid record batches")))
    }
}

/// Re-orders the given `pattern` for the given `components`.
fn reorder_result(
    schema: &Schema,
    columns: HashMap<String, Arc<dyn Array>>,
) -> Vec<Arc<dyn Array>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            Arc::clone(
                columns
                    .get(field.name())
                    .expect("Column must exist for scan"),
            )
        })
        .collect()
}

/// Encapsulates the state necessary for executing a pattern scan on a [MemQuadIndex].
///
/// See [PlannedIndexScan].
#[derive(Clone, Debug)]
pub struct PlannedPatternScan {
    /// The result schema.
    schema: SchemaRef,
    /// Holds a read lock on the index set.
    index_set: Arc<OwnedRwLockReadGuard<IndexPermutations<MemQuadIndex>>>,
    /// Which index to scan.
    index: IndexComponents,
    /// The instructions to scan the index.
    instructions: Box<MemIndexScanInstructions>,
    /// The graph variable. Used for printing the query plan.
    graph_variable: Option<Variable>,
    /// The triple pattern. Used for printing the query plan.
    pattern: Box<TriplePattern>,
    /// A list of dynamic filters that are applied to the scan.
    dynamic_filters: Vec<Arc<dyn MemIndexScanPredicateSource>>,
}

impl PlannedPatternScan {
    /// Creates a new [PlannedPatternScan].
    pub fn new(
        schema: SchemaRef,
        index_set: Arc<OwnedRwLockReadGuard<IndexPermutations<MemQuadIndex>>>,
        index: IndexComponents,
        instructions: Box<MemIndexScanInstructions>,
        graph_variable: Option<Variable>,
        pattern: Box<TriplePattern>,
    ) -> Self {
        Self {
            schema,
            index_set,
            index,
            instructions,
            graph_variable,
            pattern,
            dynamic_filters: vec![],
        }
    }

    /// Returns a reference to the graph variable.
    pub fn graph_variable(&self) -> Option<&Variable> {
        self.graph_variable.as_ref()
    }

    /// Returns a reference to the [TriplePattern].
    pub fn pattern(&self) -> &TriplePattern {
        self.pattern.as_ref()
    }

    /// Returns a reference to the [IndexComponents] that is used to scan the index.
    pub fn selected_index(&self) -> &IndexComponents {
        &self.index
    }

    /// Applies the given `filter` to the scan.
    pub fn apply_filter(self, filter: &MemStoragePredicateExpr) -> DFResult<Self> {
        if let MemStoragePredicateExpr::Dynamic(filter) = filter {
            let dyn_filter = DynamicFilterScanPredicateSource::new(Arc::clone(filter));
            return Ok(self.with_dynamic_filter(Arc::new(dyn_filter)));
        }

        let new_instructions = self.instructions.apply_filter(filter)?;
        Ok(Self {
            instructions: Box::new(new_instructions),
            ..self
        })
    }

    /// Chooses the new index to scan based on the current instructions.
    pub fn try_find_better_index(self) -> DFResult<Self> {
        let index = self.index_set.choose_index(&self.instructions);
        Ok(Self { index, ..self })
    }

    /// Executes the pattern scan and return the [SendableRecordBatchStream] that implements the
    /// scan. The resulting stream will be cooperative.
    pub fn create_stream(self, metrics: BaselineMetrics) -> SendableRecordBatchStream {
        let iterator = MemQuadIndexScanRecordBatchIterator::new(
            Arc::clone(&self.schema),
            self.index_set,
            self.index,
            *self.instructions,
            self.dynamic_filters,
        );
        Box::pin(cooperative(MemIndexScanStream::new(
            self.schema,
            iterator,
            metrics,
        )))
    }

    /// Returns a new [PlannedPatternScan] with the given `filter`.
    fn with_dynamic_filter(
        mut self,
        filter: Arc<dyn MemIndexScanPredicateSource>,
    ) -> PlannedPatternScan {
        self.dynamic_filters.push(filter);
        self
    }
}

impl Display for PlannedPatternScan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] ", self.index)?;

        if let Some(graph_variable) = &self.graph_variable {
            write!(f, "graph={graph_variable}, ")?;
        }

        let pattern = &self.pattern;
        write!(f, "subject={}", pattern.subject)?;
        write!(f, ", predicate={}", pattern.predicate)?;
        write!(f, ", object={}", pattern.object)?;

        let mut additional_filters = self
            .instructions
            .inner()
            .iter()
            .filter(|i| i.scan_variable().is_some() && i.predicate().is_some())
            .map(|i| format!("{} {}", i.scan_variable().unwrap(), i.predicate().unwrap()))
            .collect::<Vec<String>>();
        additional_filters.extend(self.dynamic_filters.iter().map(|f| f.to_string()));

        if !additional_filters.is_empty() {
            write!(
                f,
                ", additional_filters=[{}]",
                additional_filters.join(", ")
            )?;
        }

        Ok(())
    }
}

/// A reference to a [MemQuadIndex].
pub trait IndexRef {
    /// Returns a reference to the index.
    fn get_index(&self) -> &MemQuadIndex;

    /// Tries to choose a better index based on the given `instructions`.
    fn try_choose_better_index(
        &self,
        instructions: &MemIndexScanInstructions,
    ) -> Option<Arc<dyn IndexRef>>;
}

/// Reference to an index in a locked [IndexSet] with its [IndexComponents]. The
/// [IndexComponents] uniquely identifier an index within an [IndexSet].
pub struct IndexRefInSet(
    Arc<OwnedRwLockReadGuard<IndexPermutations<MemQuadIndex>>>,
    IndexComponents,
);

impl IndexRef for IndexRefInSet {
    fn get_index(&self) -> &MemQuadIndex {
        self.0.find_index(self.1).expect("Index must exist")
    }

    fn try_choose_better_index(
        &self,
        instructions: &MemIndexScanInstructions,
    ) -> Option<Arc<dyn IndexRef>> {
        let index = self.0.choose_index(instructions);

        if index == self.1 {
            None
        } else {
            Some(Arc::new(IndexRefInSet(Arc::clone(&self.0), index)))
        }
    }
}

/// Directly references a [MemQuadIndex].
pub struct DirectIndexRef<'index>(&'index MemQuadIndex);

impl IndexRef for DirectIndexRef<'_> {
    fn get_index(&self) -> &MemQuadIndex {
        self.0
    }

    fn try_choose_better_index(
        &self,
        _instructions: &MemIndexScanInstructions,
    ) -> Option<Arc<dyn IndexRef>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{EncodedQuad, IndexComponents};
    use crate::memory::MemObjectIdMapping;
    use crate::memory::object_id::EncodedObjectId;
    use crate::memory::storage::predicate_pushdown::MemStoragePredicateExpr;
    use crate::memory::storage::quad_index::MemIndexConfiguration;
    use rdf_fusion_encoding::object_id::{ObjectIdEncoding, ObjectIdMapping};
    use std::collections::{BTreeSet, HashSet};
    use std::sync::Mutex;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_dynamic_filters() {
        // Create an index and insert test data
        let mapping = Arc::new(MemObjectIdMapping::new());
        let object_id_encoding = Arc::new(ObjectIdEncoding::new(
            Arc::clone(&mapping) as Arc<dyn ObjectIdMapping>
        ));
        let index = MemQuadIndex::new(MemIndexConfiguration {
            object_id_encoding,
            batch_size: 100,
            components: IndexComponents::GSPO,
        });
        let mut index = IndexPermutations::new(HashSet::new(), vec![index]);

        index
            .insert(&[
                quad(0, 1, 10, 100),
                quad(0, 2, 10, 100),
                quad(0, 2, 10, 200),
                quad(0, 3, 10, 100),
            ])
            .unwrap();
        let index = Arc::new(RwLock::new(index));

        let dynamic_filter = MockDynamicFilter::new(MemStoragePredicateExpr::Between(
            Arc::from("subject"),
            eid(1),
            eid(2),
        ));

        // Create scan instructions
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::traverse_with_predicate(MemIndexScanPredicate::In(
                BTreeSet::from([eid(0)]),
            )),
            MemIndexScanInstruction::scan_with_predicate(
                "subject".to_owned(),
                MemIndexScanPredicate::Between(eid(2), eid(3)),
            ),
            MemIndexScanInstruction::Traverse(None),
            MemIndexScanInstruction::traverse_with_predicate(MemIndexScanPredicate::In(
                BTreeSet::from([eid(200)]),
            )),
        ]);

        // Create iterator with the dynamic filter
        let mut iterator = MemQuadIndexScanIterator::new_from_index_set(
            Arc::new(index.read_owned().await),
            IndexComponents::GSPO,
            instructions.clone(),
            vec![Arc::clone(&dynamic_filter) as Arc<dyn MemIndexScanPredicateSource>],
        );

        let batch = iterator.next().unwrap().unwrap();
        assert_eq!(batch.num_rows, 1);
    }

    #[tokio::test]
    async fn test_collect_relevant_batches_dynamic_filters_choose_better_index() {
        let mapping = Arc::new(MemObjectIdMapping::new());
        let object_id_encoding = Arc::new(ObjectIdEncoding::new(
            Arc::clone(&mapping) as Arc<dyn ObjectIdMapping>
        ));

        // Create an index and insert test data
        let gspo_index = MemQuadIndex::new(MemIndexConfiguration {
            object_id_encoding: Arc::clone(&object_id_encoding),
            batch_size: 100,
            components: IndexComponents::GSPO,
        });
        let gosp_index = MemQuadIndex::new(MemIndexConfiguration {
            object_id_encoding,
            batch_size: 100,
            components: IndexComponents::GOSP,
        });
        let mut index =
            IndexPermutations::new(HashSet::new(), vec![gspo_index, gosp_index]);

        index.insert(&[quad(0, 1, 10, 100)]).unwrap();
        let index = Arc::new(RwLock::new(index));

        // Create a dynamic filter that starts with "True" (matches everything)
        let dynamic_filter = MockDynamicFilter::new(MemStoragePredicateExpr::Between(
            Arc::from("object"),
            eid(1),
            eid(200),
        ));

        // Create scan instructions
        let instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::traverse_with_predicate(MemIndexScanPredicate::In(
                BTreeSet::from([eid(0)]),
            )),
            MemIndexScanInstruction::Scan(Arc::new("subject".to_owned()), None),
            MemIndexScanInstruction::Scan(Arc::new("predicate".to_owned()), None),
            MemIndexScanInstruction::Scan(Arc::new("object".to_owned()), None),
        ]);

        // Create iterator with the dynamic filter
        let pruning_result = collect_relevant_row_groups(
            &IndexRefInSet(Arc::new(index.read_owned().await), IndexComponents::GSPO),
            &instructions,
            &[Arc::clone(&dynamic_filter) as Arc<dyn MemIndexScanPredicateSource>],
        )
        .unwrap();

        // Switches to GOSP because the dynamic filter filers the object component
        assert_eq!(
            pruning_result.new_instructions.unwrap().index_components(),
            IndexComponents::GOSP
        )
    }

    /// A mock implementation of IndexScanPredicateSource for testing.
    #[derive(Debug)]
    struct MockDynamicFilter {
        predicate: Mutex<MemStoragePredicateExpr>,
    }

    impl MockDynamicFilter {
        fn new(predicate: MemStoragePredicateExpr) -> Arc<Self> {
            Arc::new(Self {
                predicate: Mutex::new(predicate),
            })
        }
    }

    impl Display for MockDynamicFilter {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockDynamicFilter")
        }
    }

    impl MemIndexScanPredicateSource for MockDynamicFilter {
        fn current_predicate_expr(&self) -> DFResult<MemStoragePredicateExpr> {
            Ok(self.predicate.lock().unwrap().clone())
        }
    }

    fn quad(
        graph_name: u32,
        subject: u32,
        predicate: u32,
        object: u32,
    ) -> EncodedQuad<EncodedObjectId> {
        EncodedQuad {
            graph_name: eid(graph_name),
            subject: eid(subject),
            predicate: eid(predicate),
            object: eid(object),
        }
    }

    fn eid(id: u32) -> EncodedObjectId {
        EncodedObjectId::from(id)
    }
}
