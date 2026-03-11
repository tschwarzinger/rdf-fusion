mod mem_storage;
mod pattern_data_source;
mod predicate_pushdown;
mod quad_index;
mod quad_index_data;
mod scan;
mod scan_instructions;
mod snapshot;
mod stream;

pub use mem_storage::MemQuadStorage;
pub use pattern_data_source::MemQuadPatternDataSource;
pub use snapshot::{MemQuadStorageSnapshot, PlanPatternScanResult};

#[cfg(test)]
mod tests {
    use crate::index::{
        EncodedQuad, IndexComponents, IndexPermutations, IndexQuad, QuadIndex,
    };
    use crate::memory::MemObjectIdMapping;
    use crate::memory::object_id::EncodedObjectId;
    use crate::memory::storage::quad_index::{MemIndexConfiguration, MemQuadIndex};
    use crate::memory::storage::scan::MemQuadIndexScanRecordBatchIterator;
    use crate::memory::storage::scan_instructions::{
        MemIndexScanInstruction, MemIndexScanInstructions, MemIndexScanPredicate,
    };
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use insta::assert_debug_snapshot;
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn insert_and_scan_triple() {
        let mut index = create_index();
        index.insert(vec![IndexQuad([eid(1), eid(2), eid(3), eid(4)])]);

        let mut iter = index.scan_quads(MemIndexScanInstructions::new_gspo([
            traverse(1),
            traverse(2),
            traverse(3),
            traverse(4),
        ]));
        let result = iter.next().unwrap().unwrap();

        assert_eq!(result.num_rows, 1);
    }

    #[tokio::test]
    async fn scan_returns_sorted_results_on_last_level() {
        let mut index = create_index();
        index.insert(vec![
            IndexQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexQuad([eid(1), eid(2), eid(3), eid(3)]),
        ]);

        let mut iter = index.scan_quads(MemIndexScanInstructions::new_gspo([
            traverse(1),
            traverse(2),
            traverse(3),
            scan("d"),
        ]));
        let result = iter.next().unwrap().unwrap();

        assert_debug_snapshot!(result.columns, @r#"
        {
            "d": FixedSizeBinaryArray<4>
            [
              [
                0,
                0,
                0,
                3,
            ],
              [
                0,
                0,
                0,
                4,
            ],
            ],
        }
        "#);
    }

    #[tokio::test]
    async fn scan_returns_sorted_results_on_intermediate_level() {
        let mut index = create_index();
        index.insert(vec![
            IndexQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexQuad([eid(1), eid(2), eid(2), eid(4)]),
        ]);

        let mut iter = index.scan_quads(MemIndexScanInstructions::new_gspo([
            traverse(1),
            traverse(2),
            scan("c"),
            traverse(4),
        ]));
        let result = iter.next().unwrap().unwrap();

        assert_debug_snapshot!(result.columns, @r#"
        {
            "c": FixedSizeBinaryArray<4>
            [
              [
                0,
                0,
                0,
                2,
            ],
              [
                0,
                0,
                0,
                3,
            ],
            ],
        }
        "#);
    }

    #[tokio::test]
    async fn scan_with_no_match() {
        let mut index = create_index();
        index.insert(vec![IndexQuad([eid(1), eid(2), eid(3), eid(4)])]);

        let result = index
            .scan_quads(MemIndexScanInstructions::new_gspo([
                traverse(2),
                scan("b"),
                traverse(3),
                traverse(4),
            ]))
            .next();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn scan_subject_var() {
        let mut index = create_index();
        index.insert(vec![
            IndexQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexQuad([eid(1), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            MemIndexScanInstructions::new_gspo([
                traverse(1),
                scan("b"),
                traverse(3),
                traverse(4),
            ]),
            1,
            2,
        );
    }

    #[tokio::test]
    async fn scan_predicate_var() {
        let mut index = create_index();
        index.insert(vec![
            IndexQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexQuad([eid(1), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            MemIndexScanInstructions::new_gspo([
                traverse(1),
                traverse(2),
                scan("c"),
                traverse(4),
            ]),
            1,
            1,
        );
    }

    #[tokio::test]
    async fn scan_object_var() {
        let mut index = create_index();
        index.insert(vec![
            IndexQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexQuad([eid(1), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            MemIndexScanInstructions::new_gspo([
                traverse(1),
                traverse(2),
                traverse(3),
                scan("d"),
            ]),
            1,
            1,
        );
    }

    #[tokio::test]
    async fn scan_multi_vars() {
        let mut index = create_index();
        index.insert(vec![
            IndexQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexQuad([eid(1), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            MemIndexScanInstructions::new_gspo([
                traverse(1),
                scan("b"),
                traverse(3),
                scan("d"),
            ]),
            2,
            2,
        );
    }

    #[tokio::test]
    async fn scan_all_vars() {
        let mut index = create_index();
        index.insert(vec![
            IndexQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexQuad([eid(1), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            MemIndexScanInstructions::new_gspo([
                scan("a"),
                scan("b"),
                scan("c"),
                scan("d"),
            ]),
            4,
            3,
        );
    }

    #[tokio::test]
    async fn scan_same_var_appearing_twice() {
        let mut index = create_index();
        index.insert(vec![
            IndexQuad([eid(1), eid(3), eid(3), eid(4)]),
            IndexQuad([eid(1), eid(2), eid(2), eid(4)]),
            IndexQuad([eid(1), eid(3), eid(2), eid(4)]),
        ]);

        run_matching_test(
            index,
            MemIndexScanInstructions::new_gspo([
                scan("a"),
                scan("same"),
                scan("same"),
                scan("d"),
            ]),
            3,
            2,
        );
    }

    #[tokio::test]
    async fn scan_considers_predicates() {
        let mut index = create_index();
        index.insert(vec![
            IndexQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexQuad([eid(2), eid(2), eid(5), eid(6)]),
            IndexQuad([eid(3), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            MemIndexScanInstructions::new_gspo([
                MemIndexScanInstruction::Scan(
                    Arc::new("a".to_owned()),
                    Some(MemIndexScanPredicate::In([eid(1), eid(3)].into()).into()),
                ),
                scan("b"),
                scan("c"),
                scan("d"),
            ]),
            4,
            2,
        );
    }

    #[tokio::test]
    async fn scan_batches_for_batch_size() {
        let mut index = create_index_with_batch_size(10);
        let mut quads = Vec::new();
        for i in 0..25 {
            quads.push(IndexQuad([eid(1), eid(2), eid(3), eid(i + 1)]))
        }
        index.insert(quads);

        // The lookup matches a single IndexData that will be scanned.
        run_batch_size_test(
            index,
            MemIndexScanInstructions::new_gspo([
                traverse(1),
                traverse(2),
                traverse(3),
                scan("d"),
            ]),
            &[10, 10, 5],
            true,
        );
    }

    #[tokio::test]
    async fn scan_multi_level_batches_coalesce_results() {
        let mut index = create_index_with_batch_size(10);
        let mut quads = Vec::new();
        for i in 0..25 {
            quads.push(IndexQuad([eid(1), eid(2), eid(i), eid(3)]))
        }
        index.insert(quads);

        // The lookup matches 25 different IndexLevels, each having exactly one data entry. The
        // batches should be combined into a single batch.
        run_batch_size_test(
            index,
            MemIndexScanInstructions::new_gspo([
                traverse(1),
                traverse(2),
                scan("c"),
                traverse(3),
            ]),
            &[10, 10, 5],
            true,
        );
    }

    #[tokio::test]
    async fn delete_triple_removes_it() {
        let mut index = create_index();
        let quads = vec![
            IndexQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexQuad([eid(1), eid(7), eid(3), eid(4)]),
        ];
        index.insert(quads.clone());
        index.remove(quads);

        run_non_matching_test(
            index,
            MemIndexScanInstructions::new_gspo([
                scan("a"),
                scan("b"),
                scan("c"),
                scan("d"),
            ]),
        );
    }

    #[tokio::test]
    async fn delete_triple_non_existing_returns_zero() {
        let mut index = create_index();
        let quads = vec![IndexQuad([eid(1), eid(2), eid(3), eid(4)])];
        let result = index.remove(quads);
        assert_eq!(result, 0);
    }

    #[test]
    fn try_and_with_false_predicate_returns_false() {
        let in_pred = MemIndexScanPredicate::In([eid(1), eid(2)].into());
        let between_pred = MemIndexScanPredicate::Between(eid(1), eid(5));
        let false_pred = MemIndexScanPredicate::False;

        // False with anything is False
        assert_eq!(
            in_pred.try_and_with(&false_pred),
            Some(MemIndexScanPredicate::False)
        );
        assert_eq!(
            false_pred.try_and_with(&between_pred),
            Some(MemIndexScanPredicate::False)
        );
    }

    #[test]
    fn try_and_with_in_and_in_intersects() {
        let a = MemIndexScanPredicate::In([eid(1), eid(2), eid(3)].into());
        let b = MemIndexScanPredicate::In([eid(2), eid(3), eid(4)].into());
        assert_eq!(
            a.try_and_with(&b),
            Some(MemIndexScanPredicate::In([eid(2), eid(3)].into()))
        );
    }

    #[test]
    fn try_and_with_in_and_in_disjoint_returns_false() {
        let a = MemIndexScanPredicate::In([eid(1)].into());
        let b = MemIndexScanPredicate::In([eid(2)].into());
        assert_eq!(a.try_and_with(&b), Some(MemIndexScanPredicate::False));
    }

    #[test]
    fn try_and_with_in_and_between_filters_in_set() {
        let a = MemIndexScanPredicate::In([eid(1), eid(2), eid(3)].into());
        let b = MemIndexScanPredicate::Between(eid(2), eid(3));
        // Only 2 and 3 fall into the range
        assert_eq!(
            a.try_and_with(&b),
            Some(MemIndexScanPredicate::In([eid(2), eid(3)].into()))
        );
    }

    #[test]
    fn try_and_with_between_and_in_filters_in_set() {
        let a = MemIndexScanPredicate::Between(eid(2), eid(4));
        let b = MemIndexScanPredicate::In([eid(3), eid(4), eid(5)].into());
        // Only 3 and 4 fall into both
        assert_eq!(
            a.try_and_with(&b),
            Some(MemIndexScanPredicate::In([eid(3), eid(4)].into()))
        );
    }

    #[test]
    fn try_and_with_between_and_between_intersects() {
        let a = MemIndexScanPredicate::Between(eid(2), eid(5));
        let b = MemIndexScanPredicate::Between(eid(3), eid(4));
        // Intersection is 3 to 4
        assert_eq!(
            a.try_and_with(&b),
            Some(MemIndexScanPredicate::Between(eid(3), eid(4)))
        );
    }

    #[test]
    fn try_and_with_between_and_between_disjoint_returns_false() {
        let a = MemIndexScanPredicate::Between(eid(1), eid(2));
        let b = MemIndexScanPredicate::Between(eid(3), eid(4));
        // Disjoint
        assert_eq!(a.try_and_with(&b), Some(MemIndexScanPredicate::False));
    }

    #[test]
    fn try_and_with_incompatible_returns_none() {
        let a = MemIndexScanPredicate::In([eid(1)].into());
        let b = MemIndexScanPredicate::EqualTo(Arc::new("x".to_string()));
        assert_eq!(a.try_and_with(&b), None);
    }

    #[test]
    fn choose_index_all_bound() {
        let set = create_storage();

        let pattern = MemIndexScanInstructions::new_gspo([
            traverse_and_filter(1),
            traverse_and_filter(2),
            traverse_and_filter(3),
            traverse_and_filter(4),
        ]);

        let result = set.choose_index(&pattern);

        assert_eq!(result, IndexComponents::GSPO);
    }

    #[test]
    fn choose_index_scan_predicate() {
        let set = create_storage();

        let pattern = MemIndexScanInstructions::new_gspo([
            traverse_and_filter(0),
            traverse_and_filter(1),
            MemIndexScanInstruction::Scan(Arc::new("predicate".to_string()), None),
            traverse_and_filter(3),
        ]);

        let result = set.choose_index(&pattern);
        assert_eq!(result, IndexComponents::GOSP);
    }

    #[test]
    fn choose_index_scan_subject_and_object() {
        let set = create_storage();

        let pattern = MemIndexScanInstructions::new_gspo([
            traverse_and_filter(0),
            MemIndexScanInstruction::Scan(Arc::new("subject".to_string()), None),
            traverse_and_filter(2),
            MemIndexScanInstruction::Scan(Arc::new("object".to_string()), None),
        ]);

        let result = set.choose_index(&pattern);

        assert_eq!(result, IndexComponents::GPOS);
    }

    #[tokio::test]
    async fn scan_gpos_subject_and_object() {
        let set = RwLock::new(create_storage());
        set.write()
            .await
            .insert(&[EncodedQuad {
                graph_name: EncodedObjectId::from(1),
                subject: EncodedObjectId::from(2),
                predicate: EncodedObjectId::from(3),
                object: EncodedObjectId::from(4),
            }])
            .unwrap();

        let pattern = Box::new(MemIndexScanInstructions::new_gspo([
            traverse_and_filter(1),
            MemIndexScanInstruction::Scan(Arc::new("subject".to_string()), None),
            traverse_and_filter(3),
            MemIndexScanInstruction::Scan(Arc::new("object".to_string()), None),
        ]));

        let schema = Arc::new(Schema::new(Fields::from(vec![
            Field::new("subject", DataType::FixedSizeBinary(4), false),
            Field::new("object", DataType::FixedSizeBinary(4), false),
        ])));

        let set_lock = Arc::new(Arc::new(set).read_owned().await);
        let configuration = set_lock.choose_index(&pattern);

        let mut scan = MemQuadIndexScanRecordBatchIterator::new(
            schema,
            set_lock,
            configuration,
            *pattern,
            vec![],
        );

        let batch = scan.next().unwrap().unwrap();
        assert_eq!(configuration, IndexComponents::GPOS);
        assert_debug_snapshot!(batch, @r#"
        RecordBatch {
            schema: Schema {
                fields: [
                    Field {
                        name: "subject",
                        data_type: FixedSizeBinary(
                            4,
                        ),
                    },
                    Field {
                        name: "object",
                        data_type: FixedSizeBinary(
                            4,
                        ),
                    },
                ],
                metadata: {},
            },
            columns: [
                FixedSizeBinaryArray<4>
                [
                  [
                    0,
                    0,
                    0,
                    2,
                ],
                ],
                FixedSizeBinaryArray<4>
                [
                  [
                    0,
                    0,
                    0,
                    4,
                ],
                ],
            ],
            row_count: 1,
        }
        "#);
    }

    fn create_storage() -> IndexPermutations<MemQuadIndex> {
        let mapping = Arc::new(MemObjectIdMapping::new());
        let encoding = Arc::new(ObjectIdEncoding::new(mapping));
        let components = [
            IndexComponents::GSPO,
            IndexComponents::GPOS,
            IndexComponents::GOSP,
        ];
        let indexes = components
            .iter()
            .map(|components| {
                MemQuadIndex::new(MemIndexConfiguration {
                    object_id_encoding: Arc::clone(&encoding),
                    batch_size: 100,
                    components: *components,
                })
            })
            .collect();
        IndexPermutations::new(HashSet::new(), indexes)
    }

    fn traverse_and_filter(id: u32) -> MemIndexScanInstruction {
        MemIndexScanInstruction::Traverse(Some(
            MemIndexScanPredicate::In([eid(id)].into()).into(),
        ))
    }

    fn create_index() -> MemQuadIndex {
        create_index_with_batch_size(10)
    }

    fn create_index_with_batch_size(batch_size: usize) -> MemQuadIndex {
        let mapping = Arc::new(MemObjectIdMapping::new());
        let encoding = Arc::new(ObjectIdEncoding::new(mapping));
        let configuration = MemIndexConfiguration {
            batch_size,
            object_id_encoding: encoding,
            components: IndexComponents::GSPO,
        };
        MemQuadIndex::new(configuration)
    }

    fn traverse(id: u32) -> MemIndexScanInstruction {
        MemIndexScanInstruction::Traverse(Some(
            MemIndexScanPredicate::In([EncodedObjectId::from(id)].into()).into(),
        ))
    }

    fn scan(name: impl Into<String>) -> MemIndexScanInstruction {
        MemIndexScanInstruction::Scan(Arc::new(name.into()), None)
    }

    fn eid(id: u32) -> EncodedObjectId {
        EncodedObjectId::from(id)
    }

    fn run_non_matching_test(
        index: MemQuadIndex,
        instructions: MemIndexScanInstructions,
    ) {
        let results = index.scan_quads(instructions).next();
        assert!(
            results.is_none(),
            "Expected no results in non-matching test."
        );
    }

    fn run_matching_test(
        index: MemQuadIndex,
        instructions: MemIndexScanInstructions,
        expected_columns: usize,
        expected_rows: usize,
    ) {
        let results = index.scan_quads(instructions).next().unwrap().unwrap();

        assert_eq!(results.num_rows, expected_rows);
        assert_eq!(results.columns.len(), expected_columns);
        for (_, result) in results.columns {
            assert_eq!(result.len(), expected_rows);
        }
    }

    fn run_batch_size_test(
        index: MemQuadIndex,
        instructions: MemIndexScanInstructions,
        expected_batch_sizes: &[usize],
        ordered: bool,
    ) {
        let mut batch_sizes: Vec<_> = index
            .scan_quads(instructions)
            .map(|arr| arr.unwrap().num_rows)
            .collect();

        if ordered {
            assert_eq!(batch_sizes, expected_batch_sizes);
        } else {
            let mut expected_batch_sizes = expected_batch_sizes.to_vec();
            batch_sizes.sort();
            expected_batch_sizes.sort();

            assert_eq!(batch_sizes, expected_batch_sizes);
        }
    }
}
