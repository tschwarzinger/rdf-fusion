use crate::planner::{assert_query_explanation, setup_basic_store};
use datafusion::physical_plan::displayable;
use insta::assert_snapshot;

#[tokio::test]
async fn test_exists_planner_simple() {
    let store = setup_basic_store().await;
    let query = r"SELECT ?s WHERE {
        ?s <http://example.org/p1> <http://example.org/o1> .
        FILTER NOT EXISTS { ?s <http://example.org/p1> <http://example.org/o2> . }
    }";

    assert_query_explanation(store, query, |explanation| {
        assert_snapshot!(&explanation.optimized_logical_plan, @"
        LeftAnti Join: s = __correlated_sq_1.__inner__s
          DecodeObjectIds: columns=[s]
            QuadPattern: triple_pattern=[?s <http://example.org/p1> <http://example.org/o1>]
          SubqueryAlias: __correlated_sq_1
            Projection: s AS __inner__s
              DecodeObjectIds: columns=[s]
                QuadPattern: triple_pattern=[?s <http://example.org/p1> <http://example.org/o2>]
        ");

        assert_snapshot!(displayable(explanation.execution_plan.as_ref()).indent(true), @"
        HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(s@0, __inner__s@0)]
          DecodeObjectIdsExec: projections=[s -> s]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> <http://example.org/o1>], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as s], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1 AND object@3 = 2, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= 2 AND 2 <= object_max@6, required_guarantees=[object in (2), predicate in (1)]
          ProjectionExec: expr=[s@0 as __inner__s]
            DecodeObjectIdsExec: projections=[s -> s]
              ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> <http://example.org/o2>], blank_node_mode=Variable, file_groups={1 group: [[]]}, projection=[subject@1 as s], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1 AND object@3 = 3, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= 3 AND 3 <= object_max@6, required_guarantees=[object in (3), predicate in (1)]
        ");
    }).await;
}
