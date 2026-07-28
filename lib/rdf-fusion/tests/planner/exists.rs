use super::StoreTestContext;
use crate::assert_plan_snapshot;

#[tokio::test]
async fn test_exists_planner_simple() {
    let ctx = StoreTestContext::setup_basic().await;
    let query = r"SELECT ?s WHERE {
        ?s <http://example.org/p1> <http://example.org/o1> .
        FILTER NOT EXISTS { ?s <http://example.org/p1> <http://example.org/o2> . }
    }";

    let (logical, physical) = ctx.get_query_plans(query).await;
    assert_plan_snapshot!(logical, @"
        DecodeObjectIds: columns=[s]
          Projection: s
            LeftAnti Join: s = __correlated_sq_1.__inner__s
              QuadPattern: triple_pattern=[?s <http://example.org/p1> <http://example.org/o1>]
              SubqueryAlias: __correlated_sq_1
                Projection: s AS __inner__s
                  QuadPattern: triple_pattern=[?s <http://example.org/p1> <http://example.org/o2>]
        ");
    assert_plan_snapshot!(physical, @"
    DecodeObjectIdsExec: projections=[decode(s) as s]
      HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(s@0, __inner__s@0)]
        ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> <http://example.org/o1>], blank_node_mode=Variable, file_groups={1 group: [[quad-tables/GPOS/<file>.parquet]]}, projection=[subject@1 as s], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1 AND object@3 = 2, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= 2 AND 2 <= object_max@6, required_guarantees=[object in (2), predicate in (1)]
        ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> <http://example.org/o2>], blank_node_mode=Variable, file_groups={1 group: [[quad-tables/GPOS/<file>.parquet]]}, projection=[subject@1 as __inner__s], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1 AND object@3 = 4, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= 4 AND 4 <= object_max@6, required_guarantees=[object in (4), predicate in (1)]
    ");
}
