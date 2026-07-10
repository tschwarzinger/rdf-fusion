use crate::planner::{
    assert_query_explanation, setup_basic_store, setup_store_with_graph_data,
};
use datafusion::physical_plan::displayable;
use insta::assert_snapshot;

#[tokio::test]
async fn test_bgp_planner_short_circuit() {
    let store = setup_basic_store().await;
    let query = "SELECT ?s WHERE { ?s <http://example.org/p1> <http://example.org/o1> . ?s <http://example.org/p2> ?o2 }";

    assert_query_explanation(store, query, |explanation| {
        assert_snapshot!(&explanation.optimized_logical_plan, @"
        BasicGraphPattern: projection=[s], columns_to_decode=[s], 
          QuadPattern: triple_pattern=[?s <http://example.org/p1> <http://example.org/o1>]
          QuadPattern: triple_pattern=[?s <http://example.org/p2> ?o2], projection=[0]
        ");

        assert_snapshot!(displayable(explanation.execution_plan.as_ref()).indent(true), @"
        DecodeObjectIdsExec: projections=[decode(s__oid) as s]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s__oid@0, s__oid@0)], projection=[s__oid@0]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> <http://example.org/o1>], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as s__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1 AND object@3 = 2, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= 2 AND 2 <= object_max@6, required_guarantees=[object in (2), predicate in (1)]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p2> ?o2], blank_node_mode=Variable, file_groups={1 group: [[]]}, projection=[subject@1 as s__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 3 AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 3 AND 3 <= predicate_max@2, required_guarantees=[predicate in (3)]
        ");
    }).await;
}

#[tokio::test]
async fn test_bgp_planner_filter() {
    let store = setup_basic_store().await;
    let query = "SELECT ?s WHERE { ?s <http://example.org/p1> ?o . FILTER(STR(?o) = \"target\") }";

    assert_query_explanation(store, query, |explanation| {
        assert_snapshot!(&explanation.optimized_logical_plan, @"
        DecodeObjectIds: columns=[s]
          Projection: s
            Filter: EBV(EQ(ENC_TF(STR(o)), Union 2:{value:target,language:}))
              DecodeObjectIds: columns=[o]
                QuadPattern: triple_pattern=[?s <http://example.org/p1> ?o]
        ");

        assert_snapshot!(displayable(explanation.execution_plan.as_ref()).indent(true), @"
        DecodeObjectIdsExec: projections=[decode(s) as s]
          FilterExec: EBV(EQ(ENC_TF(STR(o@1)), 2:{value:target,language:})), projection=[s@0]
            DecodeObjectIdsExec: projections=[s, decode(o) as o]
              ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> ?o], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as s, object@3 as o], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2, required_guarantees=[predicate in (1)]
        ");
    }).await;
}

#[tokio::test]
async fn test_bgp_planner_projection_pushdown() {
    let store = setup_basic_store().await;
    let query = "SELECT ?s WHERE { ?s <http://example.org/p1> ?o1 . ?s <http://example.org/p2> ?o2 }";

    assert_query_explanation(store, query, |explanation| {
        assert_snapshot!(&explanation.optimized_logical_plan, @"
        BasicGraphPattern: projection=[s], columns_to_decode=[s], 
          QuadPattern: triple_pattern=[?s <http://example.org/p1> ?o1], projection=[0]
          QuadPattern: triple_pattern=[?s <http://example.org/p2> ?o2], projection=[0]
        ");

        assert_snapshot!(displayable(explanation.execution_plan.as_ref()).indent(true), @"
        DecodeObjectIdsExec: projections=[decode(s__oid) as s]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s__oid@0, s__oid@0)], projection=[s__oid@0]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> ?o1], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as s__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2, required_guarantees=[predicate in (1)]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p2> ?o2], blank_node_mode=Variable, file_groups={1 group: [[]]}, projection=[subject@1 as s__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 3 AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 3 AND 3 <= predicate_max@2, required_guarantees=[predicate in (3)]
        ");
    }).await;
}

#[tokio::test]
async fn test_bgp_planner_late_decoding() {
    let store = setup_basic_store().await;
    let query = "SELECT ?s WHERE { ?s <http://example.org/p1> ?o1 . ?o1 <http://example.org/p2> ?o2 }";

    assert_query_explanation(store, query, |explanation| {
        assert_snapshot!(&explanation.optimized_logical_plan, @"
        BasicGraphPattern: projection=[s], columns_to_decode=[s], 
          QuadPattern: triple_pattern=[?s <http://example.org/p1> ?o1]
          QuadPattern: triple_pattern=[?o1 <http://example.org/p2> ?o2], projection=[0]
        ");

        assert_snapshot!(displayable(explanation.execution_plan.as_ref()).indent(true), @"
        DecodeObjectIdsExec: projections=[decode(s__oid) as s]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o1__oid@1, o1__oid@0)], projection=[s__oid@0]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?s <http://example.org/p1> ?o1], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as s__oid, object@3 as o1__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2, required_guarantees=[predicate in (1)]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?o1 <http://example.org/p2> ?o2], blank_node_mode=Variable, file_groups={1 group: [[]]}, projection=[subject@1 as o1__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 3 AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 3 AND 3 <= predicate_max@2, required_guarantees=[predicate in (3)]
        ");
    }).await;
}

#[tokio::test]
async fn test_bgp_planner_complex_filter() {
    let store = setup_store_with_graph_data("../../examples/data/paris.ttl").await;

    let query = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?city ?name
        WHERE {
            ?city a schema:City .
            ?city schema:population ?pop .
            ?city schema:postalCode ?code .
            ?city schema:name ?name .
            FILTER(?pop > 1000000 && STR(?code) = "75001")
        }
    "#;

    assert_query_explanation(store, query, |explanation| {
        assert_snapshot!(&explanation.optimized_logical_plan, @"
        BasicGraphPattern: projection=[city, name], columns_to_decode=[city, code, name, pop], filters=[EBV(GT(ENC_TF(pop), Union 4:4:1000000)) AND EBV(EQ(ENC_TF(STR(code)), Union 2:{value:75001,language:})) AS EBV(BOOLEAN_AS_TERM(EBV(GT(ENC_TF(pop),ENC_TF(Struct({term_type:2,value:1000000,data_type:http://www.w3.org/2001/XMLSchema#integer,language_tag:})))) AND EBV(EQ(ENC_TF(STR(code)),ENC_TF(Struct({term_type:2,value:75001,data_type:http://www.w3.org/2001/XMLSchema#string,language_tag:}))))))]
          QuadPattern: triple_pattern=[?city <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/City>]
          QuadPattern: triple_pattern=[?city <http://schema.org/population> ?pop]
          QuadPattern: triple_pattern=[?city <http://schema.org/postalCode> ?code]
          QuadPattern: triple_pattern=[?city <http://schema.org/name> ?name]
        ");

        assert_snapshot!(displayable(explanation.execution_plan.as_ref()).indent(true), @"
        ProjectionExec: expr=[city@0 as city, name@3 as name]
          DecodeObjectIdsExec: projections=[decode(city__oid) as city, pop, code, decode(name__oid) as name]
            FilterExec: EBV(EQ(ENC_TF(STR(code@2)), 2:{value:75001,language:})) AND EBV(GT(ENC_TF(pop@1), 4:4:1000000))
              DecodeObjectIdsExec: projections=[city__oid, decode(pop__oid) as pop, decode(code__oid) as code, name__oid]
                HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(city__oid@0, city__oid@0)], projection=[city__oid@0, pop__oid@1, code__oid@2, name__oid@4]
                  HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(city__oid@0, city__oid@0)], projection=[city__oid@0, pop__oid@1, code__oid@3]
                    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(city__oid@0, city__oid@0)], projection=[city__oid@0, pop__oid@2]
                      ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/City>], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as city__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1 AND object@3 = 8, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= 8 AND 8 <= object_max@6, required_guarantees=[object in (8), predicate in (1)]
                      ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://schema.org/population> ?pop], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as city__oid, object@3 as pop__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 4 AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 4 AND 4 <= predicate_max@2, required_guarantees=[predicate in (4)]
                    ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://schema.org/postalCode> ?code], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as city__oid, object@3 as code__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 7 AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 7 AND 7 <= predicate_max@2, required_guarantees=[predicate in (7)]
                  ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://schema.org/name> ?name], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as city__oid, object@3 as name__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 2 AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 2 AND 2 <= predicate_max@2, required_guarantees=[predicate in (2)]
        ");
    }).await;
}

/// Tests that when a filter is pushed down on a join column, there is no error because the data
/// types of the two columns do no longer match.
#[tokio::test]
async fn test_bgp_planner_filter_on_join_column_bsbm_explore_5() {
    let store = setup_basic_store().await;

    let query = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?city ?name
        WHERE {
            ?city a ?type .
            ?city schema:name ?name .
            FILTER(STR(?city) = "http://www.wikidata.org/entity/Q90" && STR(?name) = "Paris" && STR(?type) = "http://schema.org/City" )
        }
    "#;

    assert_query_explanation(store, query, |explanation| {
        assert_snapshot!(&explanation.optimized_logical_plan, @"
        BasicGraphPattern: projection=[city, name], columns_to_decode=[city, name, type], filters=[EBV(EQ(ENC_TF(STR(city)), Union 2:{value:http://www.wikidata.org/entity/Q90,language:})) AND EBV(EQ(ENC_TF(STR(name)), Union 2:{value:Paris,language:})) AND EBV(EQ(ENC_TF(STR(type)), Union 2:{value:http://schema.org/City,language:})) AS EBV(BOOLEAN_AS_TERM(EBV(BOOLEAN_AS_TERM(EBV(EQ(ENC_TF(STR(city)),ENC_TF(Struct({term_type:2,value:http://www.wikidata.org/entity/Q90,data_type:http://www.w3.org/2001/XMLSchema#string,language_tag:})))) AND EBV(EQ(ENC_TF(STR(name)),ENC_TF(Struct({term_type:2,value:Paris,data_type:http://www.w3.org/2001/XMLSchema#string,language_tag:})))))) AND EBV(EQ(ENC_TF(STR(type)),ENC_TF(Struct({term_type:2,value:http://schema.org/City,data_type:http://www.w3.org/2001/XMLSchema#string,language_tag:}))))))]
          QuadPattern: triple_pattern=[?city <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type]
          QuadPattern: triple_pattern=[?city <http://schema.org/name> ?name]
        ");

        assert_snapshot!(displayable(explanation.execution_plan.as_ref()).indent(true), @"
        FilterExec: EBV(EQ(ENC_TF(STR(name@2)), 2:{value:Paris,language:})) AND EBV(EQ(ENC_TF(STR(type@1)), 2:{value:http://schema.org/City,language:})) AND EBV(EQ(ENC_TF(STR(city@0)), 2:{value:http://www.wikidata.org/entity/Q90,language:})), projection=[city@0, name@2]
          DecodeObjectIdsExec: projections=[decode(city__oid) as city, decode(type__oid) as type, decode(name__oid) as name]
            HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(city__oid@0, city__oid@0)], projection=[city__oid@0, type__oid@1, name__oid@3]
              ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type], blank_node_mode=Variable, file_groups={1 group: [[]]}, projection=[subject@1 as city__oid, object@3 as type__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 4, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 4 AND 4 <= predicate_max@2, required_guarantees=[predicate in (4)]
              ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://schema.org/name> ?name], blank_node_mode=Variable, file_groups={1 group: [[]]}, projection=[subject@1 as city__oid, object@3 as name__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 3 AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 3 AND 3 <= predicate_max@2, required_guarantees=[predicate in (3)]
        ");
    }).await;
}

/// Tests that a filter can be pushed down, but it actually operates on the object id and not the
/// decoded version. The filter should be `?city = <object-id>`. The decoding pushdown of the BGP
/// planner must be smart enough to not use the decoded column or the planner must be smart enough
/// to not push these expressions down.
#[tokio::test]
async fn test_bgp_planner_filter_pushdown_with_object_id() {
    let store = setup_store_with_graph_data("../../examples/data/paris.ttl").await;

    let query = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?city
        WHERE {
            ?city a schema:City .
            ?city schema:population ?pop .
            FILTER(?city = <http://www.wikidata.org/entity/Q90>) # Will be transformed to object id equality
        }
    "#;

    assert_query_explanation(store, query, |explanation| {
        assert_snapshot!(&explanation.optimized_logical_plan, @"
        BasicGraphPattern: projection=[city], columns_to_decode=[city], filters=[city__oid = Int64(0) AS EBV(EQ(ENC_TF(city),ENC_TF(Struct({term_type:0,value:http://www.wikidata.org/entity/Q90,data_type:,language_tag:}))))]
          QuadPattern: triple_pattern=[?city <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/City>]
          QuadPattern: triple_pattern=[?city <http://schema.org/population> ?pop], projection=[0]
        ");

        assert_snapshot!(displayable(explanation.execution_plan.as_ref()).indent(true), @"
        DecodeObjectIdsExec: projections=[decode(city__oid) as city]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(city__oid@0, city__oid@0)], projection=[city__oid@0]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/City>], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as city__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1 AND object@3 = 8 AND subject@1 = 0, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= 8 AND 8 <= object_max@6 AND subject_null_count@10 != row_count@4 AND subject_min@8 <= 0 AND 0 <= subject_max@9, required_guarantees=[object in (8), predicate in (1), subject in (0)]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://schema.org/population> ?pop], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as city__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 4 AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 4 AND 4 <= predicate_max@2, required_guarantees=[predicate in (4)]
        ");
    })
    .await;
}

/// Tests that there is no error if a column is unused. This triggered an error in an early version.
#[tokio::test]
async fn test_bgp_planner_not_using_one_column() {
    let store = setup_store_with_graph_data("../../examples/data/paris.ttl").await;

    let query = r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?city ?code
        WHERE {
            ?city a schema:City .
            ?city schema:population ?pop .
            ?city schema:postalCode ?code .
        }
        ORDER BY ?code # Decodes all columns before the sort, causing columns_to_decode to be a superset of the projection
    "#;

    assert_query_explanation(store, query, |explanation| {
        assert_snapshot!(&explanation.optimized_logical_plan, @"
        Sort: AS_SORTABLE_BYTES(ENC_TF(code)) ASC NULLS FIRST
          BasicGraphPattern: projection=[city, code], columns_to_decode=[city, code], 
            QuadPattern: triple_pattern=[?city <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/City>]
            QuadPattern: triple_pattern=[?city <http://schema.org/population> ?pop], projection=[0]
            QuadPattern: triple_pattern=[?city <http://schema.org/postalCode> ?code]
        ");

        assert_snapshot!(displayable(explanation.execution_plan.as_ref()).indent(true), @"
        SortExec: expr=[AS_SORTABLE_BYTES(ENC_TF(code@1)) ASC], preserve_partitioning=[false]
          DecodeObjectIdsExec: projections=[decode(city__oid) as city, decode(code__oid) as code]
            HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(city__oid@0, city__oid@0)], projection=[city__oid@0, code__oid@2]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(city__oid@0, city__oid@0)], projection=[city__oid@0]
                ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/City>], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as city__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 1 AND object@3 = 8, pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 1 AND 1 <= predicate_max@2 AND object_null_count@7 != row_count@4 AND object_min@5 <= 8 AND 8 <= object_max@6, required_guarantees=[object in (8), predicate in (1)]
                ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://schema.org/population> ?pop], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as city__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 4 AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 4 AND 4 <= predicate_max@2, required_guarantees=[predicate in (4)]
              ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[?city <http://schema.org/postalCode> ?code], blank_node_mode=Variable, file_groups={1 group: [[GPOS/<name>.parquet]]}, projection=[subject@1 as city__oid, object@3 as code__oid], file_type=parquet, predicate=graph@0 IS NULL AND predicate@2 = 7 AND DynamicFilter [ empty ], pruning_predicate=graph_null_count@0 > 0 AND predicate_null_count@3 != row_count@4 AND predicate_min@1 <= 7 AND 7 <= predicate_max@2, required_guarantees=[predicate in (7)]
        ");
    }).await;
}
