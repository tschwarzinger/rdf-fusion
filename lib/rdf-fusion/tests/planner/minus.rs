use super::StoreTestContext;

/// The join keys of the left-anti join of a `MINUS` must not depend on the iteration order of a
/// hash set, such that planning a query is deterministic.
#[tokio::test]
async fn test_minus_plan_is_deterministic() {
    let ctx = StoreTestContext::new_in_memory();
    let query = "SELECT * WHERE {
        ?a <http://example.org/p0> ?b . ?b <http://example.org/p1> ?c . ?c <http://example.org/p2> ?d
        MINUS { ?a <http://example.org/p3> ?b . ?b <http://example.org/p4> ?c . ?c <http://example.org/p5> ?d }
    }";

    let (expected, _) = ctx.get_query_plans(query).await;
    for _ in 0..10 {
        let (logical, _) = ctx.get_query_plans(query).await;
        assert_eq!(logical, expected);
    }
}
