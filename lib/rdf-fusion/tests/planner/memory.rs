use super::StoreTestContext;
use rdf_fusion::common::{GraphName, NamedNode, Quad};

const P1: &str = "http://example.org/p1";
const P2: &str = "http://example.org/p2";
const P3: &str = "http://example.org/p3";

/// The in-memory storage reports static statistics that only depend on the bound components of a
/// pattern. Patterns with the same estimate are joined in the order in which they appear in the
/// query, regardless of the number of matching quads.
#[tokio::test]
async fn test_memory_patterns_with_equal_estimates_are_joined_in_query_order() {
    let ctx = store_with_predicates().await;

    let query =
        format!("SELECT * WHERE {{ ?s <{P3}> ?a . ?s <{P2}> ?b . ?s <{P1}> ?c }}");
    assert_join_order(&ctx, &query, &[P3, P2, P1]).await;
    assert_eq!(ctx.count_solutions(&query).await, 2);
}

/// Patterns with more bound components are joined first.
#[tokio::test]
async fn test_memory_patterns_with_bound_object_are_joined_first() {
    let ctx = store_with_predicates().await;

    let object = quad(0, P3).object.to_string();
    let query =
        format!("SELECT * WHERE {{ ?s <{P1}> ?a . ?s <{P3}> {object} . ?s <{P2}> ?b }}");
    assert_join_order(&ctx, &query, &[P3, P1, P2]).await;
    assert_eq!(ctx.count_solutions(&query).await, 1);
}

/// Creates an in-memory store in which [`P1`] has the fewest and [`P3`] the most matches.
async fn store_with_predicates() -> StoreTestContext {
    let quads = (0..100)
        .flat_map(|i| {
            let mut predicates = vec![P3];
            if i < 50 {
                predicates.push(P2);
            }
            if i < 2 {
                predicates.push(P1);
            }
            predicates
                .into_iter()
                .map(move |predicate| quad(i, predicate))
        })
        .collect::<Vec<_>>();

    let ctx = StoreTestContext::new_in_memory();
    ctx.extend(&quads).await;
    ctx
}

/// Returns `<subject/i> <predicate> <object/i>`.
fn quad(i: usize, predicate: &str) -> Quad {
    Quad::new(
        NamedNode::new_unchecked(format!("http://example.org/subject/{i}")),
        NamedNode::new_unchecked(predicate),
        NamedNode::new_unchecked(format!("http://example.org/object/{i}")),
        GraphName::DefaultGraph,
    )
}

/// Asserts that the scans of the `predicates` appear in this order in the physical plan of
/// `query`.
async fn assert_join_order(ctx: &StoreTestContext, query: &str, predicates: &[&str]) {
    let (_, plan) = ctx.get_query_plans(query).await;
    let positions = predicates
        .iter()
        .map(|predicate| {
            plan.find(&format!("<{predicate}>"))
                .unwrap_or_else(|| panic!("{predicate} not in plan:\n{plan}"))
        })
        .collect::<Vec<_>>();
    assert!(
        positions.is_sorted(),
        "Expected join order {predicates:?} in plan:\n{plan}"
    );
}
