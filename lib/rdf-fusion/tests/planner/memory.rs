use super::StoreTestContext;
use rdf_fusion::common::{GraphName, NamedNode, Quad};

const SMALL: &str = "http://example.org/small";
const LARGE_1: &str = "http://example.org/large1";
const LARGE_2: &str = "http://example.org/large2";

/// Large scans report no statistics. Hence, they are joined after the small scans, in the order in
/// which they appear in the query.
#[tokio::test]
async fn test_memory_large_scans_are_joined_last_in_query_order() {
    let ctx = store_with_small_and_large_predicates().await;

    let query = format!(
        "SELECT * WHERE {{ ?s <{LARGE_2}> ?a . ?s <{LARGE_1}> ?b . ?s <{SMALL}> ?c }}"
    );
    assert_join_order(&ctx, &query, &[SMALL, LARGE_2, LARGE_1]).await;

    let query = format!(
        "SELECT * WHERE {{ ?s <{LARGE_1}> ?a . ?s <{SMALL}> ?c . ?s <{LARGE_2}> ?b }}"
    );
    assert_join_order(&ctx, &query, &[SMALL, LARGE_1, LARGE_2]).await;
    assert_eq!(ctx.count_solutions(&query).await, 2);
}

/// Creates an in-memory store in which the patterns with the predicates [`LARGE_1`] and [`LARGE_2`]
/// are too large to report statistics, while the pattern with [`SMALL`] is small. [`LARGE_2`] has
/// more matches than [`LARGE_1`], so ordering by size would join [`LARGE_1`] first.
async fn store_with_small_and_large_predicates() -> StoreTestContext {
    let quads = (0..15_000)
        .flat_map(|i| {
            let mut predicates = vec![LARGE_2];
            if i < 10_000 {
                predicates.push(LARGE_1);
            }
            if i < 2 {
                predicates.push(SMALL);
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

/// Returns `<subject/i> <predicate> <object/i>`. The subjects are long to make the quads large.
fn quad(i: usize, predicate: &str) -> Quad {
    Quad::new(
        NamedNode::new_unchecked(format!(
            "http://example.org/a-long-subject-iri-to-make-the-quads-large/{i}"
        )),
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
