use crate::memory::MemoryQuadStorage;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, InListExpr, lit,
};
use datafusion::physical_plan::filter_pushdown::{
    ChildFilterPushdownResult, ChildPushdownResult, FilterPushdownPhase, PushedDown,
};
use datafusion::physical_plan::{
    ExecutionPlan, StatisticsArgs, StatisticsContext, collect,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use rdf_fusion_common::{
    ActiveGraph, BlankNode, BlankNodeMatchingMode, GraphName, Literal, NamedNode,
    NamedNodePattern, NamedNodeRef, Quad, QuadPattern, Term, TermPattern, TriplePattern,
    Variable,
};
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::plain_term::PlainTermArrayElementBuilder;
use rdf_fusion_encoding::plain_term::PlainTermQuadsBuilder;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::{QuadStorageEncoding, TermDecoder, TermEncoding};
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use rdf_fusion_logical::quad_pattern::compute_quad_pattern_filters;
use std::sync::Arc;

const EX: NamedNodeRef<'static> = NamedNodeRef::new_unchecked("http://example.com/ex");
const G: NamedNodeRef<'static> = NamedNodeRef::new_unchecked("http://example.com/g");

#[tokio::test]
async fn insert_counts_duplicates_once() {
    let fixture = Fixture::new();

    let tx = fixture.begin().await;
    assert_eq!(tx.insert(&[quad("a"), quad("a"), quad_in("b", G)]).await, 2);
    tx.commit().await;

    assert_eq!(fixture.len().await, 2);
}

#[tokio::test]
async fn reinserting_committed_quads_inserts_nothing() {
    let fixture = Fixture::with_committed(&[quad("a"), quad_in("b", G)]).await;

    let tx = fixture.begin().await;
    assert_eq!(tx.insert(&[quad("a"), quad_in("b", G)]).await, 0);
    assert_eq!(tx.len().await, 2);
}

#[tokio::test]
async fn removed_committed_quad_can_be_reinserted() {
    let fixture = Fixture::with_committed(&[quad("a"), quad_in("b", G)]).await;

    let tx = fixture.begin().await;
    assert!(tx.remove(&[quad("a"), quad("a")]).await);
    assert_eq!(tx.len().await, 1);
    assert!(!tx.remove(&[quad("a")]).await);
    assert_eq!(tx.insert(&[quad("a"), quad("a")]).await, 1);
    assert_eq!(tx.len().await, 2);
    tx.commit().await;

    assert_eq!(fixture.len().await, 2);
}

#[tokio::test]
async fn inserted_and_removed_quad_is_not_counted() {
    let fixture = Fixture::with_committed(&[quad("a")]).await;

    let tx = fixture.begin().await;
    assert_eq!(tx.insert(&[quad_in("c", G), quad_in("c", G)]).await, 1);
    assert!(tx.remove(&[quad_in("c", G)]).await);
    assert_eq!(tx.len().await, 1);
    tx.commit().await;

    assert_eq!(fixture.len().await, 1);
}

#[tokio::test]
async fn dropped_transaction_is_rolled_back() {
    let fixture = Fixture::new();

    let tx = fixture.begin().await;
    tx.insert(&[quad_in("a", G)]).await;
    drop(tx);

    assert_eq!(fixture.len().await, 0);
    assert_eq!(fixture.num_named_graphs().await, 0);
}

#[tokio::test]
async fn snapshot_of_storage_is_unaffected_by_later_commits() {
    let fixture = Fixture::with_committed(&[quad("a")]).await;
    let before = fixture.snapshot().await;

    let tx = fixture.begin().await;
    tx.insert(&[quad("b")]).await;
    tx.commit().await;

    assert_eq!(fixture.len_of(&before).await, 1);
    assert_eq!(fixture.len().await, 2);
}

#[tokio::test]
async fn snapshot_of_transaction_is_unaffected_by_later_changes() {
    let fixture = Fixture::with_committed(&[quad("a")]).await;

    let tx = fixture.begin().await;
    tx.insert(&[quad("b")]).await;
    let tx_snapshot = tx.snapshot().await;
    tx.remove(&[quad("a"), quad("b")]).await;
    assert_eq!(fixture.len_of(&tx_snapshot).await, 2);
    assert_eq!(tx.len().await, 0);

    tx.commit().await;
    assert_eq!(fixture.len_of(&tx_snapshot).await, 2);
    assert_eq!(fixture.len().await, 0);
}

#[tokio::test]
async fn scan_streams_batches() {
    let fixture = Fixture::with_batch_size(2);
    let mut quads = default_graph_quads(["a", "b", "c", "d", "e"]);
    quads.push(quad_in("a", G)); // Not in the default graph, hence not matched.
    fixture.commit(&quads).await;

    let plan = fixture.scan_predicate_ex().await;

    assert_eq!(plan.name(), "MemoryQuadScanExec");
    let batches = fixture.execute(plan).await;
    assert_eq!(batches.len(), 3);
    assert_eq!(count_rows(&batches), 5);
}

#[tokio::test]
async fn planned_scan_is_unaffected_by_later_commits() {
    let fixture = Fixture::with_committed(&[quad("a")]).await;
    let plan = fixture.scan_predicate_ex().await;

    // The quads are only read during execution, i.e., after this commit.
    let tx = fixture.begin().await;
    tx.remove(&[quad("a")]).await;
    tx.insert(&[quad("b")]).await;
    tx.commit().await;

    assert_eq!(count_rows(&fixture.execute(plan).await), 1);
    assert_eq!(fixture.len().await, 1);
}

#[tokio::test]
async fn scan_reports_static_statistics() {
    let fixture = Fixture::with_committed(&pattern_test_quads()).await;
    let ex = NamedNodePattern::NamedNode(EX.into_owned());
    let num_rows = async |subject, object| {
        let pattern =
            quad_pattern(ActiveGraph::AllGraphs, None, subject, ex.clone(), object);
        let statistics = scan_statistics(&fixture.scan(&pattern, None).await);
        assert!(!statistics.num_rows.is_exact().unwrap());
        *statistics.num_rows.get_value().unwrap()
    };

    let ex_term = TermPattern::NamedNode(EX.into_owned());
    // The estimates do not depend on the quads, only on the bound components.
    assert_eq!(num_rows(var("s"), var("o")).await, 1_000_000);
    assert_eq!(num_rows(ex_term.clone(), var("o")).await, 10);
    assert_eq!(num_rows(ex_term.clone(), ex_term).await, 1);
}

#[tokio::test]
async fn scan_without_match_reports_exact_empty_statistics() {
    let fixture = Fixture::with_committed(&pattern_test_quads()).await;
    let pattern = quad_pattern(
        ActiveGraph::Union(vec![]),
        None,
        var("s"),
        NamedNodePattern::NamedNode(EX.into_owned()),
        var("o"),
    );

    let statistics = scan_statistics(&fixture.scan(&pattern, None).await);
    assert_eq!(statistics.num_rows, Precision::Exact(0));
}

#[tokio::test]
async fn scan_checks_repeated_variables() {
    let fixture = Fixture::with_committed(&pattern_test_quads()).await;

    // `?x <ex> ?x` only matches quads whose subject equals their object.
    let pattern = quad_pattern(
        ActiveGraph::AllGraphs,
        None,
        var("x"),
        NamedNodePattern::NamedNode(EX.into_owned()),
        var("x"),
    );
    assert_eq!(
        fixture.scan_rows(&pattern, None).await,
        vec![
            vec!["<http://example.com/a>"],
            vec!["<http://example.com/a>"],
            vec!["<http://example.com/ex>"],
            vec!["_:x"],
        ]
    );

    // `GRAPH ?g { ?g ?p ?o }` only matches quads whose graph equals their subject.
    let pattern = quad_pattern(
        ActiveGraph::AllGraphs,
        Some("g"),
        var("g"),
        NamedNodePattern::Variable(Variable::new_unchecked("p")),
        var("o"),
    );
    assert_eq!(
        fixture.scan_rows(&pattern, None).await,
        vec![vec![
            "<http://example.com/g>",
            "<http://example.com/ex>",
            "<http://example.com/b>"
        ]]
    );
}

#[tokio::test]
async fn scan_respects_active_graph() {
    let fixture = Fixture::with_committed(&pattern_test_quads()).await;
    let h = GraphName::from(example_iri("h"));
    let count = async |active_graph| {
        let pattern = quad_pattern(
            active_graph,
            None,
            var("s"),
            NamedNodePattern::Variable(Variable::new_unchecked("p")),
            var("o"),
        );
        fixture.scan_rows(&pattern, None).await.len()
    };

    assert_eq!(count(ActiveGraph::DefaultGraph).await, 4);
    assert_eq!(count(ActiveGraph::AllGraphs).await, 7);
    assert_eq!(count(ActiveGraph::AnyNamedGraph).await, 3);
    assert_eq!(count(ActiveGraph::Union(vec![])).await, 0);
    assert_eq!(count(ActiveGraph::Union(vec![G.into()])).await, 2);
    // Duplicate graphs are only scanned once.
    let union = vec![G.into(), h.clone(), G.into(), GraphName::DefaultGraph, h];
    assert_eq!(count(ActiveGraph::Union(union)).await, 7);
}

#[tokio::test]
async fn scan_respects_blank_node_mode() {
    let fixture = Fixture::with_committed(&pattern_test_quads()).await;
    let blank_node_pattern = |mode| {
        let mut pattern = quad_pattern(
            ActiveGraph::AllGraphs,
            None,
            TermPattern::BlankNode(BlankNode::new_unchecked("x")),
            NamedNodePattern::NamedNode(EX.into_owned()),
            var("o"),
        );
        pattern.blank_node_mode = mode;
        pattern
    };

    // As a variable, the blank node matches any subject and is projected.
    let rows = fixture
        .scan_rows(&blank_node_pattern(BlankNodeMatchingMode::Variable), None)
        .await;
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0].len(), 2);

    // As a filter, the blank node only matches itself and is not projected.
    let rows = fixture
        .scan_rows(&blank_node_pattern(BlankNodeMatchingMode::Filter), None)
        .await;
    assert_eq!(rows, vec![vec!["_:x"]]);
}

#[tokio::test]
async fn scan_applies_projection_in_order() {
    let fixture = Fixture::with_committed(&pattern_test_quads()).await;
    let pattern = quad_pattern(
        ActiveGraph::Union(vec![G.into()]),
        Some("g"),
        var("s"),
        NamedNodePattern::NamedNode(EX.into_owned()),
        var("o"),
    );

    let plan = fixture.scan(&pattern, Some(vec![2, 0])).await;
    assert_eq!(column_names(&plan.schema()), ["o", "g"]);
    assert_eq!(
        rows(&fixture.execute(plan).await),
        vec![
            vec!["<http://example.com/a>", "<http://example.com/g>"],
            vec!["<http://example.com/b>", "<http://example.com/g>"],
        ]
    );

    let plan = fixture.scan(&pattern, Some(vec![])).await;
    assert!(plan.schema().fields().is_empty());
    assert_eq!(count_rows(&fixture.execute(plan).await), 2);
}

#[tokio::test]
async fn scan_filters_repeated_variables_in_datafusion() {
    let fixture = Fixture::with_committed(&pattern_test_quads()).await;
    let pattern = quad_pattern(
        ActiveGraph::AllGraphs,
        None,
        var("x"),
        NamedNodePattern::NamedNode(EX.into_owned()),
        var("x"),
    );

    let plan = fixture.scan(&pattern, None).await;
    assert_eq!(plan.name(), "ProjectionExec");
    assert_eq!(plan.children()[0].name(), "FilterExec");
    assert_eq!(leaf_scan(&plan).name(), "MemoryQuadScanExec");
    assert_eq!(column_names(&plan.schema()), ["x"]);
}

#[tokio::test]
async fn scan_restricts_lookups_with_dynamic_in_list_filter() {
    let fixture = Fixture::with_committed(&pattern_test_quads()).await;
    // `?s ?p ?o` produces the columns `s`, `p`, and `o`.
    let pattern = quad_pattern(
        ActiveGraph::AllGraphs,
        None,
        var("s"),
        NamedNodePattern::Variable(Variable::new_unchecked("p")),
        var("o"),
    );
    let plan = fixture.scan(&pattern, None).await;
    let object = Column::new("o", 2);

    let filter = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(object.clone())],
        lit(true),
    ));
    let result = plan
        .handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            ChildPushdownResult {
                parent_filters: vec![ChildFilterPushdownResult {
                    filter: Arc::clone(&filter) as _,
                    child_results: vec![],
                }],
                self_filters: vec![],
            },
            &ConfigOptions::default(),
        )
        .unwrap();
    assert!(matches!(result.filters[..], [PushedDown::No]));
    let plan = result.updated_node.unwrap();

    // Before the filter is updated (e.g., the build side is not collected), all quads are scanned.
    assert_eq!(count_rows(&fixture.execute(Arc::clone(&plan)).await), 7);

    // The duplicate and the literal (which is never a subject) must not cause duplicate rows.
    let objects = [
        Term::from(example_iri("a")),
        Term::from(example_iri("a")),
        Term::from(BlankNode::new_unchecked("x")),
    ];
    let mut builder = PlainTermArrayElementBuilder::with_capacity(objects.len());
    for object in &objects {
        builder.append_term(object.as_ref());
    }
    let in_list = InListExpr::try_new_from_array(
        Arc::new(object),
        builder.finish().into_array_ref(),
        false,
        plan.schema().as_ref(),
    )
    .unwrap();
    filter
        .update(Arc::new(BinaryExpr::new(
            Arc::new(in_list),
            Operator::And,
            lit(true),
        )))
        .unwrap();

    assert_eq!(
        rows(&fixture.execute(plan).await),
        vec![
            vec![
                "<http://example.com/a>",
                "<http://example.com/ex>",
                "<http://example.com/a>"
            ],
            vec![
                "<http://example.com/a>",
                "<http://example.com/ex>",
                "<http://example.com/a>"
            ],
            vec!["_:x", "<http://example.com/ex>", "_:x"],
            vec!["_:y", "<http://example.com/ex>", "_:x"],
        ]
    );
}

/// Compares the scan with filtering and projecting all quads in DataFusion, which is how the
/// other storages scan a pattern.
#[tokio::test]
async fn scan_matches_generic_filter_and_projection() {
    let quads = pattern_test_quads();
    let fixture = Fixture::with_committed(&quads).await;
    let h = GraphName::from(example_iri("h"));

    let active_graphs = [
        ActiveGraph::DefaultGraph,
        ActiveGraph::AllGraphs,
        ActiveGraph::AnyNamedGraph,
        ActiveGraph::Union(vec![]),
        ActiveGraph::Union(vec![G.into()]),
        ActiveGraph::Union(vec![h.clone(), G.into(), h.clone()]),
        ActiveGraph::Union(vec![GraphName::DefaultGraph, h]),
    ];
    let p = NamedNodePattern::Variable(Variable::new_unchecked("p"));
    let ex = NamedNodePattern::NamedNode(EX.into_owned());
    let blank_x = TermPattern::BlankNode(BlankNode::new_unchecked("x"));
    let literal = TermPattern::Literal(Literal::new_simple_literal("lit"));
    let triple_patterns = [
        (var("s"), p.clone(), var("o")),
        (var("x"), ex.clone(), var("x")),
        (var("g"), p.clone(), var("o")),
        (
            var("x"),
            NamedNodePattern::Variable(Variable::new_unchecked("x")),
            var("x"),
        ),
        (blank_x.clone(), ex.clone(), var("o")),
        (blank_x.clone(), ex.clone(), blank_x),
        (
            TermPattern::NamedNode(EX.into_owned()),
            p.clone(),
            literal.clone(),
        ),
        (literal, p, var("o")),
    ];

    for active_graph in &active_graphs {
        for graph_variable in [None, Some("g")] {
            for (subject, predicate, object) in &triple_patterns {
                for mode in [
                    BlankNodeMatchingMode::Variable,
                    BlankNodeMatchingMode::Filter,
                ] {
                    let mut pattern = quad_pattern(
                        active_graph.clone(),
                        graph_variable,
                        subject.clone(),
                        predicate.clone(),
                        object.clone(),
                    );
                    pattern.blank_node_mode = mode;

                    let plan = fixture.scan(&pattern, None).await;
                    let (expected_schema, expected_rows) =
                        fixture.filter_and_project(&quads, &pattern).await;
                    assert_eq!(plan.schema(), expected_schema, "{pattern:?}");
                    let actual_rows = rows(&fixture.execute(plan).await);
                    assert_eq!(actual_rows, expected_rows, "{pattern:?}");
                }
            }
        }
    }
}

/// A [`MemoryQuadStorage`] with a session to access it.
struct Fixture {
    ctx: SessionContext,
    storage: MemoryQuadStorage,
}

impl Fixture {
    fn new() -> Self {
        Self::with_config(SessionConfig::new())
    }

    fn with_batch_size(batch_size: usize) -> Self {
        Self::with_config(SessionConfig::new().with_batch_size(batch_size))
    }

    fn with_config(config: SessionConfig) -> Self {
        Self {
            ctx: SessionContext::new_with_config(config),
            storage: MemoryQuadStorage::new(),
        }
    }

    /// Creates a storage that contains `quads`.
    async fn with_committed(quads: &[Quad]) -> Self {
        let fixture = Self::new();
        fixture.commit(quads).await;
        fixture
    }

    fn state(&self) -> SessionState {
        self.ctx.state()
    }

    async fn begin(&self) -> Transaction<'_> {
        let tx = self.storage.begin_transaction(&self.state()).await.unwrap();
        Transaction { fixture: self, tx }
    }

    /// Inserts `quads` in a single transaction.
    async fn commit(&self, quads: &[Quad]) {
        let tx = self.begin().await;
        tx.insert(quads).await;
        tx.commit().await;
    }

    async fn snapshot(&self) -> Arc<dyn QuadStorageSnapshot> {
        self.storage.snapshot().await.unwrap()
    }

    /// Returns the number of committed quads.
    async fn len(&self) -> usize {
        self.len_of(&self.snapshot().await).await
    }

    async fn len_of(&self, snapshot: &Arc<dyn QuadStorageSnapshot>) -> usize {
        snapshot.len(&self.state()).await.unwrap()
    }

    /// Returns the number of committed named graphs.
    async fn num_named_graphs(&self) -> usize {
        let plan = self
            .snapshot()
            .await
            .named_graphs(&self.state())
            .await
            .unwrap();
        count_rows(&self.execute(plan).await)
    }

    /// Plans a scan of `?s <ex> ?o` in the default graph on the committed state.
    async fn scan_predicate_ex(&self) -> Arc<dyn ExecutionPlan> {
        let pattern = quad_pattern(
            ActiveGraph::DefaultGraph,
            None,
            var("s"),
            NamedNodePattern::NamedNode(EX.into_owned()),
            var("o"),
        );
        self.scan(&pattern, None).await
    }

    /// Plans a scan of `pattern` on the committed state.
    async fn scan(
        &self,
        pattern: &QuadPattern,
        projection: Option<Vec<usize>>,
    ) -> Arc<dyn ExecutionPlan> {
        self.snapshot()
            .await
            .scan_quad_pattern(pattern, projection, &self.state())
            .await
            .unwrap()
    }

    /// Returns the sorted rows of a scan of `pattern` on the committed state.
    async fn scan_rows(
        &self,
        pattern: &QuadPattern,
        projection: Option<Vec<usize>>,
    ) -> Vec<Vec<String>> {
        let plan = self.scan(pattern, projection).await;
        rows(&self.execute(plan).await)
    }

    /// Matches `pattern` against `quads` by filtering and projecting them in DataFusion. Returns
    /// the schema and the sorted rows.
    async fn filter_and_project(
        &self,
        quads: &[Quad],
        pattern: &QuadPattern,
    ) -> (SchemaRef, Vec<Vec<String>>) {
        let mut data_frame = self.data_frame(quads);
        let filters =
            compute_quad_pattern_filters(pattern, &QuadStorageEncoding::PlainTerm)
                .await
                .unwrap();
        if let Some(filter) = filters.into_iter().reduce(Expr::and) {
            data_frame = data_frame.filter(filter).unwrap();
        }
        let columns = pattern
            .compute_projection()
            .into_iter()
            .map(|(expr, name)| expr.alias(name));
        let data_frame = data_frame.select(columns).unwrap();
        let schema = Arc::new(data_frame.schema().as_arrow().clone());
        (schema, rows(&data_frame.collect().await.unwrap()))
    }

    async fn execute(&self, plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
        collect(plan, self.ctx.task_ctx()).await.unwrap()
    }

    fn data_frame(&self, quads: &[Quad]) -> DataFrame {
        let mut builder = PlainTermQuadsBuilder::with_capacity(quads.len());
        for quad in quads {
            builder.append_quad(quad.as_ref());
        }
        self.ctx
            .read_batch(builder.finish().into_record_batch())
            .unwrap()
    }
}

/// An open transaction of a [`Fixture`].
struct Transaction<'a> {
    fixture: &'a Fixture,
    tx: Box<dyn QuadStorageTransaction>,
}

impl Transaction<'_> {
    /// Returns the number of inserted quads.
    async fn insert(&self, quads: &[Quad]) -> usize {
        let quads = self.fixture.data_frame(quads);
        self.tx.insert(quads).await.unwrap().unwrap()
    }

    /// Returns whether any quad was removed.
    async fn remove(&self, quads: &[Quad]) -> bool {
        let quads = self.fixture.data_frame(quads);
        self.tx.remove(quads).await.unwrap().unwrap()
    }

    async fn len(&self) -> usize {
        self.tx.len(&self.fixture.state()).await.unwrap()
    }

    async fn snapshot(&self) -> Arc<dyn QuadStorageSnapshot> {
        self.tx.snapshot().await.unwrap()
    }

    async fn commit(self) {
        self.tx.commit().await.unwrap();
    }
}

/// Returns `<ex> <ex> <http://example.com/{object}>` in the default graph.
fn quad(object: &str) -> Quad {
    Quad::new(EX, EX, example_iri(object), GraphName::DefaultGraph)
}

/// Returns `<ex> <ex> <http://example.com/{object}>` in `graph`.
fn quad_in(object: &str, graph: NamedNodeRef<'_>) -> Quad {
    Quad::new(EX, EX, example_iri(object), graph)
}

fn default_graph_quads(objects: impl IntoIterator<Item = impl AsRef<str>>) -> Vec<Quad> {
    objects.into_iter().map(|o| quad(o.as_ref())).collect()
}

/// Returns quads with repeated terms, blank nodes, and literals in several graphs.
fn pattern_test_quads() -> Vec<Quad> {
    let a = example_iri("a");
    let g = example_iri("g");
    let h = example_iri("h");
    let x = BlankNode::new_unchecked("x");
    let y = BlankNode::new_unchecked("y");
    vec![
        // The default graph.
        Quad::new(EX, EX, EX, GraphName::DefaultGraph),
        Quad::new(a.clone(), EX, a.clone(), GraphName::DefaultGraph),
        Quad::new(x.clone(), EX, x.clone(), GraphName::DefaultGraph),
        Quad::new(y, EX, x, GraphName::DefaultGraph),
        // The graph `G`.
        Quad::new(a.clone(), EX, a, G),
        Quad::new(g, EX, example_iri("b"), G),
        // The graph `h`.
        Quad::new(EX, h.clone(), Literal::new_simple_literal("lit"), h),
    ]
}

/// Returns a pattern in the [`BlankNodeMatchingMode::Variable`] mode.
fn quad_pattern(
    active_graph: ActiveGraph,
    graph_variable: Option<&str>,
    subject: TermPattern,
    predicate: NamedNodePattern,
    object: TermPattern,
) -> QuadPattern {
    QuadPattern::new(
        active_graph,
        graph_variable.map(Variable::new_unchecked),
        TriplePattern {
            subject,
            predicate,
            object,
        },
        BlankNodeMatchingMode::Variable,
    )
}

fn var(name: &str) -> TermPattern {
    TermPattern::Variable(Variable::new_unchecked(name))
}

fn column_names(schema: &SchemaRef) -> Vec<&str> {
    schema.fields().iter().map(|f| f.name().as_str()).collect()
}

/// Returns the sorted rows of `batches`, with the terms formatted as strings. Nulls (i.e., the
/// default graph) are formatted as `DEFAULT`.
fn rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                let array = PLAIN_TERM_ENCODING
                    .try_new_array(Arc::clone(column))
                    .unwrap();
                DefaultPlainTermDecoder::decode_terms(&array)
                    .map(|term| match term {
                        Ok(term) => Term::from(term).to_string(),
                        Err(_) => "DEFAULT".to_owned(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            rows.push(columns.iter().map(|column| column[row].clone()).collect());
        }
    }
    rows.sort();
    rows
}

fn example_iri(name: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("http://example.com/{name}"))
}

/// Returns the leaf of `plan` (i.e., the scan below the filters and projections).
fn leaf_scan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let mut scan = Arc::clone(plan);
    while let Some(child) = scan.children().first() {
        scan = Arc::clone(child);
    }
    scan
}

/// Returns the statistics of the scan in `plan`. The filter above the scan estimates its own
/// selectivity, hence the scan itself is inspected.
fn scan_statistics(plan: &Arc<dyn ExecutionPlan>) -> Arc<Statistics> {
    StatisticsContext::new()
        .compute(leaf_scan(plan).as_ref(), &StatisticsArgs::new())
        .unwrap()
}

fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}
