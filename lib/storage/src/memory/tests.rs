use crate::memory::MemoryQuadStorage;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::Statistics;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionState;
use datafusion::physical_plan::{
    ExecutionPlan, StatisticsArgs, StatisticsContext, collect,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use rdf_fusion_common::{
    ActiveGraph, BlankNodeMatchingMode, GraphName, NamedNode, NamedNodePattern,
    NamedNodeRef, Quad, QuadPattern, TermPattern, TriplePattern, Variable,
};
use rdf_fusion_encoding::plain_term::PlainTermQuadsBuilder;
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
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
async fn scan_streams_batches_and_estimates_rows() {
    let fixture = Fixture::with_batch_size(2);
    let mut quads = default_graph_quads(["a", "b", "c", "d", "e"]);
    quads.push(quad_in("a", G)); // Not in the default graph, hence not matched.
    fixture.commit(&quads).await;

    let plan = fixture.scan_predicate_ex().await;

    assert_eq!(leaf_scan(&plan).name(), "MemoryQuadScanExec");
    assert_eq!(scan_statistics(&plan).num_rows.get_value(), Some(&5));
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
async fn large_scan_reports_no_statistics() {
    // Roughly 2 MiB in the plain term encoding, which exceeds the statistics limit.
    let objects = (0..20_000)
        .map(|i| format!("a-long-object-iri-to-make-the-quads-large/{i}"))
        .collect::<Vec<_>>();
    let fixture = Fixture::with_committed(&default_graph_quads(&objects)).await;

    let statistics = scan_statistics(&fixture.scan_predicate_ex().await);

    assert_eq!(statistics.num_rows.get_value(), None);
    assert_eq!(statistics.total_byte_size.get_value(), None);
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
        let pattern = QuadPattern::new(
            ActiveGraph::DefaultGraph,
            None,
            TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked("s")),
                predicate: NamedNodePattern::NamedNode(EX.into_owned()),
                object: TermPattern::Variable(Variable::new_unchecked("o")),
            },
            BlankNodeMatchingMode::Variable,
        );
        self.snapshot()
            .await
            .scan_quad_pattern(&pattern, None, &self.state())
            .await
            .unwrap()
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
