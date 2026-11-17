use crate::memory::changes::Changes;
use crate::memory::{MemoryQuadStorageSnapshot, MemoryState};
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionState;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{
    DFResult, GraphName, GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad,
    StorageError, TermRef,
};
use rdf_fusion_encoding::plain_term::PlainTermArray;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::{TermDecoder, TermEncoding};
use rdf_fusion_extensions::storage::{QuadStorageSnapshot, QuadStorageTransaction};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::OwnedMutexGuard;

/// A transaction on a [`MemoryQuadStorage`](super::MemoryQuadStorage).
///
/// The transaction records its changes on top of the committed state at the start of the
/// transaction. Committing the transaction applies the changes to the committed state. As
/// transactions are serialized, no other commit can happen in between.
#[derive(Debug)]
pub struct MemoryQuadStorageTransaction {
    /// The committed state of the storage. Modified on commit.
    storage_state: Arc<RwLock<Arc<MemoryState>>>,
    /// The committed state at the start of the transaction.
    base: Arc<MemoryState>,
    /// The uncommitted changes. Shared with the snapshots of this transaction and copied on the
    /// next modification if a snapshot is still alive (see [`Arc::make_mut`]).
    changes: Mutex<Arc<Changes>>,
    /// Prevents other transactions from starting until this transaction is committed or dropped.
    write_guard: OwnedMutexGuard<()>,
}

impl MemoryQuadStorageTransaction {
    pub(super) fn new(
        storage_state: Arc<RwLock<Arc<MemoryState>>>,
        base: Arc<MemoryState>,
        write_guard: OwnedMutexGuard<()>,
    ) -> Self {
        Self {
            storage_state,
            base,
            changes: Mutex::new(Arc::new(Changes::default())),
            write_guard,
        }
    }

    /// Returns the current changes. Later modifications do not affect the returned changes.
    fn current_changes(&self) -> Arc<Changes> {
        Arc::clone(&self.changes.lock().expect("Lock is not poisoned"))
    }

    fn with_changes_mut<R>(&self, f: impl FnOnce(&mut Changes, &MemoryState) -> R) -> R {
        let mut changes = self.changes.lock().expect("Lock is not poisoned");
        f(Arc::make_mut(&mut changes), &self.base)
    }
}

#[async_trait]
impl QuadStorageTransaction for MemoryQuadStorageTransaction {
    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        Ok(Arc::new(MemoryQuadStorageSnapshot::new(
            Arc::clone(&self.base),
            self.current_changes(),
        )))
    }

    async fn insert(&self, quads: DataFrame) -> Result<Option<usize>, StorageError> {
        let quads = collect_quads(quads).await?;
        Ok(self.with_changes_mut(|changes, base| {
            // Duplicates (in `quads` or with existing quads) are only counted once.
            let inserted = quads
                .iter()
                .filter(|quad| changes.insert(base, quad.as_ref()))
                .count();
            Some(inserted)
        }))
    }

    async fn remove(&self, quads: DataFrame) -> Result<Option<bool>, StorageError> {
        let quads = collect_quads(quads).await?;
        Ok(self.with_changes_mut(|changes, base| {
            // Not `any`, as it would stop after the first removal.
            let removed = quads
                .iter()
                .filter(|quad| changes.remove(base, quad.as_ref()))
                .count();
            Some(removed > 0)
        }))
    }

    async fn create_named_graph(
        &self,
        graphs: DataFrame,
    ) -> Result<Option<bool>, StorageError> {
        let graphs = collect_graphs(graphs).await?;
        Ok(self.with_changes_mut(|changes, base| {
            let mut all_new = true;
            for graph in graphs.iter().filter_map(to_named_graph) {
                all_new &= changes.add_graph(base, graph);
            }
            Some(all_new)
        }))
    }

    async fn clear_graph(&self, graphs: DataFrame) -> Result<(), StorageError> {
        let graphs = collect_graphs(graphs).await?;
        self.with_changes_mut(|changes, base| {
            for graph in &graphs {
                changes.clear_graph(base, graph.as_ref());
            }
        });
        Ok(())
    }

    async fn drop_graph(&self, graphs: DataFrame) -> Result<(), StorageError> {
        let graphs = collect_graphs(graphs).await?;
        self.with_changes_mut(|changes, base| {
            for graph in &graphs {
                match to_named_graph(graph) {
                    Some(graph) => changes.drop_graph(base, graph),
                    // The default graph always exists. Dropping it clears it.
                    None => changes.clear_graph(base, GraphNameRef::DefaultGraph),
                }
            }
        });
        Ok(())
    }

    async fn len(&self, _state: &SessionState) -> Result<usize, StorageError> {
        Ok(self.current_changes().len(&self.base))
    }

    async fn commit(self: Box<Self>) -> Result<(), StorageError> {
        let Self {
            storage_state,
            base,
            changes,
            write_guard,
        } = *self;

        let changes = changes.into_inner().expect("Lock is not poisoned");
        if !changes.is_empty() {
            // Release our reference to the committed state, so that it is only copied if a
            // snapshot still uses it. Snapshots of this transaction may still use the changes.
            drop(base);
            let changes = Arc::unwrap_or_clone(changes);
            let mut state = storage_state.write().expect("Lock is not poisoned");
            changes.apply(Arc::make_mut(&mut state));
        }

        drop(write_guard);
        Ok(())
    }
}

/// Collects the quads of the `data_frame`.
async fn collect_quads(data_frame: DataFrame) -> DFResult<Vec<Quad>> {
    let mut result = Vec::new();
    for batch in data_frame.collect().await? {
        let graphs = column(&batch, COL_GRAPH)?;
        let subjects = column(&batch, COL_SUBJECT)?;
        let predicates = column(&batch, COL_PREDICATE)?;
        let objects = column(&batch, COL_OBJECT)?;
        let rows = decode(&graphs)
            .zip(decode(&subjects))
            .zip(decode(&predicates))
            .zip(decode(&objects));
        for (((graph, subject), predicate), object) in rows {
            let subject: NamedOrBlankNode = match subject {
                Some(TermRef::NamedNode(node)) => node.into_owned().into(),
                Some(TermRef::BlankNode(node)) => node.into_owned().into(),
                _ => return exec_err!("Invalid subject: {subject:?}"),
            };
            let Some(TermRef::NamedNode(predicate)) = predicate else {
                return exec_err!("Invalid predicate: {predicate:?}");
            };
            let Some(object) = object else {
                return exec_err!("Missing object");
            };
            result.push(Quad::new(
                subject,
                predicate.into_owned(),
                object.into_owned(),
                to_graph_name(graph)?,
            ));
        }
    }
    Ok(result)
}

/// Collects the graph names of the `data_frame`. Null is the default graph.
async fn collect_graphs(data_frame: DataFrame) -> DFResult<Vec<GraphName>> {
    let mut result = Vec::new();
    for batch in data_frame.collect().await? {
        let graphs = column(&batch, COL_GRAPH)?;
        for graph in decode(&graphs) {
            result.push(to_graph_name(graph)?);
        }
    }
    Ok(result)
}

/// Returns the column `name` of `batch` as a [`PlainTermArray`]. Columns in the string encoding
/// are converted to the plain term encoding.
fn column(batch: &RecordBatch, name: &str) -> DFResult<PlainTermArray> {
    let array = batch.column(batch.schema().index_of(name)?);
    if array.data_type() == STRING_ENCODING.data_type() {
        return Ok(STRING_ENCODING
            .try_new_array(ArrayRef::clone(array))?
            .as_plain_term_array()?);
    }
    PlainTermArray::try_from(ArrayRef::clone(array)).map_err(|_| {
        exec_datafusion_err!(
            "The MemoryQuadStorage only accepts terms in the plain term or string encoding. Got: {}",
            array.data_type()
        )
    })
}

/// Decodes all terms of `array`. Null is decoded as [`None`].
fn decode(array: &PlainTermArray) -> impl Iterator<Item = Option<TermRef<'_>>> {
    DefaultPlainTermDecoder::decode_terms(array).map(Result::ok)
}

fn to_graph_name(term: Option<TermRef<'_>>) -> DFResult<GraphName> {
    match term {
        None => Ok(GraphName::DefaultGraph),
        Some(TermRef::NamedNode(node)) => Ok(node.into_owned().into()),
        Some(TermRef::BlankNode(node)) => Ok(node.into_owned().into()),
        Some(term) => exec_err!("Invalid graph name: {term}"),
    }
}

/// Returns the named graph of `graph_name`, or [`None`] for the default graph.
fn to_named_graph(graph_name: &GraphName) -> Option<NamedOrBlankNodeRef<'_>> {
    match graph_name {
        GraphName::NamedNode(node) => Some(node.into()),
        GraphName::BlankNode(node) => Some(node.into()),
        GraphName::DefaultGraph => None,
    }
}
