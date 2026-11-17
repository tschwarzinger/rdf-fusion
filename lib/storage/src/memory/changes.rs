use crate::memory::MemoryState;
use rdf_fusion_common::{
    Dataset, GraphNameRef, NamedNodeRef, NamedOrBlankNode, NamedOrBlankNodeRef, QuadRef,
    TermRef,
};
use std::collections::HashSet;

/// The uncommitted changes of a transaction relative to a base [`MemoryState`].
///
/// The changes are kept minimal, which makes counting the quads straightforward:
/// - [`Self::inserted`] only contains quads that are *not* in the base.
/// - [`Self::removed`] only contains quads that *are* in the base.
/// - The same holds for [`Self::added_graphs`] and [`Self::dropped_graphs`].
///
/// All methods that take a `base` must always be called with the same base.
#[derive(Debug, Clone, Default)]
pub(super) struct Changes {
    /// Inserted quads that are not in the base.
    inserted: Dataset,
    /// Removed quads that are in the base.
    removed: Dataset,
    /// Named graphs that are not in the base.
    added_graphs: HashSet<NamedOrBlankNode>,
    /// Named graphs of the base that were dropped.
    dropped_graphs: HashSet<NamedOrBlankNode>,
}

impl Changes {
    /// Returns `true` if there are no changes.
    pub fn is_empty(&self) -> bool {
        self.inserted.is_empty()
            && self.removed.is_empty()
            && self.added_graphs.is_empty()
            && self.dropped_graphs.is_empty()
    }

    /// Returns the number of quads in `base` with these changes applied.
    pub fn len(&self, base: &MemoryState) -> usize {
        // Exact, as `inserted` and `base` are disjoint and `removed` is a subset of `base`.
        base.quads.len() + self.inserted.len() - self.removed.len()
    }

    /// Returns the quads in `base` with these changes applied that match the given constants.
    pub fn quads_for_pattern<'a>(
        &'a self,
        base: &'a MemoryState,
        subject: Option<NamedOrBlankNodeRef<'a>>,
        predicate: Option<NamedNodeRef<'a>>,
        object: Option<TermRef<'a>>,
        graph: Option<GraphNameRef<'a>>,
    ) -> impl Iterator<Item = QuadRef<'a>> + Send + 'a {
        let has_removed = !self.removed.is_empty();
        let from_base =
            dataset_quads_for_pattern(&base.quads, subject, predicate, object, graph)
                .filter(move |quad| !has_removed || !self.removed.contains(*quad));
        let from_inserted =
            dataset_quads_for_pattern(&self.inserted, subject, predicate, object, graph);
        from_base.chain(from_inserted)
    }

    /// Inserts `quad`. Returns `true` if it was not yet contained.
    ///
    /// The named graph of the quad is added if it does not exist yet.
    pub fn insert(&mut self, base: &MemoryState, quad: QuadRef<'_>) -> bool {
        let inserted = if self.removed.remove(quad) {
            true
        } else if base.quads.contains(quad) {
            false
        } else {
            self.inserted.insert(quad)
        };

        if inserted {
            match quad.graph_name {
                GraphNameRef::NamedNode(node) => {
                    self.add_graph(base, node.into());
                }
                GraphNameRef::BlankNode(node) => {
                    self.add_graph(base, node.into());
                }
                GraphNameRef::DefaultGraph => {}
            }
        }
        inserted
    }

    /// Removes `quad`. Returns `true` if it was contained.
    pub fn remove(&mut self, base: &MemoryState, quad: QuadRef<'_>) -> bool {
        if self.inserted.remove(quad) {
            true
        } else if base.quads.contains(quad) {
            self.removed.insert(quad)
        } else {
            false
        }
    }

    /// Removes all quads of `graph`.
    pub fn clear_graph(&mut self, base: &MemoryState, graph: GraphNameRef<'_>) {
        let quads = self
            .quads_for_pattern(base, None, None, None, Some(graph))
            .map(QuadRef::into_owned)
            .collect::<Vec<_>>();
        for quad in &quads {
            self.remove(base, quad.as_ref());
        }
    }

    /// Adds the named `graph`. Returns `true` if it did not exist yet.
    pub fn add_graph(
        &mut self,
        base: &MemoryState,
        graph: NamedOrBlankNodeRef<'_>,
    ) -> bool {
        let graph = graph.into_owned();
        if self.dropped_graphs.remove(&graph) {
            true
        } else if base.named_graphs.contains(&graph) {
            false
        } else {
            self.added_graphs.insert(graph)
        }
    }

    /// Removes all quads of the named `graph` and the graph itself.
    pub fn drop_graph(&mut self, base: &MemoryState, graph: NamedOrBlankNodeRef<'_>) {
        self.clear_graph(base, graph.into());
        let graph = graph.into_owned();
        if !self.added_graphs.remove(&graph) && base.named_graphs.contains(&graph) {
            self.dropped_graphs.insert(graph);
        }
    }

    /// Returns the named graphs of `base` with these changes applied.
    pub fn named_graphs<'a>(
        &'a self,
        base: &'a MemoryState,
    ) -> impl Iterator<Item = NamedOrBlankNodeRef<'a>> + 'a {
        base.named_graphs
            .iter()
            .filter(|graph| !self.dropped_graphs.contains(*graph))
            .chain(&self.added_graphs)
            .map(NamedOrBlankNode::as_ref)
    }

    /// Applies the changes to `base`.
    pub fn apply(self, base: &mut MemoryState) {
        if base.quads.is_empty() {
            // Nothing can have been removed. Avoids re-inserting all quads (e.g., initial load).
            base.quads = self.inserted;
        } else {
            for quad in &self.removed {
                base.quads.remove(quad);
            }
            for quad in &self.inserted {
                base.quads.insert(quad);
            }
        }

        for graph in &self.dropped_graphs {
            base.named_graphs.remove(graph);
        }
        base.named_graphs.extend(self.added_graphs);
    }
}

/// Returns the quads of `dataset` that match the given constants.
///
/// Like [`Dataset::quads_for_pattern`], but the iterator is [`Send`], such that it can be used in
/// a record batch stream.
fn dataset_quads_for_pattern<'a>(
    dataset: &'a Dataset,
    subject: Option<NamedOrBlankNodeRef<'a>>,
    predicate: Option<NamedNodeRef<'a>>,
    object: Option<TermRef<'a>>,
    graph: Option<GraphNameRef<'a>>,
) -> Box<dyn Iterator<Item = QuadRef<'a>> + Send + 'a> {
    let matches = move |quad: &QuadRef<'a>| {
        subject.is_none_or(|s| s == quad.subject)
            && predicate.is_none_or(|p| p == quad.predicate)
            && object.is_none_or(|o| o == quad.object)
            && graph.is_none_or(|g| g == quad.graph_name)
    };
    // Use the index of the (presumably) most selective constant.
    if let Some(subject) = subject {
        Box::new(dataset.quads_for_subject(subject).filter(matches))
    } else if let Some(object) = object {
        Box::new(dataset.quads_for_object(object).filter(matches))
    } else if let Some(predicate) = predicate {
        Box::new(dataset.quads_for_predicate(predicate).filter(matches))
    } else if let Some(graph) = graph {
        // The graph is the only constant, so all quads of the graph match.
        Box::new(dataset.quads_for_graph_name(graph))
    } else {
        Box::new(dataset.iter())
    }
}
