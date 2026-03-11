use crate::index::{
    EncodedTerm, IndexComponent, IndexComponents, NamedGraphStorage, QuadIndex,
    ScanInstructions,
};
use rdf_fusion_model::StorageError;
use std::fmt::Debug;
use std::hash::Hash;

/// Represents a quad with encoded terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EncodedQuad<TTerm: EncodedTerm> {
    /// The graph name.
    pub graph_name: TTerm,
    /// The subject.
    pub subject: TTerm,
    /// The predicate.
    pub predicate: TTerm,
    /// The object.
    pub object: TTerm,
}

impl<TTerm: EncodedTerm> EncodedQuad<TTerm> {
    /// Creates a new [IndexQuad] for an index with the given `components`.
    pub fn for_index(&self, components: IndexComponents) -> IndexQuad<TTerm> {
        let mut terms = Vec::with_capacity(4);
        for component in components.inner() {
            match component {
                IndexComponent::GraphName => terms.push(self.graph_name),
                IndexComponent::Subject => terms.push(self.subject),
                IndexComponent::Predicate => terms.push(self.predicate),
                IndexComponent::Object => terms.push(self.object),
            }
        }
        let terms = TryInto::<[TTerm; 4]>::try_into(terms)
            .expect("Components always have length 4");
        IndexQuad(terms)
    }
}

/// A quad that is sorted for some index.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexQuad<TTerm: EncodedTerm>(pub [TTerm; 4]);

/// Represents a set of multiple indexes, each of which indexes a different ordering of the
/// triple component (e.g., SPO, POS). This is necessary as different triple patterns require
/// different index structures.
///
/// For example, the pattern `<S> <P> ?o` can be best served by having an SPO index. The scan would
/// then look up `<S>`, traverse into the next level looking up `<P>`, and lastly scanning the
/// entries and binding them to `?o`. However, the triple pattern `?s <P> <O>` cannot be efficiently
/// evaluated with an SPO index. For this pattern, the query engine should use an POS or OPS index.
///
/// The [IndexPermutations] allows managing multiple such indices.
#[derive(Debug)]
pub struct IndexPermutations<TIndex: QuadIndex> {
    /// The [NamedGraphStorage] that is used to separately store named graphs.
    named_graphs: TIndex::NamedGraphStorage,
    /// The index variations.
    indexes: Vec<TIndex>,
}

impl<TIndex: QuadIndex> IndexPermutations<TIndex> {
    /// Creates a new [IndexPermutations].
    pub fn new(named_graphs: TIndex::NamedGraphStorage, indexes: Vec<TIndex>) -> Self {
        Self {
            named_graphs,
            indexes,
        }
    }

    /// Finds an index with the given `configuration`.
    pub fn find_index(&self, configuration: IndexComponents) -> Option<&TIndex> {
        self.indexes
            .iter()
            .find(|index| index.components() == configuration)
    }

    /// Chooses the index for scanning the given `pattern`.
    ///
    /// This returns an [IndexComponents] that identifies the chosen index. Use
    /// [MemIndexSetScanIterator] for executing the scan operation.
    pub fn choose_index(&self, pattern: &TIndex::ScanInstructions) -> IndexComponents {
        self.indexes
            .iter()
            .rev() // Prefer SPO (max by uses the last on equality)
            .max_by(|lhs, rhs| {
                let lhs_pattern = pattern.reorder(lhs.components());
                let rhs_pattern = pattern.reorder(rhs.components());

                let lhs_score = lhs.compute_scan_score(&lhs_pattern);
                let rhs_score = rhs.compute_scan_score(&rhs_pattern);

                lhs_score.cmp(&rhs_score)
            })
            .expect("At least one index must be available")
            .components()
    }

    pub fn len(&self) -> usize {
        self.any_index().len()
    }

    pub fn insert(
        &mut self,
        quads: &[EncodedQuad<TIndex::Term>],
    ) -> Result<usize, StorageError> {
        let mut count = 0;
        for index in self.indexes.iter_mut() {
            let components = index.components();
            let quads = quads.iter().map(|q| q.for_index(components));
            count = index.insert(quads);
        }

        for quad in quads.iter().filter(|q| !q.graph_name.is_default_graph()) {
            self.named_graphs.insert(quad.graph_name);
        }

        Ok(count)
    }

    pub fn remove(&mut self, quads: &[EncodedQuad<TIndex::Term>]) -> usize {
        let mut count = 0;
        for index in self.indexes.iter_mut() {
            let components = index.components();
            let quads = quads.iter().map(|q| q.for_index(components));
            count = index.remove(quads);
        }
        count
    }

    pub fn insert_named_graph(&mut self, graph_name: TIndex::Term) -> bool {
        self.named_graphs.insert(graph_name)
    }

    pub fn named_graphs(&self) -> Vec<TIndex::Term> {
        self.named_graphs.iter().collect()
    }

    pub fn contains_named_graph(&self, graph_name: TIndex::Term) -> bool {
        self.named_graphs.contains(&graph_name)
    }

    pub fn clear(&mut self) {
        for index in self.indexes.iter_mut() {
            index.clear();
        }
    }

    pub fn clear_graph(&mut self, graph_name: &TIndex::Term) {
        for index in self.indexes.iter_mut() {
            index.clear_graph(*graph_name);
        }
    }

    pub fn drop_named_graph(&mut self, graph_name: &TIndex::Term) -> bool {
        self.clear_graph(graph_name);
        self.named_graphs.remove(graph_name)
    }

    fn any_index(&self) -> &TIndex {
        self.indexes.first().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_index_gspo() {
        let quad = dummy_quad();
        let reordered = quad.for_index(IndexComponents::GSPO);
        assert_eq!(reordered.0, [oid(1), oid(2), oid(3), oid(4)]);
    }

    #[test]
    fn test_for_index_gpos() {
        let quad = dummy_quad();
        let reordered = quad.for_index(IndexComponents::GPOS);
        assert_eq!(reordered.0, [oid(1), oid(3), oid(4), oid(2)]);
    }

    fn dummy_quad() -> EncodedQuad<MyId> {
        EncodedQuad {
            graph_name: MyId(1),
            subject: MyId(2),
            predicate: MyId(3),
            object: MyId(4),
        }
    }

    fn oid(id: u32) -> MyId {
        MyId(id)
    }

    #[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
    struct MyId(u32);

    impl EncodedTerm for MyId {
        fn is_default_graph(&self) -> bool {
            self.0 == 0
        }
    }
}
