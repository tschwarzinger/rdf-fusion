use crate::QuadStorageEncoding;
use crate::encoding::EncodingArray;
use crate::plain_term::{PlainTermArray, PlainTermArrayElementBuilder};
use datafusion::arrow::array::RecordBatch;
use rdf_fusion_model::QuadRef;
use std::sync::Arc;

/// A structure containing four [`PlainTermArray`] components: graph, subject, predicate, object.
#[derive(Debug, Clone)]
pub struct PlainTermQuads {
    pub graphs: PlainTermArray,
    pub subjects: PlainTermArray,
    pub predicates: PlainTermArray,
    pub objects: PlainTermArray,
}

impl PlainTermQuads {
    /// Creates a new [`PlainTermQuads`].
    pub fn new(
        graphs: PlainTermArray,
        subjects: PlainTermArray,
        predicates: PlainTermArray,
        objects: PlainTermArray,
    ) -> Self {
        Self {
            graphs,
            subjects,
            predicates,
            objects,
        }
    }

    /// Returns the number of quads.
    pub fn len(&self) -> usize {
        self.subjects.inner().len()
    }

    /// Returns true if there are no quads.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a [`RecordBatch`] containing the quads.
    pub fn into_record_batch(self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(QuadStorageEncoding::PlainTerm.quad_schema().inner()),
            vec![
                self.graphs.into_array_ref(),
                self.subjects.into_array_ref(),
                self.predicates.into_array_ref(),
                self.objects.into_array_ref(),
            ],
        )
        .expect("Valid RecordBatch")
    }
}

pub struct PlainTermQuadsBuilder {
    graphs: PlainTermArrayElementBuilder,
    subjects: PlainTermArrayElementBuilder,
    predicates: PlainTermArrayElementBuilder,
    objects: PlainTermArrayElementBuilder,
}

impl PlainTermQuadsBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            graphs: PlainTermArrayElementBuilder::with_capacity(capacity),
            subjects: PlainTermArrayElementBuilder::with_capacity(capacity),
            predicates: PlainTermArrayElementBuilder::with_capacity(capacity),
            objects: PlainTermArrayElementBuilder::with_capacity(capacity),
        }
    }

    pub fn append_quad(&mut self, quad: QuadRef<'_>) {
        self.graphs.append_graph_name(quad.graph_name);
        self.subjects.append_named_or_blank_node(quad.subject);
        self.predicates.append_named_node(quad.predicate);
        self.objects.append_term(quad.object);
    }

    pub fn append_graph(
        &mut self,
        graph_name: Option<rdf_fusion_model::NamedOrBlankNodeRef<'_>>,
    ) {
        let graph_name = match graph_name {
            Some(rdf_fusion_model::NamedOrBlankNodeRef::NamedNode(nn)) => {
                rdf_fusion_model::GraphNameRef::NamedNode(nn)
            }
            Some(rdf_fusion_model::NamedOrBlankNodeRef::BlankNode(bn)) => {
                rdf_fusion_model::GraphNameRef::BlankNode(bn)
            }
            None => rdf_fusion_model::GraphNameRef::DefaultGraph,
        };
        self.graphs.append_graph_name(graph_name);
        self.subjects.append_null();
        self.predicates.append_null();
        self.objects.append_null();
    }

    pub fn finish(self) -> PlainTermQuads {
        PlainTermQuads {
            graphs: self.graphs.finish(),
            subjects: self.subjects.finish(),
            predicates: self.predicates.finish(),
            objects: self.objects.finish(),
        }
    }
}
