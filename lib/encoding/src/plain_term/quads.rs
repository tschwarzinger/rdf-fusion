use crate::encoding::EncodingArray;
use crate::plain_term::{PlainTermArray, PlainTermArrayElementBuilder};
use rdf_fusion_model::QuadRef;

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
}

pub struct PlainTermQuadsBuilder {
    graphs: PlainTermArrayElementBuilder,
    subjects: PlainTermArrayElementBuilder,
    predicates: PlainTermArrayElementBuilder,
    objects: PlainTermArrayElementBuilder,
}

impl PlainTermQuadsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            graphs: PlainTermArrayElementBuilder::new(capacity),
            subjects: PlainTermArrayElementBuilder::new(capacity),
            predicates: PlainTermArrayElementBuilder::new(capacity),
            objects: PlainTermArrayElementBuilder::new(capacity),
        }
    }

    pub fn append_quad(&mut self, quad: QuadRef<'_>) {
        self.graphs.append_graph_name(quad.graph_name);
        self.subjects.append_named_or_blank_node(quad.subject);
        self.predicates.append_named_node(quad.predicate);
        self.objects.append_term(quad.object);
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
