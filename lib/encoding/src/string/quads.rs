use crate::QuadStorageEncoding;
use crate::encoding::EncodingArray;
use crate::string::StringTermArray;
use datafusion::arrow::array::{ArrayBuilder, RecordBatch, StringBuilder};
use rdf_fusion_common::QuadRef;
use std::fmt::{Display, Write};
use std::sync::Arc;

/// A structure containing four [`StringTermArray`] components: graph, subject, predicate, object.
#[derive(Debug, Clone)]
pub struct StringQuads {
    pub graphs: StringTermArray,
    pub subjects: StringTermArray,
    pub predicates: StringTermArray,
    pub objects: StringTermArray,
}

impl StringQuads {
    /// Creates a new [`StringQuads`].
    pub fn new(
        graphs: StringTermArray,
        subjects: StringTermArray,
        predicates: StringTermArray,
        objects: StringTermArray,
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
            Arc::clone(QuadStorageEncoding::String.quad_schema().inner()),
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

pub struct StringQuadsBuilder {
    graphs: StringBuilder,
    subjects: StringBuilder,
    predicates: StringBuilder,
    objects: StringBuilder,
}

impl StringQuadsBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            graphs: StringBuilder::with_capacity(capacity, capacity * 50),
            subjects: StringBuilder::with_capacity(capacity, capacity * 50),
            predicates: StringBuilder::with_capacity(capacity, capacity * 50),
            objects: StringBuilder::with_capacity(capacity, capacity * 50),
        }
    }

    pub fn append_quad(&mut self, quad: QuadRef<'_>) {
        match quad.graph_name {
            rdf_fusion_common::GraphNameRef::NamedNode(nn) => {
                append_term(&mut self.graphs, nn)
            }
            rdf_fusion_common::GraphNameRef::BlankNode(bn) => {
                append_term(&mut self.graphs, bn)
            }
            rdf_fusion_common::GraphNameRef::DefaultGraph => self.graphs.append_null(),
        }
        append_term(&mut self.subjects, quad.subject);
        append_term(&mut self.predicates, quad.predicate);
        append_term(&mut self.objects, quad.object);
    }

    pub fn append_graph(
        &mut self,
        graph_name: Option<rdf_fusion_common::NamedOrBlankNodeRef<'_>>,
    ) {
        match graph_name {
            Some(rdf_fusion_common::NamedOrBlankNodeRef::NamedNode(nn)) => {
                append_term(&mut self.graphs, nn);
            }
            Some(rdf_fusion_common::NamedOrBlankNodeRef::BlankNode(bn)) => {
                append_term(&mut self.graphs, bn);
            }
            None => self.graphs.append_null(),
        };
        self.subjects.append_null();
        self.predicates.append_null();
        self.objects.append_null();
    }

    /// Returns the number of quads in the builder.
    pub fn len(&self) -> usize {
        self.subjects.len()
    }

    /// Returns true if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(mut self) -> StringQuads {
        StringQuads {
            graphs: StringTermArray::new_unchecked(Arc::new(self.graphs.finish())),
            subjects: StringTermArray::new_unchecked(Arc::new(self.subjects.finish())),
            predicates: StringTermArray::new_unchecked(Arc::new(
                self.predicates.finish(),
            )),
            objects: StringTermArray::new_unchecked(Arc::new(self.objects.finish())),
        }
    }
}

/// Appends the string representation of `term` to the `builder`.
///
/// The term is formatted directly into the value buffer of the builder, which avoids allocating
/// a temporary [`String`] for every term.
fn append_term(builder: &mut StringBuilder, term: impl Display) {
    write!(builder, "{term}").expect("Writing to a StringBuilder cannot fail");
    builder.append_value("");
}
