use crate::plain_term::encoding::PlainTermType;
use crate::plain_term::{PlainTermArray, PlainTermEncoding};
use datafusion::arrow::array::{
    ArrayBuilder, Int8Builder, NullBufferBuilder, StringBuilder, StructArray,
};
use rdf_fusion_common::{
    BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef, NamedOrBlankNodeRef, TermRef,
};
use std::sync::Arc;

/// Provides a convenient API for building arrays (element-by-element) of RDF terms with the
/// [PlainTermEncoding]. The documentation of the encoding provides additional information.
pub struct PlainTermArrayElementBuilder {
    null_buffer: NullBufferBuilder,
    term_type: Int8Builder,
    value: StringBuilder,
    data_type: StringBuilder,
    language_tag: StringBuilder,
}

impl Default for PlainTermArrayElementBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PlainTermArrayElementBuilder {
    /// Create a [PlainTermArrayElementBuilder].
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Create a [PlainTermArrayElementBuilder] with the given `capacity`.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            null_buffer: NullBufferBuilder::new(capacity),
            term_type: Int8Builder::with_capacity(capacity),
            value: StringBuilder::with_capacity(capacity, capacity * 100),
            data_type: StringBuilder::with_capacity(capacity, capacity * 100),
            language_tag: StringBuilder::with_capacity(capacity, capacity * 10),
        }
    }

    /// Appends a null value to the array.
    pub fn append_null(&mut self) {
        self.term_type.append_null();
        self.value.append_null();
        self.data_type.append_null();
        self.language_tag.append_null();
        self.null_buffer.append_null();
    }

    /// Appends a name node to the array.
    pub fn append_named_node(&mut self, named_node: NamedNodeRef<'_>) {
        self.append(PlainTermType::NamedNode, named_node.as_str(), None, None);
    }

    /// Appends a blank node to the array.
    pub fn append_blank_node(&mut self, blank_node: BlankNodeRef<'_>) {
        self.append(PlainTermType::BlankNode, blank_node.as_str(), None, None);
    }

    /// Appends a named or blank node to the array.
    pub fn append_named_or_blank_node(&mut self, node: NamedOrBlankNodeRef<'_>) {
        match node {
            NamedOrBlankNodeRef::NamedNode(nn) => self.append_named_node(nn),
            NamedOrBlankNodeRef::BlankNode(bnode) => self.append_blank_node(bnode),
        }
    }

    /// Appends a graph name to the array.
    pub fn append_graph_name(&mut self, graph_name: GraphNameRef<'_>) {
        match graph_name {
            GraphNameRef::NamedNode(nn) => self.append_named_node(nn),
            GraphNameRef::BlankNode(bnode) => self.append_blank_node(bnode),
            GraphNameRef::DefaultGraph => self.append_null(),
        }
    }

    /// Appends a literal to the array.
    ///
    /// This encoding retains invalid lexical values for typed RDF literals.
    pub fn append_literal(&mut self, literal: LiteralRef<'_>) {
        self.append(
            PlainTermType::Literal,
            literal.value(),
            Some(literal.datatype().as_str()),
            literal.language(),
        );
    }

    /// Appends an arbitrary RDF term to the array.
    ///
    /// This encoding retains invalid lexical values for typed RDF literals.
    pub fn append_term(&mut self, literal: TermRef<'_>) {
        match literal {
            TermRef::NamedNode(nn) => self.append_named_node(nn),
            TermRef::BlankNode(bnode) => self.append_blank_node(bnode),
            TermRef::Literal(lit) => self.append_literal(lit),
        }
    }

    /// Appends the given RDF term to the array.
    ///
    /// All literals must pass a `data_type`.
    fn append(
        &mut self,
        term_type: PlainTermType,
        value: &str,
        data_type: Option<&str>,
        language_tag: Option<&str>,
    ) {
        assert!(
            !(term_type == PlainTermType::Literal && data_type.is_none()),
            "Literal term must have a data type"
        );

        self.append_raw(term_type as i8, value, data_type, language_tag);
    }

    /// Appends to the builder without any validation.
    pub fn append_raw(
        &mut self,
        term_type: i8,
        value: &str,
        data_type: Option<&str>,
        language_tag: Option<&str>,
    ) {
        self.null_buffer.append_non_null();
        self.term_type.append_value(term_type);
        self.value.append_value(value);
        self.data_type.append_option(data_type);
        self.language_tag.append_option(language_tag);
    }

    /// Returns the number of elements in the builder.
    pub fn len(&self) -> usize {
        self.term_type.len()
    }

    /// Returns true if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(mut self) -> PlainTermArray {
        PlainTermArray::new_unchecked(Arc::new(StructArray::new(
            PlainTermEncoding::fields(),
            vec![
                Arc::new(self.term_type.finish()),
                Arc::new(self.value.finish()),
                Arc::new(self.data_type.finish()),
                Arc::new(self.language_tag.finish()),
            ],
            self.null_buffer.finish(),
        )))
    }
}
