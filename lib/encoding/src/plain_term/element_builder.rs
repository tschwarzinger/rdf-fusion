use crate::plain_term::encoding::{PlainTermEncodingField, PlainTermType};
use crate::plain_term::{PlainTermArray, PlainTermEncoding};
use datafusion::arrow::array::{StringBuilder, StructBuilder, UInt8Builder};
use rdf_fusion_model::{
    BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef, NamedOrBlankNodeRef, TermRef,
};
use std::sync::Arc;

/// Provides a convenient API for building arrays (element-by-element) of RDF terms with the
/// [PlainTermEncoding]. The documentation of the encoding provides additional information.
pub struct PlainTermArrayElementBuilder {
    /// The underlying [StructBuilder].
    builder: StructBuilder,
}

impl Default for PlainTermArrayElementBuilder {
    fn default() -> Self {
        Self::new(0)
    }
}

impl PlainTermArrayElementBuilder {
    /// Create a [PlainTermArrayElementBuilder] with the given `capacity`.
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: StructBuilder::from_fields(PlainTermEncoding::fields(), capacity),
        }
    }

    /// Appends a null value to the array.
    pub fn append_null(&mut self) {
        self.builder
            .field_builder::<UInt8Builder>(PlainTermEncodingField::TermType.index())
            .unwrap()
            .append_null();
        self.builder
            .field_builder::<StringBuilder>(PlainTermEncodingField::Value.index())
            .unwrap()
            .append_null();
        self.builder
            .field_builder::<StringBuilder>(PlainTermEncodingField::DataType.index())
            .unwrap()
            .append_null();
        self.builder
            .field_builder::<StringBuilder>(PlainTermEncodingField::LanguageTag.index())
            .unwrap()
            .append_null();
        self.builder.append(false)
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

        self.builder
            .field_builder::<UInt8Builder>(PlainTermEncodingField::TermType.index())
            .unwrap()
            .append_value(term_type.into());

        self.builder
            .field_builder::<StringBuilder>(PlainTermEncodingField::Value.index())
            .unwrap()
            .append_value(value);

        let data_type_builder = self
            .builder
            .field_builder::<StringBuilder>(PlainTermEncodingField::DataType.index())
            .unwrap();
        match data_type {
            None => data_type_builder.append_null(),
            Some(data_type) => data_type_builder.append_value(data_type),
        }

        let language_tag_builder = self
            .builder
            .field_builder::<StringBuilder>(PlainTermEncodingField::LanguageTag.index())
            .unwrap();
        match language_tag {
            None => language_tag_builder.append_null(),
            Some(language_tag) => language_tag_builder.append_value(language_tag),
        }

        self.builder.append(true)
    }

    pub fn finish(mut self) -> PlainTermArray {
        PlainTermArray::new_unchecked(Arc::new(self.builder.finish()))
    }
}
