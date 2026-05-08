use crate::TermEncoding;
use crate::encoding::EncodingScalar;
use crate::object_id::{ObjectIdEncoding, ObjectIdEncodingRef};
use crate::plain_term::{PLAIN_TERM_ENCODING, PlainTermEncoding};
use crate::string::STRING_ENCODING;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{DFResult, TermRef};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use thiserror::Error;

/// Defines which encoding is used for retrieving quads from the storage.
///
/// Defining this is necessary such that the query planner knows what type should be assigned to the
/// schema of quad pattern logical nodes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QuadStorageEncoding {
    /// Uses the plain term encoding.
    ///
    /// Currently, the plain term encoding is not parameterizable. Therefore, this variant has no
    /// further information.
    PlainTerm,
    /// Uses the provided object id encoding.
    ObjectId(ObjectIdEncodingRef),
    /// Uses the string encoding.
    ///
    /// Currently, the string encoding is not parameterizable. Therefore, this variant has no
    /// further information.
    String,
}

/// A version of [`QuadStorageEncoding`] that only reflects the name of the encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuadStorageEncodingName {
    /// See [`QuadStorageEncoding::PlainTerm`]
    PlainTerm,
    /// See [`QuadStorageEncoding::ObjectId`]
    ObjectId,
    /// See [`QuadStorageEncoding::String`]
    String,
}

impl Display for QuadStorageEncodingName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QuadStorageEncodingName::PlainTerm => f.write_str("PlainTerm"),
            QuadStorageEncodingName::ObjectId => f.write_str("ObjectId"),
            QuadStorageEncodingName::String => f.write_str("String"),
        }
    }
}

#[derive(Debug, Error)]
#[error(
    "Invalid quad storage encoding: {0}. Supported encodings: plain-term, object-id."
)]
pub struct QuadStorageEncodingNameParserError(String);

impl FromStr for QuadStorageEncodingName {
    type Err = QuadStorageEncodingNameParserError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PlainTerm" => Ok(QuadStorageEncodingName::PlainTerm),
            "ObjectId" => Ok(QuadStorageEncodingName::ObjectId),
            "String" => Ok(QuadStorageEncodingName::String),
            _ => Err(QuadStorageEncodingNameParserError(s.to_string())),
        }
    }
}

static PLAIN_TERM_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, PlainTermEncoding::data_type(), true),
        Field::new(COL_SUBJECT, PlainTermEncoding::data_type(), false),
        Field::new(COL_PREDICATE, PlainTermEncoding::data_type(), false),
        Field::new(COL_OBJECT, PlainTermEncoding::data_type(), false),
    ]))
});

static PLAIN_TERM_QUAD_DFSCHEMA: LazyLock<DFSchemaRef> = LazyLock::new(|| {
    DFSchemaRef::new(DFSchema::try_from(PLAIN_TERM_QUAD_SCHEMA.clone()).unwrap())
});

static STRING_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, STRING_ENCODING.data_type().clone(), true),
        Field::new(COL_SUBJECT, STRING_ENCODING.data_type().clone(), false),
        Field::new(COL_PREDICATE, STRING_ENCODING.data_type().clone(), false),
        Field::new(COL_OBJECT, STRING_ENCODING.data_type().clone(), false),
    ]))
});

static STRING_QUAD_DFSCHEMA: LazyLock<DFSchemaRef> = LazyLock::new(|| {
    DFSchemaRef::new(DFSchema::try_from(STRING_QUAD_SCHEMA.clone()).unwrap())
});

impl QuadStorageEncoding {
    /// Returns the data type of a single term column, given the current encoding.
    pub fn term_type(&self) -> &DataType {
        match self {
            QuadStorageEncoding::PlainTerm => PLAIN_TERM_ENCODING.data_type(),
            QuadStorageEncoding::ObjectId(enc) => enc.data_type(),
            QuadStorageEncoding::String => STRING_ENCODING.data_type(),
        }
    }

    /// Returns the schema of an entire quad, given the current encoding.
    pub fn quad_schema(&self) -> DFSchemaRef {
        match self {
            QuadStorageEncoding::PlainTerm => PLAIN_TERM_QUAD_DFSCHEMA.clone(),
            QuadStorageEncoding::ObjectId(encoding) => object_id_quad_schema(encoding),
            QuadStorageEncoding::String => STRING_QUAD_DFSCHEMA.clone(),
        }
    }

    /// Returns an optional reference to the contained [ObjectIdEncoding].
    ///
    /// Returns [None] otherwise.
    pub fn object_id_encoding(&self) -> Option<&ObjectIdEncodingRef> {
        match &self {
            QuadStorageEncoding::ObjectId(encoding) => Some(encoding),
            _ => None,
        }
    }

    /// Encodes the given term into a [ScalarValue] that can be used for filtering.
    pub fn encode_term_scalar(&self, term: TermRef<'_>) -> DFResult<ScalarValue> {
        match self {
            QuadStorageEncoding::PlainTerm => Ok(PLAIN_TERM_ENCODING
                .encode_term(Ok(term))?
                .into_scalar_value()),
            QuadStorageEncoding::ObjectId(enc) => {
                let pt_scalar = PLAIN_TERM_ENCODING.encode_term(Ok(term))?;
                Ok(enc.encode_scalar(&pt_scalar)?.into_scalar_value())
            }
            QuadStorageEncoding::String => {
                Ok(STRING_ENCODING.encode_term(Ok(term))?.into_scalar_value())
            }
        }
    }

    /// Creates a null scalar for the given storage encoding.
    pub fn create_null_scalar(&self) -> DFResult<ScalarValue> {
        ScalarValue::try_new_null(self.term_type())
    }
}

impl Display for QuadStorageEncoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QuadStorageEncoding::PlainTerm => write!(f, "PlainTerm"),
            QuadStorageEncoding::ObjectId(encoding) => {
                write!(f, "ObjectId({})", encoding.object_id_data_type())
            }
            QuadStorageEncoding::String => {
                write!(f, "String")
            }
        }
    }
}

/// Computes the quad schema based on the given [ObjectIdEncoding].
fn object_id_quad_schema(encoding: &ObjectIdEncoding) -> DFSchemaRef {
    let data_type = encoding.data_type();
    Arc::new(
        DFSchema::from_unqualified_fields(
            Fields::from(vec![
                Field::new(COL_GRAPH, data_type.clone(), true),
                Field::new(COL_SUBJECT, data_type.clone(), false),
                Field::new(COL_PREDICATE, data_type.clone(), false),
                Field::new(COL_OBJECT, data_type.clone(), false),
            ]),
            HashMap::new(),
        )
        .expect("Fields are fixed"),
    )
}

#[cfg(test)]
mod test {
    use crate::QuadStorageEncodingName;

    #[test]
    fn test_quad_storage_name_parsing_round_trip() {
        test_parsing_round_trip(QuadStorageEncodingName::PlainTerm);
        test_parsing_round_trip(QuadStorageEncodingName::ObjectId);

        fn test_parsing_round_trip(name: QuadStorageEncodingName) {
            let serialized = name.to_string();
            assert_eq!(serialized.parse::<QuadStorageEncodingName>().unwrap(), name);
        }
    }
}
