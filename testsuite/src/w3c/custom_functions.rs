//! Registers the custom-IRI functions exercised by the W3C syntax test suite.
//!
//! Several positive syntax tests call functions under example IRIs such as
//! `http://example.org/ns#myFunc`. Each is registered as a standalone function
//! whose arity matches how the corresponding tests invoke it, so that planning
//! (not just tokenizing/parsing) succeeds.

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion::encoding::plain_term::{PLAIN_TERM_ENCODING, PlainTermArray};
use rdf_fusion::encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion::extensions::RdfFusionContextView;
use rdf_fusion::functions::scalar::SparqlUDFTypeSignatureBuilder;
use rdf_fusion::functions::scalar::signature::SparqlUDFArity;
use rdf_fusion_common::DFResult;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// A concrete scalar function registered under a custom IRI with a matching arity.
#[derive(Clone, PartialEq, Eq, Hash)]
struct TestCustomFunction {
    name: String,
    aliases: Vec<String>,
    signature: Signature,
}

impl Debug for TestCustomFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestCustomFunction")
            .field("name", &self.name)
            .finish()
    }
}

impl TestCustomFunction {
    fn new(iri: &str, encodings: &RdfFusionEncodings, arity: SparqlUDFArity) -> Self {
        let type_signature = SparqlUDFTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_arity(arity)
            .build();
        Self {
            name: format!("<{iri}>"),
            aliases: vec![iri.to_string()],
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TestCustomFunction {
    fn name(&self) -> &str {
        &self.name
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(PLAIN_TERM_ENCODING.data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // These functions are only exercised by syntax/planning tests and are
        // never actually executed. Return nulls of the expected length.
        let array = PlainTermArray::new_null(args.number_rows);
        Ok(ColumnarValue::Array(array.into_array_ref()))
    }
}

/// Registers the custom-IRI functions used by the W3C syntax tests.
pub fn register_custom_functions(context_view: &RdfFusionContextView) {
    let encodings = context_view.encodings();
    let definitions = [
        (
            "http://example.org/ns#myFunc",
            SparqlUDFArity::Fixed(NonZeroUsize::new(2).unwrap()),
        ),
        (
            "http://example.org/ns#func",
            SparqlUDFArity::Fixed(NonZeroUsize::new(2).unwrap()),
        ),
        (
            "http://example.org/ns#func2",
            SparqlUDFArity::Fixed(NonZeroUsize::new(1).unwrap()),
        ),
        ("http://example.org/name", SparqlUDFArity::Variadic),
        (
            "http://example/function",
            SparqlUDFArity::Fixed(NonZeroUsize::new(1).unwrap()),
        ),
    ];
    for (iri, arity) in definitions {
        let udf =
            ScalarUDF::new_from_impl(TestCustomFunction::new(iri, encodings, arity));
        context_view.functions().register_udf(Arc::new(udf));
    }
}
