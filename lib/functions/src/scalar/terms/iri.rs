use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{Array, StringBuilder};
use datafusion::arrow::compute::filter;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, NullFamilyArray, ResourceFamily, ResourceFamilyArray,
    StringFamilyArray, TypedFamilyArray, TypedFamilyArrayBuilder, TypedFamilyEncoding,
    TypedFamilyEncodingRef, TypedFamilyId,
};
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::{AResult, DFResult, Iri};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Implementation of the SPARQL `IRI` function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - IRI](https://www.w3.org/TR/sparql11-query/#func-iri)
pub fn iri_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(IriSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct IriSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for IriSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IriSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl IriSparqlOp {
    /// Create a new [`IriSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .with_binary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::Iri.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IriSparqlOp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match detect_encoding_from_types(&self.encodings, arg_types)? {
            Some(EncodingName::TypedFamily) => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            _ => exec_err!("Unsupported encoding for IRI return type"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args_wrapped =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args_wrapped.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => tf_args
                .map_children_tf(|children| {
                    let family_len = children[0].to_array().len();
                    let children = children
                        .iter()
                        .map(|c| c.as_downcast_array())
                        .collect::<Vec<_>>();

                    match children.as_slice() {
                        // Binary String (Input + Base)
                        [
                            DowncastTypedFamilyArray::String(i_arr),
                            DowncastTypedFamilyArray::String(b_arr),
                        ] => iri_string_impl(tf_encoding, i_arr, Some(b_arr)),
                        // Binary String + Null (Input + No Base)
                        [
                            DowncastTypedFamilyArray::String(i_arr),
                            DowncastTypedFamilyArray::Null(_),
                        ] => iri_string_impl(tf_encoding, i_arr, None),
                        // Unary String
                        [DowncastTypedFamilyArray::String(i_arr)] => {
                            iri_string_impl(tf_encoding, i_arr, None)
                        }
                        // Unary Resource (If the argument is an IRI, it is returned)
                        [DowncastTypedFamilyArray::Resource(r_arr)] => {
                            iri_resource_impl(tf_encoding, r_arr)
                        }
                        _ => tf_encoding.create_null_array(family_len),
                    }
                })?
                .into_array_ref(),
            _ => exec_err!("IRI is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
    }
}

fn iri_string_impl(
    encoding: &TypedFamilyEncodingRef,
    i_arr: &StringFamilyArray,
    b_arr: Option<&StringFamilyArray>,
) -> AResult<TypedFamilyArray> {
    let input = i_arr.value_array();
    let base = b_arr.map(|b| b.value_array());

    let mut res_values = StringBuilder::with_capacity(input.len(), input.len() * 20);

    for i in 0..input.len() {
        let is_base_null = base.as_ref().is_some_and(|b| b.is_null(i));

        if input.is_null(i) || is_base_null {
            res_values.append_null();
        } else {
            let input_val = input.value(i);

            let resolved = if let Some(base_arr) = &base {
                let base_val = base_arr.value(i);
                Iri::parse(base_val)
                    .and_then(|b| b.resolve(input_val).map(|iri| iri.to_string()))
            } else {
                Iri::parse(input_val).map(|iri| iri.to_string())
            };

            if let Ok(resolved_str) = resolved {
                res_values.append_value(&resolved_str);
            } else {
                res_values.append_null();
            }
        }
    }

    let iris = ResourceFamily::create_named_nodes_array(res_values.finish())?;
    encoding.create_array_from_family(iris)
}

fn iri_resource_impl(
    encoding: &TypedFamilyEncodingRef,
    r_arr: &ResourceFamilyArray,
) -> AResult<TypedFamilyArray> {
    let mut res_tids = Vec::with_capacity(r_arr.inner().len());
    let mut res_offsets = Vec::with_capacity(r_arr.inner().len());
    let mut null_count = 0;
    let mut resource_count = 0;

    for i in 0..r_arr.inner().len() {
        if r_arr.is_named_node().value(i) {
            res_tids.push(
                encoding
                    .find_typed_family_type_id(TypedFamilyId::Resource)
                    .unwrap(),
            );
            res_offsets.push(resource_count);
            resource_count += 1;
        } else {
            res_tids.push(TypedFamilyEncoding::NULL_TYPE_ID);
            res_offsets.push(null_count as i32);
            null_count += 1;
        }
    }

    let filtered_resource = filter(r_arr.inner(), &r_arr.is_named_node())?;

    TypedFamilyArrayBuilder::new(Arc::clone(encoding), res_tids, res_offsets)?
        .with_nulls(NullFamilyArray::new(null_count))?
        .with_array(TypedFamilyId::Resource, Some(filtered_resource))?
        .finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_default_encodings;
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::typed_family::{StringFamily, TypedFamily};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_iri_typed_family() {
        let encodings = create_default_encodings();
        let encoding = encodings.typed_family();

        let input_array =
            Arc::new(StringArray::from(vec!["test", "http://example.org/bar"]))
                as ArrayRef;
        let input = encoding
            .create_array_with_single_family(
                StringFamily::FAMILY_ID,
                StringFamily::create_simple_strings_array(input_array),
            )
            .unwrap()
            .into_array_ref();

        let base_array = Arc::new(StringArray::from(vec![
            "http://example.org/",
            "http://example.org/",
        ])) as ArrayRef;
        let base = encoding
            .create_array_with_single_family(
                StringFamily::FAMILY_ID,
                StringFamily::create_simple_strings_array(base_array),
            )
            .unwrap()
            .into_array_ref();

        let udf = Arc::new(iri_udf(encodings).unwrap());

        let df = datafusion::prelude::DataFrame::from_columns(vec![
            ("input", input),
            ("base", base),
        ])
        .unwrap();

        let result = df
            .select(vec![udf.call(vec![col("input"), col("base")])])
            .unwrap();

        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r"
        +-------------------------------------------------------------+
        | IRI(?table?.input,?table?.base)                             |
        +-------------------------------------------------------------+
        | {rdf-fusion.resources={named_node=http://example.org/test}} |
        | {rdf-fusion.resources={named_node=http://example.org/bar}}  |
        +-------------------------------------------------------------+
        "
        );
    }
}
