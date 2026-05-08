use crate::scalar::args::ScalarSparqlFunctionArgs;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::compute::with_string_encoding_from_plain_term;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingDatum, EncodingName, RdfFusionEncodings,
    TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

/// A function that encodes the given values with the String term encoding.
pub fn with_string_encoding(encodings: RdfFusionEncodings) -> ScalarUDF {
    let udf_impl = WithStringEncoding::new(encodings);
    ScalarUDF::new_from_impl(udf_impl)
}

/// Transforms RDF Terms into the [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding).
#[derive(Debug, PartialEq, Eq, Hash)]
struct WithStringEncoding {
    /// The name of this function
    name: String,
    /// The signature of this function
    signature: Signature,
    /// The registered encodings
    encodings: RdfFusionEncodings,
}

impl WithStringEncoding {
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        Self {
            name: BuiltinName::WithStringEncoding.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(
                    1,
                    encodings.get_data_types(&[EncodingName::PlainTerm]),
                ),
                Volatility::Immutable,
            ),
            encodings,
        }
    }
}

impl ScalarUDFImpl for WithStringEncoding {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        panic!("This function should not be called. See return_field_from_args.")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(
            "",
            STRING_ENCODING.data_type().clone(),
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let was_scalar =
            !args.args.is_empty() && matches!(args.args[0], ColumnarValue::Scalar(_));
        let sparql_args =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        match sparql_args.downcast_arrays() {
            Some(DowncastEncodingArgs::PlainTerm(arrays)) => {
                let result_array = with_string_encoding_from_plain_term(
                    &EncodingDatum::Array(arrays.get(0).clone()),
                )?;
                if was_scalar {
                    let scalar_value =
                        ScalarValue::try_from_array(&result_array.into_array_ref(), 0)?;
                    Ok(ColumnarValue::Scalar(scalar_value))
                } else {
                    Ok(ColumnarValue::Array(result_array.into_array_ref()))
                }
            }
            _ => {
                exec_err!(
                    "Cannot convert to plain term encoding for arguments: {:?}",
                    args.args
                )
            }
        }
    }
}
