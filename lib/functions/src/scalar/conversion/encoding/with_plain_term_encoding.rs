use crate::scalar::args::ScalarSparqlFunctionArgs;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub fn with_plain_term_encoding(encodings: RdfFusionEncodings) -> ScalarUDF {
    let udf_impl = WithPlainTermEncoding::new(encodings);
    ScalarUDF::new_from_impl(udf_impl)
}

/// Transforms RDF Terms into the [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding).
#[derive(Debug, PartialEq, Eq)]
struct WithPlainTermEncoding {
    /// The name of this function
    name: String,
    /// The signature of this function
    signature: Signature,
    /// The registered encodings
    encodings: RdfFusionEncodings,
}

impl WithPlainTermEncoding {
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        Self {
            name: BuiltinName::WithPlainTermEncoding.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(
                    1,
                    encodings.get_data_types(&[
                        EncodingName::PlainTerm,
                        EncodingName::TypedFamily,
                        EncodingName::ObjectId,
                    ]),
                ),
                Volatility::Immutable,
            ),
            encodings,
        }
    }
}

impl ScalarUDFImpl for WithPlainTermEncoding {
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
            "output",
            PLAIN_TERM_ENCODING.data_type().clone(),
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let was_scalar =
            !args.args.is_empty() && matches!(args.args[0], ColumnarValue::Scalar(_));
        let sparql_args =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        let result_array = match sparql_args.downcast_arrays() {
            Some(DowncastEncodingArgs::PlainTerm(arrays)) => {
                Arc::clone(arrays.get(0).inner())
            }
            Some(DowncastEncodingArgs::TypedFamily(arrays)) => {
                let array = arrays.get(0);
                array.as_plain_term_array()?.into_array_ref()
            }
            Some(DowncastEncodingArgs::ObjectId(arrays)) => {
                match &self.encodings.object_id() {
                    None => {
                        return exec_err!(
                            "Cannot from object id as no encoding is provided."
                        );
                    }
                    Some(encoding) => {
                        let array = arrays.get(0);
                        let decoded = encoding.mapping().decode_array(array.inner())?;
                        decoded.into_array_ref()
                    }
                }
            }
            _ => {
                if args.args.is_empty() {
                    return Ok(ColumnarValue::Array(
                        PLAIN_TERM_ENCODING.create_null_array(0).into_array_ref(),
                    ));
                }
                return exec_err!(
                    "Cannot convert to plain term encoding for arguments: {:?}",
                    args.args
                );
            }
        };

        if was_scalar {
            let scalar_value = ScalarValue::try_from_array(&result_array, 0)?;
            Ok(ColumnarValue::Scalar(scalar_value))
        } else {
            Ok(ColumnarValue::Array(result_array))
        }
    }
}

impl Hash for WithPlainTermEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}
