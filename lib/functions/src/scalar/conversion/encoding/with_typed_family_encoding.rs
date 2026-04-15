use crate::scalar::args::ScalarSparqlFunctionArgs;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub fn with_typed_family_encoding(encodings: RdfFusionEncodings) -> ScalarUDF {
    let udf_impl = WithTypedFamilyEncoding::new(encodings);
    ScalarUDF::new_from_impl(udf_impl)
}

/// Transforms RDF Terms into the [TypedFamilyEncoding](rdf_fusion_encoding::typed_family::TypedFamilyEncoding).
#[derive(Debug, PartialEq, Eq)]
struct WithTypedFamilyEncoding {
    /// The name of this function
    name: String,
    /// The signature of this function
    signature: Signature,
    /// The registered encodings
    encodings: RdfFusionEncodings,
}

impl WithTypedFamilyEncoding {
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        Self {
            name: BuiltinName::WithTypedFamilyEncoding.to_string(),
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

impl ScalarUDFImpl for WithTypedFamilyEncoding {
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
        Ok(self.encodings.typed_family().data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let was_scalar =
            !args.args.is_empty() && matches!(args.args[0], ColumnarValue::Scalar(_));
        let sparql_args =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        let result_array = match sparql_args.downcast_arrays() {
            Some(DowncastEncodingArrays::PlainTerm(arrays)) => {
                let array = arrays.get(0);
                self.encodings
                    .typed_family()
                    .cast_from_plain_term_array(array)?
                    .into_array_ref()
            }
            Some(DowncastEncodingArrays::TypedFamily(arrays)) => {
                Arc::clone(arrays.get(0).inner())
            }
            Some(DowncastEncodingArrays::ObjectId(arrays)) => {
                match &self.encodings.object_id() {
                    None => {
                        return exec_err!(
                            "Cannot from object id as no encoding is provided."
                        );
                    }
                    Some(encoding) => {
                        let array = arrays.get(0);
                        let decoded = encoding.mapping().decode_array_to_typed_family(
                            self.encodings.typed_family(),
                            array.inner(),
                        )?;
                        decoded.into_array_ref()
                    }
                }
            }
            _ => {
                return exec_err!(
                    "Cannot convert to typed family encoding for arguments: {:?}",
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

impl Hash for WithTypedFamilyEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}
