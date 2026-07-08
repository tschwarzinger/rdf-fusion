use crate::scalar::args::ScalarSparqlFunctionArgs;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{ScalarValue, exec_err};
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::object_id::ObjectIdMapping;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub fn decode_term(encodings: RdfFusionEncodings) -> Option<ScalarUDF> {
    let mapping = Arc::clone(encodings.object_id()?.mapping());
    // Clone encodings for the UDF (RdfFusionEncodings implements Clone)
    let encodings_clone = encodings.clone();
    let udf_impl = DecodeTermUDF::new(encodings_clone, mapping);
    Some(AsyncScalarUDF::new(Arc::new(udf_impl)).into_scalar_udf())
}

/// Transforms RDF Terms into the [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding).
struct DecodeTermUDF {
    /// The name of this function
    name: String,
    /// The signature of this function
    signature: Signature,
    /// The registered encodings
    encodings: RdfFusionEncodings,
    /// Mapping for object ID decoding
    mapping: Arc<dyn ObjectIdMapping>,
}

impl DecodeTermUDF {
    /// Creates a new [`DecodeTermUDF`] with full encodings and mapping.
    pub fn new(encodings: RdfFusionEncodings, mapping: Arc<dyn ObjectIdMapping>) -> Self {
        Self {
            name: BuiltinName::DecodeTerm.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(
                    1,
                    encodings.get_data_types(&[EncodingName::ObjectId]),
                ),
                Volatility::Volatile,
            ),
            encodings,
            mapping,
        }
    }
}

impl ScalarUDFImpl for DecodeTermUDF {
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
            Some(DowncastEncodingArgs::ObjectId(arrays)) => {
                let array = arrays.get(0);
                let decoded = self.mapping.decode_array(array.inner())?;
                decoded.into_array_ref()
            }
            _ => {
                return exec_err!(
                    "DECODE_PT only supports ObjectId encoding, got: {:?}",
                    args.args
                );
            }
        };

        if was_scalar {
            let scalar = ScalarValue::try_from_array(&result_array, 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        } else {
            Ok(ColumnarValue::Array(result_array))
        }
    }
}

impl Debug for DecodeTermUDF {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodeTermUDF").finish()
    }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for DecodeTermUDF {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        self.invoke_with_args(args)
    }
}

impl PartialEq for DecodeTermUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
            && self.signature.eq(&other.signature)
            && self.encodings == other.encodings
    }
}

impl Eq for DecodeTermUDF {}

impl Hash for DecodeTermUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}
