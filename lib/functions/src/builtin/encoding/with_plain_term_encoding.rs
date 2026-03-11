use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use rdf_fusion_encoding::object_id::ObjectId;
use rdf_fusion_encoding::plain_term::encoders::TypedValueRefPlainTermEncoder;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::{
    EncodingArray, EncodingName, EncodingScalar, RdfFusionEncodings, TermDecoder,
    TermEncoder, TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::hash::{Hash, Hasher};

pub fn with_plain_term_encoding(encodings: RdfFusionEncodings) -> ScalarUDF {
    let udf_impl = WithPlainTermEncoding::new(encodings);
    ScalarUDF::new_from_impl(udf_impl)
}

/// Transforms RDF Terms into the [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding).
#[derive(Debug, PartialEq, Eq)]
struct WithPlainTermEncoding {
    /// The name of the UDF
    name: String,
    /// The signature
    signature: Signature,
    /// A reference to used encodings.
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
                        EncodingName::TypedValue,
                        EncodingName::ObjectId,
                    ]),
                ),
                Volatility::Immutable,
            ),
            encodings,
        }
    }

    fn convert_array(
        &self,
        encoding_name: EncodingName,
        array: ArrayRef,
    ) -> DFResult<ColumnarValue> {
        match encoding_name {
            EncodingName::PlainTerm => Ok(ColumnarValue::Array(array)),
            EncodingName::TypedValue => {
                let array = self.encodings.typed_value().try_new_array(array)?;
                let input = DefaultTypedValueDecoder::decode_terms(&array);
                let result = TypedValueRefPlainTermEncoder.encode_terms(input)?;
                Ok(ColumnarValue::Array(result.into_array_ref()))
            }
            EncodingName::Sortable => exec_err!("Cannot from sortable term."),
            EncodingName::ObjectId => match self.encodings.object_id() {
                None => exec_err!("Cannot from object id as no encoding is provided."),
                Some(encoding) => {
                    let array = encoding.try_new_array(array)?;
                    let decoded = encoding.mapping().decode_array(array.object_ids())?;
                    Ok(ColumnarValue::Array(decoded.into_array_ref()))
                }
            },
        }
    }

    fn convert_scalar(
        &self,
        encoding_name: EncodingName,
        scalar: ScalarValue,
    ) -> DFResult<ColumnarValue> {
        match encoding_name {
            EncodingName::PlainTerm => Ok(ColumnarValue::Scalar(scalar)),
            EncodingName::TypedValue => {
                let scalar = self.encodings.typed_value().try_new_scalar(scalar)?;
                let input = DefaultTypedValueDecoder::decode_term(&scalar);
                let result = TypedValueRefPlainTermEncoder.encode_term(input)?;
                Ok(ColumnarValue::Scalar(result.into_scalar_value()))
            }
            EncodingName::Sortable => exec_err!("Cannot from sortable term."),
            EncodingName::ObjectId => match self.encodings.object_id() {
                None => exec_err!("Cannot from object id as no encoding is provided."),
                Some(encoding) => {
                    let oid = ObjectId::from(encoding.try_new_scalar(scalar)?);
                    let decoded = encoding.mapping().decode_scalar(&oid)?;
                    Ok(ColumnarValue::Scalar(decoded.into_scalar_value()))
                }
            },
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

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion::common::Result<DataType> {
        exec_err!("return_field_from_args should be called")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> DFResult<FieldRef> {
        if args.arg_fields.len() != 1 {
            return plan_err!(
                "Unexpected number of arg fields in return_field_from_args."
            );
        }

        let data_type = PLAIN_TERM_ENCODING.data_type().clone();
        let incoming_null = args.arg_fields[0].is_nullable();
        Ok(FieldRef::new(Field::new(
            "output",
            data_type,
            incoming_null,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = TryInto::<[ColumnarValue; 1]>::try_into(args.args)
            .map_err(|_| exec_datafusion_err!("Invalid number of arguments."))?;
        let encoding_name = self
            .encodings
            .try_get_encoding_name(&args[0].data_type())
            .ok_or(exec_datafusion_err!(
                "Cannot obtain encoding from argument."
            ))?;

        match args {
            [ColumnarValue::Array(array)] => self.convert_array(encoding_name, array),
            [ColumnarValue::Scalar(scalar)] => self.convert_scalar(encoding_name, scalar),
        }
    }
}

impl Hash for WithPlainTermEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}
