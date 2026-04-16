use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::{
    Array, ArrayRef, Decimal128Builder, Int16Builder, UInt8Builder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::{
    DateTimeArrayBuilder, DateTimeFamily, DowncastTypedFamilyArray, StringFamilyArray,
    TypedFamily, TypedFamilyArray,
};
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::{AResult, DFResult, DateTime, Decimal};
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// Implementation of the SPARQL `xsd:dateTime()` cast function.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - CAST](https://www.w3.org/TR/sparql11-query/#func-cast)
pub fn cast_datetime_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CastDateTimeSparqlOp::new(
        encodings,
    )))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CastDateTimeSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for CastDateTimeSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CastDateTimeSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl CastDateTimeSparqlOp {
    /// Create a new [`CastDateTimeSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::CastDateTime.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }

    fn invoke_tf(&self, tf_array: &TypedFamilyArray) -> AResult<TypedFamilyArray> {
        let encoding = self.encodings.typed_family();
        tf_array.map_unary_tf(|child| match child.as_downcast_array() {
            DowncastTypedFamilyArray::DateTime(array) => {
                encoding.create_array_from_family(array)
            }
            DowncastTypedFamilyArray::String(array) => {
                let values = cast_string_to_datetime(&array)?;
                encoding
                    .create_array_with_single_family(DateTimeFamily::FAMILY_ID, values)
            }
            _ => encoding.create_null_array(child.to_array().len()),
        })
    }
}

impl ScalarUDFImpl for CastDateTimeSparqlOp {
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
        let encoding_name = detect_encoding_from_types(&self.encodings, arg_types)?;

        match encoding_name {
            Some(EncodingName::TypedFamily) => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            _ => exec_err!("xsd:dateTime is only supported for TypedFamily encoding"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => {
                self.invoke_tf(&tf_args.get(0))?.into_array_ref()
            }
            _ => exec_err!("xsd:dateTime is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
    }
}

fn cast_string_to_datetime(args: &StringFamilyArray) -> AResult<ArrayRef> {
    let len = args.value_array().len();
    let mut type_ids = UInt8Builder::with_capacity(len);
    let mut values = Decimal128Builder::with_capacity(len)
        .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
        .expect("Valid configuration");
    let mut offsets = Int16Builder::with_capacity(len);

    for i in 0..len {
        let is_simple = args.language_array().is_null(i);
        let parsed = args.value_array().value(i).parse::<DateTime>();

        if !is_simple || parsed.is_err() {
            type_ids.append_null();
            values.append_null();
            offsets.append_null();
            continue;
        }

        let parsed = parsed.expect("Checked above");
        type_ids.append_value(DateTimeFamily::DATE_TIME_TYPE_ID);
        values.append_value(i128::from_be_bytes(
            parsed.timestamp().value().to_be_bytes(),
        ));
        offsets.append_option(parsed.timestamp().offset().map(|o| o.in_minutes()));
    }

    let res_array =
        DateTimeArrayBuilder::new(type_ids.finish(), values.finish(), offsets.finish())
            .finish()
            .map_err(|e| ArrowError::ComputeError(e.to_string()))?;

    Ok(res_array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_default_encodings, create_standard_test_vector, evaluate_function_for_test,
    };
    use insta::assert_snapshot;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_cast_date_time_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(cast_datetime_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+
        | input                                                                                        | xsd:dateTime(?table?.input)                                                                  |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.null=}                                                                           |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+
        ");
    }
}
