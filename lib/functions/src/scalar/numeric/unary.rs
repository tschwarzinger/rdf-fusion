use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::typed_family::DowncastTypedFamilyArray;
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, RdfFusionEncodings, TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};

pub fn abs_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(NumericUnarySparqlOp::new(
        encodings,
        BuiltinName::Abs.to_string(),
        NumericUnaryOperation::Abs,
    )))
}

pub fn ceil_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(NumericUnarySparqlOp::new(
        encodings,
        BuiltinName::Ceil.to_string(),
        NumericUnaryOperation::Ceil,
    )))
}

pub fn floor_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(NumericUnarySparqlOp::new(
        encodings,
        BuiltinName::Floor.to_string(),
        NumericUnaryOperation::Floor,
    )))
}

pub fn round_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(NumericUnarySparqlOp::new(
        encodings,
        BuiltinName::Round.to_string(),
        NumericUnaryOperation::Round,
    )))
}

pub fn unary_minus_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(NumericUnarySparqlOp::new(
        encodings,
        BuiltinName::UnaryMinus.to_string(),
        NumericUnaryOperation::Minus,
    )))
}

pub fn unary_plus_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(NumericUnarySparqlOp::new(
        encodings,
        BuiltinName::UnaryPlus.to_string(),
        NumericUnaryOperation::Plus,
    )))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum NumericUnaryOperation {
    Abs,
    Ceil,
    Floor,
    Round,
    Minus,
    Plus,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct NumericUnarySparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    op: NumericUnaryOperation,
    signature: Signature,
}

impl Debug for NumericUnarySparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NumericUnarySparqlOp")
            .field("name", &self.name)
            .field("op", &self.op)
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl NumericUnarySparqlOp {
    fn new(
        encodings: RdfFusionEncodings,
        name: String,
        op: NumericUnaryOperation,
    ) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            name,
            op,
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NumericUnarySparqlOp {
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
        let args_wrapped =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args_wrapped.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => tf_args
                .map_children_tf_unary(|child| match child.as_downcast_array() {
                    DowncastTypedFamilyArray::Numeric(array) => {
                        let res = match self.op {
                            NumericUnaryOperation::Abs => array.abs()?,
                            NumericUnaryOperation::Ceil => array.ceil()?,
                            NumericUnaryOperation::Floor => array.floor()?,
                            NumericUnaryOperation::Round => array.round()?,
                            NumericUnaryOperation::Minus => array.neg()?,
                            NumericUnaryOperation::Plus => array.clone(),
                        };
                        Ok(tf_encoding.create_array_from_family(res)?)
                    }
                    _ => tf_encoding.create_null_array(child.to_array().len()),
                })?
                .into_array_ref(),
            _ => exec_err!("{} is only supported for TypedFamily encoding", self.name)?,
        };

        Ok(ColumnarValue::Array(result))
    }
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
    async fn test_abs_typed_family() {
        let encodings = create_default_encodings();
        let test_vector = create_standard_test_vector(encodings.typed_family());
        let udf = Arc::new(abs_udf(encodings).unwrap());
        let result = evaluate_function_for_test(test_vector, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+------------------------------------------------------+
        | input                                                                                        | ABS(?table?.input)                                   |
        +----------------------------------------------------------------------------------------------+------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                   |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                   |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                   |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                   |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}                    |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=10.0}}                    |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={float=0.0}}                     |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={double=20.0}}                   |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={decimal=30.000000000000000000}} |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={int=40}}                        |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}                                   |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                                   |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                                   |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.null=}                                   |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                   |
        +----------------------------------------------------------------------------------------------+------------------------------------------------------+
        "
        );
    }
}
