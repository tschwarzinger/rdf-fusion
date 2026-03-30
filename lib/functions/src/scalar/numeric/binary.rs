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
    DowncastEncodingArrays, EncodingArray, RdfFusionEncodings, TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::{Debug, Formatter};

pub fn add_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(NumericBinarySparqlOp::new(
        encodings,
        BuiltinName::Add.to_string(),
        NumericBinaryOpType::Add,
    )))
}

pub fn sub_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(NumericBinarySparqlOp::new(
        encodings,
        BuiltinName::Sub.to_string(),
        NumericBinaryOpType::Sub,
    )))
}

pub fn mul_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(NumericBinarySparqlOp::new(
        encodings,
        BuiltinName::Mul.to_string(),
        NumericBinaryOpType::Mul,
    )))
}

pub fn div_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(NumericBinarySparqlOp::new(
        encodings,
        BuiltinName::Div.to_string(),
        NumericBinaryOpType::Div,
    )))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum NumericBinaryOpType {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct NumericBinarySparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    op_type: NumericBinaryOpType,
    signature: Signature,
}

impl Debug for NumericBinarySparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NumericBinarySparqlOp")
            .field("name", &self.name)
            .field("op_type", &self.op_type)
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl NumericBinarySparqlOp {
    fn new(
        encodings: RdfFusionEncodings,
        name: String,
        op_type: NumericBinaryOpType,
    ) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_binary_arity()
            .build();
        Self {
            encodings,
            name,
            op_type,
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NumericBinarySparqlOp {
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
            Some(DowncastEncodingArrays::TypedFamily(tf_args)) => tf_args
                .map_children_tf_binary(|lhs, rhs| {
                    match (lhs.downcast(), rhs.downcast()) {
                        (
                            DowncastTypedFamilyArray::Numeric(l),
                            DowncastTypedFamilyArray::Numeric(r),
                        ) => {
                            let res = match self.op_type {
                                NumericBinaryOpType::Add => l.add(&r)?,
                                NumericBinaryOpType::Sub => l.sub(&r)?,
                                NumericBinaryOpType::Mul => l.mul(&r)?,
                                NumericBinaryOpType::Div => l.div(&r)?,
                            };
                            Ok(tf_encoding.create_array_from_family(res)?)
                        }
                        _ => tf_encoding.create_null_array(lhs.array().len()),
                    }
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
        create_default_encodings, create_standard_test_vector,
        evaluate_binary_function_for_test,
    };
    use insta::assert_snapshot;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_add_typed_family() {
        let encodings = create_default_encodings();
        let left = create_standard_test_vector(&encodings.typed_family());
        let right = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(add_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------------------------------------+
        | left                                                                                         | right                                                                                        | ADD(?table?.left,?table?.right)                      |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                   |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                   |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                   |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                   |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=20}}                    |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=20.0}}                    |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={float=0.0}}                     |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={double=40.0}}                   |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={decimal=60.000000000000000000}} |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={int=80}}                        |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}                                   |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                                   |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                                   |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.null=}                                   |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                   |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+------------------------------------------------------+
        "
        );
    }

    #[tokio::test]
    async fn test_sub_typed_family() {
        let encodings = create_default_encodings();
        let left = create_standard_test_vector(&encodings.typed_family());
        let right = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(sub_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-----------------------------------------------------+
        | left                                                                                         | right                                                                                        | SUB(?table?.left,?table?.right)                     |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-----------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                  |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                  |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                  |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                  |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=0}}                    |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=0.0}}                    |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={float=0.0}}                    |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={double=0.0}}                   |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={decimal=0.000000000000000000}} |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={int=0}}                        |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}                                  |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                                  |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                                  |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.null=}                                  |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                  |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-----------------------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_mul_typed_family() {
        let encodings = create_default_encodings();
        let left = create_standard_test_vector(&encodings.typed_family());
        let right = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(mul_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-------------------------------------------------------+
        | left                                                                                         | right                                                                                        | MUL(?table?.left,?table?.right)                       |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-------------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                    |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                    |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                    |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                    |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=100}}                    |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=100.0}}                    |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={float=0.0}}                      |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={double=400.0}}                   |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={decimal=900.000000000000000000}} |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={int=1600}}                       |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}                                    |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                                    |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                                    |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.null=}                                    |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                    |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-------------------------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_div_typed_family() {
        let encodings = create_default_encodings();
        let left = create_standard_test_vector(&encodings.typed_family());
        let right = create_standard_test_vector(&encodings.typed_family());
        let udf = Arc::new(div_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-----------------------------------------------------+
        | left                                                                                         | right                                                                                        | DIV(?table?.left,?table?.right)                     |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-----------------------------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                  |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.null=}                                  |
        | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.null=}                                  |
        | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.resources={blank_node=123456}}                                                   | {rdf-fusion.null=}                                  |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={decimal=1.000000000000000000}} |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=1.0}}                    |
        | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.numeric={float=NaN}}                    |
        | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={double=20.0}}                                                           | {rdf-fusion.numeric={double=1.0}}                   |
        | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={decimal=30.000000000000000000}}                                         | {rdf-fusion.numeric={decimal=1.000000000000000000}} |
        | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={int=40}}                                                                | {rdf-fusion.numeric={decimal=1.000000000000000000}} |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}                                  |
        | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.null=}                                  |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.null=}                                  |
        | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.strings={value: 123, language: }}                                                | {rdf-fusion.null=}                                  |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.null=}                                  |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-----------------------------------------------------+
        ");
    }
}
