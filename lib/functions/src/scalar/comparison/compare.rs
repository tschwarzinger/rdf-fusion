use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};

/// Implementation of the SPARQL `=` operator.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - Operator Mapping](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
pub fn equal_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CompareSparqlUdf::new(
        encodings,
        BuiltinName::Equal.to_string(),
        CompareOperator::Equal,
    )))
}

/// Implementation of the SPARQL `>=` operator.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - Operator Mapping](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
pub fn greater_or_equal_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CompareSparqlUdf::new(
        encodings,
        BuiltinName::GreaterOrEqual.to_string(),
        CompareOperator::GreaterOrEqual,
    )))
}

/// Implementation of the SPARQL `>` operator.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - Operator Mapping](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
pub fn greater_than_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CompareSparqlUdf::new(
        encodings,
        BuiltinName::GreaterThan.to_string(),
        CompareOperator::GreaterThan,
    )))
}

/// Implementation of the SPARQL `<=` operator.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - Operator Mapping](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
pub fn less_or_equal_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CompareSparqlUdf::new(
        encodings,
        BuiltinName::LessOrEqual.to_string(),
        CompareOperator::LessOrEqual,
    )))
}

/// Implementation of the SPARQL `<` operator.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - Operator Mapping](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
pub fn less_than_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(CompareSparqlUdf::new(
        encodings,
        BuiltinName::LessThan.to_string(),
        CompareOperator::LessThan,
    )))
}

/// Defines the specific comparison operation to apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum CompareOperator {
    Equal,
    LessThan,
    LessOrEqual,
    GreaterThan,
    GreaterOrEqual,
}

impl CompareOperator {
    /// Evaluates whether the given `Ordering` satisfies this comparison operator.
    pub fn matches_ordering(self, ord: Ordering) -> bool {
        match self {
            Self::Equal => matches!(ord, Ordering::Equal),
            Self::LessThan => matches!(ord, Ordering::Less),
            Self::LessOrEqual => matches!(ord, Ordering::Less | Ordering::Equal),
            Self::GreaterThan => matches!(ord, Ordering::Greater),
            Self::GreaterOrEqual => {
                matches!(ord, Ordering::Greater | Ordering::Equal)
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CompareSparqlUdf {
    encodings: RdfFusionEncodings,
    name: String,
    op: CompareOperator,
    signature: Signature,
}

impl Debug for CompareSparqlUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompareSparqlUdf")
            .field("name", &self.name)
            .field("op", &self.op)
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl CompareSparqlUdf {
    /// Create a new [`CompareSparqlUdf`].
    pub fn new(encodings: RdfFusionEncodings, name: String, op: CompareOperator) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_binary_arity()
            .build();
        Self {
            encodings,
            name,
            op,
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CompareSparqlUdf {
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
            _ => exec_err!("Comparison is only supported for TypedFamily encoding"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let op = self.op;

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => tf_args
                .map_binary_comparison(move |ord| op.matches_ordering(ord))?
                .into_array_ref(),
            _ => exec_err!("Comparison is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::comparison::test_utils::create_comparison_test_vector;
    use crate::scalar::conversion::encoding::with_typed_family_encoding;
    use crate::test_utils::{
        create_default_encodings, evaluate_binary_function_for_test,
    };
    use datafusion::dataframe::DataFrame;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_common::LiteralRef;
    use rdf_fusion_common::vocab::xsd;
    use rdf_fusion_encoding::plain_term::PlainTermArrayElementBuilder;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_equal_typed_family() {
        let encodings = create_default_encodings();
        let (left, right) = create_comparison_test_vector(encodings.typed_family());
        let udf = Arc::new(equal_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+--------------------------------+
        | left                                                                                         | right                                                                                        | EQ(?table?.left,?table?.right) |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+--------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}             |
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}             |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.boolean=true}      |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=true}      |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=true}      |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}             |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.boolean=true}      |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.boolean=true}      |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}             |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.boolean=true}      |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+--------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_greater_than_typed_family() {
        let encodings = create_default_encodings();
        let (left, right) = create_comparison_test_vector(encodings.typed_family());
        let udf = Arc::new(greater_than_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+--------------------------------+
        | left                                                                                         | right                                                                                        | GT(?table?.left,?table?.right) |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+--------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}             |
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}             |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.boolean=true}      |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=true}      |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}             |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}             |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.boolean=false}     |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+--------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_greater_or_equal_typed_family() {
        let encodings = create_default_encodings();
        let (left, right) = create_comparison_test_vector(encodings.typed_family());
        let udf = Arc::new(greater_or_equal_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+---------------------------------+
        | left                                                                                         | right                                                                                        | GEQ(?table?.left,?table?.right) |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+---------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}              |
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}              |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}              |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.boolean=false}      |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}              |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.boolean=true}       |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+---------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_less_than_typed_family() {
        let encodings = create_default_encodings();
        let (left, right) = create_comparison_test_vector(encodings.typed_family());
        let udf = Arc::new(less_than_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+--------------------------------+
        | left                                                                                         | right                                                                                        | LT(?table?.left,?table?.right) |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+--------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}             |
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}             |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}             |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.boolean=true}      |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.boolean=false}     |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}             |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.boolean=false}     |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+--------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_less_or_equal_typed_family() {
        let encodings = create_default_encodings();
        let (left, right) = create_comparison_test_vector(encodings.typed_family());
        let udf = Arc::new(less_or_equal_udf(encodings).unwrap());
        let result = evaluate_binary_function_for_test(left, right, udf);
        assert_snapshot!(result.to_string().await.unwrap(), @"
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+---------------------------------+
        | left                                                                                         | right                                                                                        | LEQ(?table?.left,?table?.right) |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+---------------------------------+
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}                                                                           | {rdf-fusion.null=}              |
        | {rdf-fusion.null=}                                                                           | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.null=}              |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.resources={named_node=http://example.com/test}}                                  | {rdf-fusion.resources={blank_node=my-blank-node}}                                            | {rdf-fusion.boolean=false}      |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.numeric={float=10.0}}                                                            | {rdf-fusion.numeric={float=0.0}}                                                             | {rdf-fusion.boolean=false}      |
        | {rdf-fusion.numeric={integer=10}}                                                            | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}              |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.strings={value: just a string, language: }}                                      | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.boolean=true}       |
        | {rdf-fusion.strings={value: hello, language: en}}                                            | {rdf-fusion.strings={value: b1, language: }}                                                 | {rdf-fusion.null=}              |
        | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.date-time={date_time_type: 0, value: 63808171200.000000000000000000, offset: 0}} | {rdf-fusion.boolean=true}       |
        +----------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+---------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_equal_ill_formed_numeric() {
        let encodings = create_default_encodings();
        let mut plain_terms = PlainTermArrayElementBuilder::new();
        plain_terms.append_literal(LiteralRef::new_typed_literal("xyz", xsd::INTEGER));
        let plain_terms = plain_terms.finish();

        let with_encoding_udf = Arc::new(with_typed_family_encoding(encodings.clone()));
        let equal_udf = Arc::new(equal_udf(encodings).unwrap());

        let input =
            DataFrame::from_columns(vec![("input", plain_terms.into_array_ref())])
                .unwrap();
        let result = input
            .select([
                col("input"),
                equal_udf.call(vec![with_encoding_udf.call(vec![col("input")]); 2]),
            ])
            .unwrap();

        assert_snapshot!(result.to_string().await.unwrap(), @"
        +-------------------------------------------------------------------------------------------------+-------------------------------------------------+
        | input                                                                                           | EQ(ENC_TF(?table?.input),ENC_TF(?table?.input)) |
        +-------------------------------------------------------------------------------------------------+-------------------------------------------------+
        | {term_type: 2, value: xyz, data_type: http://www.w3.org/2001/XMLSchema#integer, language_tag: } | {rdf-fusion.null=}                              |
        +-------------------------------------------------------------------------------------------------+-------------------------------------------------+
        ");
    }
}
