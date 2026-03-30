use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::function::{
    AccumulatorArgs, AggregateFunctionSimplification,
};
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use rdf_fusion_encoding::typed_family::{
    DowncastTypedFamilyArray, StringFamily, StringFamilyArray, TypedFamilyEncodingRef,
    TypedFamilyScalar,
};
use rdf_fusion_encoding::{EncodingScalar, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::sync::Arc;

pub fn group_concat_typed_family(encoding: TypedFamilyEncodingRef) -> AggregateUDF {
    AggregateUDF::new_from_impl(SparqlGroupConcat::new(encoding))
}

/// Concatenates the strings in a set with a given separator.
///
/// Relevant Resources:
/// - [SPARQL 1.1 - GROUP CONCAT](https://www.w3.org/TR/sparql11-query/#defn_aggGroupConcat)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparqlGroupConcat {
    encoding: TypedFamilyEncodingRef,
    name: String,
    signature: Signature,
}

impl SparqlGroupConcat {
    /// Creates a new [SparqlGroupConcat] aggregate UDF.
    pub fn new(encoding: TypedFamilyEncodingRef) -> Self {
        let name = BuiltinName::GroupConcat.to_string();
        let signature = Signature::uniform(
            2,
            vec![encoding.data_type().clone()],
            Volatility::Immutable,
        );
        SparqlGroupConcat {
            encoding,
            name,
            signature,
        }
    }
}

impl AggregateUDFImpl for SparqlGroupConcat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.encoding.data_type().clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        unreachable!("GROUP_CONCAT should have been simplified by the optimizer")
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        let encoding = Arc::clone(&self.encoding);
        Some(Box::new(move |function, _info| {
            debug_assert!(
                function.params.args.len() == 2,
                "Separator should be the second argument"
            );

            let separator_expr = &function.params.args[1];
            let separator = match separator_expr {
                Expr::Literal(value, _) => {
                    let scalar = encoding.try_new_scalar(value.clone())?;
                    let arr = scalar.to_array(1)?;
                    let children = arr.non_empty_children();
                    if children.len() != 1 {
                        return plan_err!("Separator should be a simple literal");
                    }
                    match children[0].downcast() {
                        DowncastTypedFamilyArray::String(s_arr) => {
                            if s_arr.language_array().is_null(0) {
                                s_arr.value_array().value(0).to_owned()
                            } else {
                                return plan_err!("Separator should be a simple literal");
                            }
                        }
                        _ => return plan_err!("Separator should be a simple literal"),
                    }
                }
                _ => return plan_err!("Separator should be a literal"),
            };

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                AggregateUDF::new_from_impl(SparqlGroupConcatWithSeparator::new(
                    Arc::clone(&encoding),
                    separator,
                ))
                .into(),
                vec![function.params.args[0].clone()],
                function.params.distinct,
                function.params.filter.clone(),
                function.params.order_by.clone(),
                function.params.null_treatment,
            )))
        }))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparqlGroupConcatWithSeparator {
    encoding: TypedFamilyEncodingRef,
    name: String,
    signature: Signature,
    separator: String,
}

impl SparqlGroupConcatWithSeparator {
    /// Creates a new [SparqlGroupConcatWithSeparator] aggregate UDF.
    pub fn new(encoding: TypedFamilyEncodingRef, separator: String) -> Self {
        let data_type = encoding.data_type().clone();
        SparqlGroupConcatWithSeparator {
            encoding,
            name: BuiltinName::GroupConcat.to_string(),
            signature: Signature::exact(vec![data_type], Volatility::Immutable),
            separator,
        }
    }
}

impl AggregateUDFImpl for SparqlGroupConcatWithSeparator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.encoding.data_type().clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SparqlGroupConcatAccumulator::new(
            Arc::clone(&self.encoding),
            self.separator.clone(),
        )))
    }
}

#[derive(Debug)]
struct SparqlGroupConcatAccumulator {
    encoding: TypedFamilyEncodingRef,
    separator: String,
    error: bool,
    value: Option<String>,
    language_error: bool,
    language: Option<String>,
}

impl SparqlGroupConcatAccumulator {
    pub fn new(encoding: TypedFamilyEncodingRef, separator: String) -> Self {
        SparqlGroupConcatAccumulator {
            encoding,
            separator,
            error: false,
            value: None,
            language_error: false,
            language: None,
        }
    }

    /// Updates the accumulator for a [`StringFamilyArray`].
    fn update_accumulator_for_string(
        &mut self,
        array: &StringFamilyArray,
    ) -> DFResult<()> {
        let val_arr = array.value_array();
        let lang_arr = array.language_array();

        for i in 0..val_arr.len() {
            if !self.language_error {
                let lang = if lang_arr.is_null(i) {
                    None
                } else {
                    Some(lang_arr.value(i))
                };

                if self.value.is_some() {
                    if self.language.as_deref() != lang {
                        self.language_error = true;
                        self.language = None;
                    }
                } else {
                    self.language = lang.map(ToOwned::to_owned);
                }
            }

            if let Some(mut current) = self.value.take() {
                current += self.separator.as_str();
                current += val_arr.value(i);
                self.value = Some(current);
            } else {
                self.value = Some(val_arr.value(i).to_owned());
            }
        }

        Ok(())
    }

    fn encode_result(&self) -> DFResult<TypedFamilyScalar> {
        if self.error {
            return Ok(self.encoding.create_scalar_null());
        }

        let value = self.value.as_deref().unwrap_or("");
        let language = if self.language_error {
            None
        } else {
            self.language.as_deref()
        };

        let val_arr = Arc::new(StringArray::from(vec![value])) as ArrayRef;
        let lang_arr = Arc::new(StringArray::from(vec![language])) as ArrayRef;
        let struct_arr = StringFamily::create_strings_array(val_arr, lang_arr);

        self.encoding.create_scalar_from_family::<StringFamily>(
            ScalarValue::try_from_array(&struct_arr, 0)?,
        )
    }
}

impl Accumulator for SparqlGroupConcatAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if self.error || values.is_empty() {
            return Ok(());
        }

        let arr = self.encoding.try_new_array(Arc::clone(&values[0]))?;
        for child in arr.non_empty_children() {
            match child.downcast() {
                DowncastTypedFamilyArray::Null(_) => continue,
                DowncastTypedFamilyArray::String(array) => {
                    self.update_accumulator_for_string(&array)?;
                }
                _ => {
                    self.error = true;
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.encode_result()?.into_scalar_value())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Boolean(Some(self.error)),
            ScalarValue::Utf8(self.value.clone()),
            ScalarValue::Boolean(Some(self.language_error)),
            ScalarValue::Utf8(self.language.clone()),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let error = states[0].as_boolean().iter().any(|e| e == Some(true));
        if error {
            self.error = true;
            return Ok(());
        }

        let language_error = states[2].as_boolean().iter().any(|e| e == Some(true));
        if language_error {
            self.language_error = true;
            self.language = None;
        }

        let values = states[1].as_string::<i32>();
        let languages = states[3].as_string::<i32>();
        let family_array = StringFamilyArray::try_new(values.clone(), languages.clone())?;
        self.update_accumulator_for_string(&family_array)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::evaluate_aggregate_with_args_for_test;
    use datafusion::arrow::array::{BooleanArray, StringArray};
    use datafusion::logical_expr::{col, lit};
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::typed_family::{
        StringFamilyArray, TypedFamilyArray, TypedFamilyEncoding,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_group_concat_typed_family() {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let values = StringArray::from(vec!["a", "b", "c"]);
        let typed_array = encoding
            .create_array_from_family(StringFamilyArray::new_simple(values))
            .unwrap();
        let separator_lit = create_separator_lit(&encoding, ";");

        assert_snapshot!(run_test(typed_array, separator_lit).await, @r"
        +-----------------------------------------------------+
        | GROUP_CONCAT(?table?.a,Union 2:{value:;,language:}) |
        +-----------------------------------------------------+
        | {rdf-fusion.strings={value: a;b;c, language: }}     |
        +-----------------------------------------------------+");
    }

    #[tokio::test]
    async fn test_group_concat_matching_languages() {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let typed_array = create_test_string_array(
            &encoding,
            &[Some("hello"), Some("world")],
            &[Some("en"), Some("en")],
        );
        let separator_lit = create_separator_lit(&encoding, " ");

        assert_snapshot!(run_test(typed_array, separator_lit).await, @r"
        +---------------------------------------------------------+
        | GROUP_CONCAT(?table?.a,Union 2:{value: ,language:})     |
        +---------------------------------------------------------+
        | {rdf-fusion.strings={value: hello world, language: en}} |
        +---------------------------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_group_concat_differing_languages() {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let typed_array = create_test_string_array(
            &encoding,
            &[Some("bonjour"), Some("world")],
            &[Some("fr"), Some("en")],
        );
        let separator_lit = create_separator_lit(&encoding, "-");

        assert_snapshot!(run_test(typed_array, separator_lit).await, @r"
        +---------------------------------------------------------+
        | GROUP_CONCAT(?table?.a,Union 2:{value:-,language:})     |
        +---------------------------------------------------------+
        | {rdf-fusion.strings={value: bonjour-world, language: }} |
        +---------------------------------------------------------+
        ");
    }

    #[test]
    fn test_accumulator_merge_batch_success() {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let mut accum =
            SparqlGroupConcatAccumulator::new(Arc::clone(&encoding), "|".to_string());

        // Simulate state arrays produced by multiple partitions
        let error_arr = Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef;
        let value_arr = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let lang_err_arr = Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef;
        let lang_arr =
            Arc::new(StringArray::from(vec![Some("en"), Some("en")])) as ArrayRef;

        accum
            .merge_batch(&[error_arr, value_arr, lang_err_arr, lang_arr])
            .unwrap();

        let state = accum.state().unwrap();
        assert_eq!(state[0], ScalarValue::Boolean(Some(false))); // error
        assert_eq!(state[1], ScalarValue::Utf8(Some("a|b".to_string()))); // value
        assert_eq!(state[2], ScalarValue::Boolean(Some(false))); // language_error
        assert_eq!(state[3], ScalarValue::Utf8(Some("en".to_string()))); // language
    }

    #[test]
    fn test_accumulator_merge_batch_error_propagation() {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let mut accum =
            SparqlGroupConcatAccumulator::new(Arc::clone(&encoding), "|".to_string());

        // Simulate an error occurring in one of the partitions
        let error_arr = Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef;
        let value_arr = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let lang_err_arr = Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef;
        let lang_arr =
            Arc::new(StringArray::from(vec![Some("en"), Some("en")])) as ArrayRef;

        accum
            .merge_batch(&[error_arr, value_arr, lang_err_arr, lang_arr])
            .unwrap();

        // The accumulator itself should now be in an error state
        assert!(accum.error);
    }

    #[test]
    fn test_accumulator_update_batch_empty() {
        let encoding = Arc::new(TypedFamilyEncoding::default());
        let mut accum =
            SparqlGroupConcatAccumulator::new(Arc::clone(&encoding), "|".to_string());

        // Empty update
        accum.update_batch(&[]).unwrap();
        assert_eq!(accum.value, None);

        // Empty array update
        let empty_arr = create_test_string_array(&encoding, &[], &[]);
        accum.update_batch(&[empty_arr.into_array_ref()]).unwrap();
        assert_eq!(accum.value, None);
    }

    /// Creates an test array with the given values and languages.
    fn create_test_string_array(
        encoding: &TypedFamilyEncodingRef,
        values: &[Option<&str>],
        languages: &[Option<&str>],
    ) -> TypedFamilyArray {
        let val_arr = StringArray::from(values.to_vec());
        let lang_arr = StringArray::from(languages.to_vec());
        encoding
            .create_array_from_family(
                StringFamilyArray::try_new(val_arr, lang_arr).unwrap(),
            )
            .unwrap()
    }

    /// Helper function to create a separator literal.
    fn create_separator_lit(encoding: &TypedFamilyEncodingRef, value: &str) -> Expr {
        let val_arr = Arc::new(StringArray::from(vec![Some(value)])) as ArrayRef;
        let lang_arr = Arc::new(StringArray::new_null(1)) as ArrayRef;
        let struct_arr = StringFamily::create_strings_array(val_arr, lang_arr);
        let separator_scalar = encoding
            .create_scalar_from_family::<StringFamily>(
                ScalarValue::try_from_array(&struct_arr, 0).unwrap(),
            )
            .unwrap();
        lit(separator_scalar.into_scalar_value())
    }

    /// Executes a test and returns the pretty-printed result.
    async fn run_test(typed_array: TypedFamilyArray, separator_lit: Expr) -> String {
        let encoding = Arc::clone(typed_array.encoding());
        let df = evaluate_aggregate_with_args_for_test(
            typed_array.into_array_ref(),
            Arc::new(group_concat_typed_family(encoding)),
            vec![col("a"), separator_lit],
        );
        df.to_string().await.unwrap()
    }
}
