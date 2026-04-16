use crate::scalar::args::ScalarSparqlFunctionArgs;
use crate::scalar::error::SparqlUDFCreationError;
use crate::scalar::signature::SparqlOpTypeSignatureBuilder;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermType};
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use rdf_fusion_model::vocab::xsd;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// Creates a new RDF term from a plain literal and a datatype.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STRDT](https://www.w3.org/TR/sparql11-query/#func-strdt)
pub fn strdt_udf(
    encodings: RdfFusionEncodings,
) -> Result<ScalarUDF, SparqlUDFCreationError> {
    Ok(ScalarUDF::new_from_impl(StrDtSparqlOp::new(encodings)))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct StrDtSparqlOp {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for StrDtSparqlOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrDtSparqlOp")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl StrDtSparqlOp {
    /// Create a new [`StrDtSparqlOp`].
    fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_binary_arity()
            .build();
        Self {
            encodings,
            name: BuiltinName::StrDt.to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StrDtSparqlOp {
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
                // TODO: Maybe this should return PlainTerm?
                Ok(self.encodings.typed_family().data_type().clone())
            }
            _ => exec_err!("STRDT only supports the TypedFamily encoding"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let num_rows = args.number_rows;
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        let tf_encoding = self.encodings.typed_family();

        let result = match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(tf_args)) => {
                let lhs = tf_args.get(0);
                let rhs = tf_args.get(1);

                let lhs_pt = lhs.as_plain_term_array()?;
                let rhs_pt = rhs.as_plain_term_array()?;

                let res_pt = self.compute_plain_term(&lhs_pt, &rhs_pt, num_rows)?;
                tf_encoding
                    .cast_from_plain_term_array(&res_pt)?
                    .into_array_ref()
            }
            _ => exec_err!("STRDT is only supported for TypedFamily encoding")?,
        };

        Ok(ColumnarValue::Array(result))
    }
}

impl StrDtSparqlOp {
    fn compute_plain_term(
        &self,
        lhs: &PlainTermArray,
        rhs: &PlainTermArray,
        num_rows: usize,
    ) -> DFResult<PlainTermArray> {
        let lhs_parts = lhs.as_parts();
        let rhs_parts = rhs.as_parts();

        let mut result_values = Vec::with_capacity(num_rows);
        let mut result_datatypes = Vec::with_capacity(num_rows);
        let mut is_valid = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            let lhs_type = lhs_parts.term_type.value(i);
            let rhs_type = rhs_parts.term_type.value(i);

            if lhs_type == i8::from(PlainTermType::Literal)
                && rhs_type == i8::from(PlainTermType::NamedNode)
            {
                let lhs_lang = lhs_parts.language_tag.value(i);
                let lhs_dt = lhs_parts.data_type.value(i);
                if lhs_lang.is_empty()
                    && (lhs_dt.is_empty() || lhs_dt == xsd::STRING.as_str())
                {
                    result_values.push(lhs_parts.value.value(i).to_string());
                    result_datatypes.push(rhs_parts.value.value(i).to_string());
                    is_valid.push(true);
                    continue;
                }
            }
            result_values.push(String::new());
            result_datatypes.push(String::new());
            is_valid.push(false);
        }

        let validity = NullBuffer::from(is_valid);
        let result = PlainTermArray::try_new_literals(
            StringArray::from(result_values),
            StringArray::from(result_datatypes),
            StringArray::new_null(num_rows),
            Some(validity),
        )?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        create_default_encodings, evaluate_function_with_args_for_test,
    };
    use datafusion::logical_expr::{col, lit};
    use insta::assert_snapshot;
    use rdf_fusion_encoding::typed_family::{
        FamilyArray, ResourceFamily, StringFamily, StringFamilyArray,
        TypedFamilyArrayBuilder, TypedFamilyId,
    };
    use rdf_fusion_encoding::{EncodingArray, EncodingScalar};
    use rdf_fusion_model::vocab::xsd;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_strdt_typed_family() {
        let encodings = create_default_encodings();

        let strings_array =
            StringFamily::create_simple_strings_array(Arc::new(StringArray::from(vec![
                "123", "abc",
            ])));
        let string_type_id = encodings
            .typed_family()
            .find_typed_family_type_id(TypedFamilyId::String)
            .unwrap();
        let test_vector = TypedFamilyArrayBuilder::new(
            Arc::clone(&encodings.typed_family()),
            vec![string_type_id, string_type_id],
            vec![0, 1],
        )
        .unwrap()
        .with_family_array::<StringFamilyArray>(Some(
            StringFamilyArray::from_array_unchecked(strings_array.clone()),
        ))
        .unwrap()
        .finish()
        .unwrap()
        .into_array_ref();

        let dt_array = encodings
            .typed_family()
            .create_array_from_family(
                ResourceFamily::create_named_nodes_array(StringArray::from(vec![
                    xsd::INTEGER.as_str(),
                ]))
                .unwrap(),
            )
            .unwrap();
        let dt_scalar = dt_array.try_as_scalar(0).unwrap();

        let udf = Arc::new(strdt_udf(encodings).unwrap());
        let result = evaluate_function_with_args_for_test(
            test_vector,
            udf,
            vec![col("input"), lit(dt_scalar.into_scalar_value())],
        );
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r"
        +-----------------------------------------------+-------------------------------------------------------------------------+
        | input                                         | STRDT(?table?.input,Union 1:0:http://www.w3.org/2001/XMLSchema#integer) |
        +-----------------------------------------------+-------------------------------------------------------------------------+
        | {rdf-fusion.strings={value: 123, language: }} | {rdf-fusion.numeric={integer=123}}                                      |
        | {rdf-fusion.strings={value: abc, language: }} | {rdf-fusion.null=}                                                      |
        +-----------------------------------------------+-------------------------------------------------------------------------+
        "
        )
    }
}
