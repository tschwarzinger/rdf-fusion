use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::compute::is_not_null;
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{exec_err, internal_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{
    EncodingName, RdfFusionEncodings, detect_encoding_from_fields,
    with_rdf_term_extension_type,
};
use rdf_fusion_extensions::functions::BuiltinName;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Implementation of the SPARQL `COALESCE` function.
///
/// In contrast to DataFusion's `coalesce`, this function retains the RDF term extension type of
/// its arguments. DataFusion's `coalesce` is simplified to a `CASE` expression, which does not
/// propagate field metadata.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - COALESCE](https://www.w3.org/TR/sparql11-query/#func-coalesce)
pub fn coalesce_udf(encodings: RdfFusionEncodings) -> ScalarUDF {
    ScalarUDF::new_from_impl(SparqlCoalesce::new(encodings))
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct SparqlCoalesce {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl Debug for SparqlCoalesce {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparqlCoalesce").finish()
    }
}

impl SparqlCoalesce {
    fn new(encodings: RdfFusionEncodings) -> Self {
        let signature = Signature::variadic(
            encodings.get_data_types(&[
                EncodingName::PlainTerm,
                EncodingName::ObjectId,
                EncodingName::TypedFamily,
                EncodingName::String,
            ]),
            Volatility::Immutable,
        );
        Self {
            encodings,
            name: BuiltinName::Coalesce.to_string(),
            signature,
        }
    }
}

impl ScalarUDFImpl for SparqlCoalesce {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        let Some(encoding) =
            detect_encoding_from_fields(&self.encodings, args.arg_fields)?
        else {
            return plan_err!("COALESCE requires at least one argument");
        };

        // If any of the arguments is non-null, the result is non-null.
        let nullable = args.arg_fields.iter().all(|f| f.is_nullable());
        let field = args.arg_fields[0]
            .as_ref()
            .clone()
            .with_name(self.name())
            .with_nullable(nullable);
        Ok(Arc::new(with_rdf_term_extension_type(field, encoding)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let number_rows = args.number_rows;
        let mut args = args.args.into_iter();
        let Some(first) = args.next() else {
            return exec_err!("COALESCE requires at least one argument");
        };

        let mut result: ArrayRef = first.to_array(number_rows)?;
        for arg in args {
            if result.null_count() == 0 {
                break;
            }

            let other = arg.to_array(number_rows)?;
            let mask = is_not_null(result.as_ref())?;
            result = zip(&mask, &result, &other)?;
        }

        Ok(ColumnarValue::Array(result))
    }
}
