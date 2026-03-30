use crate::scalar::args::ScalarSparqlFunctionArgs;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::common::{ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingArray, RdfFusionEncodings, TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub fn with_sortable_term_encoding(encodings: RdfFusionEncodings) -> ScalarUDF {
    let udf_impl = WithSortableEncoding::new(encodings);
    ScalarUDF::new_from_impl(udf_impl)
}

/// Transforms RDF Terms into the [SortableTermEncoding](rdf_fusion_encoding::sortable_term::SortableTermEncoding).
#[derive(Debug, PartialEq, Eq)]
struct WithSortableEncoding {
    /// The name of this function
    name: String,
    /// The signature of this function
    signature: Signature,
    /// The registered encodings
    encodings: RdfFusionEncodings,
}

impl WithSortableEncoding {
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        Self {
            name: BuiltinName::WithSortableEncoding.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(
                    1,
                    vec![encodings.typed_family().data_type().clone()],
                ),
                Volatility::Immutable,
            ),
            encodings,
        }
    }
}

impl ScalarUDFImpl for WithSortableEncoding {
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
        Ok(SORTABLE_TERM_ENCODING.data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let was_scalar =
            !args.args.is_empty() && matches!(args.args[0], ColumnarValue::Scalar(_));
        let sparql_args =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        let result_array = match sparql_args.downcast_arrays() {
            Some(DowncastEncodingArrays::TypedFamily(arrays)) => arrays.map_children(
                |children| {
                    if children.len() != 1 {
                        return Err(ArrowError::InvalidArgumentError(
                            "Unexpected number of children".to_owned(),
                        ));
                    }
                    let child = &children[0];

                    child
                        .family()
                        .cast_to_sortable_array(Arc::clone(child.array()))
                        .map(|arr| arr.into_array_ref())
                },
                self.encodings.sortable_term().data_type(),
            )?,
            _ => {
                return exec_err!(
                    "Cannot convert to sortable encoding for arguments: {:?}",
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

impl Hash for WithSortableEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}
