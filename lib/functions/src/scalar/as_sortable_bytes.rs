use crate::scalar::args::ScalarSparqlFunctionArgs;
use datafusion::arrow::array::{ArrayRef, GenericBinaryBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{
    DowncastEncodingArgs, EncodingDatum, RdfFusionEncodings, TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A UDF that converts an RDF term in TypedFamily encoding into sortable bytes.
///
/// This is used internally for Z-Order curves and other sorting-related operations.
#[derive(Debug, PartialEq, Eq)]
pub struct AsSortableBytesUdf {
    name: String,
    signature: Signature,
    encodings: RdfFusionEncodings,
}

impl AsSortableBytesUdf {
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        Self {
            name: BuiltinName::AsSortableBytes.to_string(),
            signature: Signature::exact(
                vec![encodings.typed_family().data_type().clone()],
                Volatility::Immutable,
            ),
            encodings,
        }
    }
}

impl ScalarUDFImpl for AsSortableBytesUdf {
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
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let was_scalar =
            !args.args.is_empty() && matches!(args.args[0], ColumnarValue::Scalar(_));
        let sparql_args =
            ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;

        let result_array = match sparql_args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(args)) => {
                if args.args().len() != 1 {
                    return exec_err!("AsSortableBytes requires exactly 1 argument");
                }
                match &args.args()[0] {
                    EncodingDatum::Array(array) => {
                        Arc::new(array.as_sortable_bytes()?) as ArrayRef
                    }
                    EncodingDatum::Scalar(scalar) => {
                        let type_id = scalar.type_id();
                        let family = &self.encodings.typed_family().type_families()
                            [type_id as usize];
                        let inner_sv = scalar.inner_value();
                        let inner_array = inner_sv.to_array()?;

                        let sortable_bytes =
                            family.cast_to_sortable_array(inner_array)?;

                        let mut builder = GenericBinaryBuilder::<i32>::new();
                        let row_val = sortable_bytes.value(0);
                        let mut row = Vec::with_capacity(1 + row_val.len());
                        row.push(type_id as u8);
                        row.extend_from_slice(row_val);
                        builder.append_value(&row);

                        Arc::new(builder.finish()) as ArrayRef
                    }
                }
            }
            _ => {
                return exec_err!(
                    "AsSortableBytes only supports TypedFamily encoding, got {:?}",
                    args.args
                );
            }
        };

        if was_scalar {
            let scalar_value =
                datafusion::common::ScalarValue::try_from_array(&result_array, 0)?;
            Ok(ColumnarValue::Scalar(scalar_value))
        } else {
            Ok(ColumnarValue::Array(result_array))
        }
    }
}

impl Hash for AsSortableBytesUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}

pub fn as_sortable_bytes_udf(encodings: RdfFusionEncodings) -> ScalarUDF {
    ScalarUDF::new_from_impl(AsSortableBytesUdf::new(encodings))
}
