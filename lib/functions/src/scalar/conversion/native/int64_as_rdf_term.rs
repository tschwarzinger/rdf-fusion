use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_encoding::typed_family::{NumericFamilyArray, TypedFamilyEncoding};
use rdf_fusion_encoding::{EncodingArray, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A function that transforms an arrow-native Int64 expression into an RDF term.
pub fn native_int64_as_term(encoding: Arc<TypedFamilyEncoding>) -> ScalarUDF {
    let udf_impl = NativeInt64AsTerm::new(encoding);
    ScalarUDF::new_from_impl(udf_impl)
}

#[derive(Debug, Eq)]
struct NativeInt64AsTerm {
    encoding: Arc<TypedFamilyEncoding>,
    name: String,
    signature: Signature,
}

impl NativeInt64AsTerm {
    pub fn new(encoding: Arc<TypedFamilyEncoding>) -> Self {
        Self {
            encoding,
            name: BuiltinName::NativeInt64AsTerm.to_string(),
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Int64]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NativeInt64AsTerm {
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
        Ok(self.encoding.data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("Unexpected number of arguments");
        }

        let arg = &args.args[0];
        if arg.data_type() != DataType::Int64 {
            return exec_err!("Unexpected argument type: {:?}", arg.data_type());
        }

        let arg = arg.to_array(args.number_rows)?;
        let family_array = NumericFamilyArray::try_from_primitive(arg)?;
        let result = self.encoding.create_array_from_family(family_array)?;
        Ok(ColumnarValue::Array(result.into_array_ref()))
    }
}

impl Hash for NativeInt64AsTerm {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.encoding.hash(state);
    }
}

impl PartialEq for NativeInt64AsTerm {
    fn eq(&self, other: &Self) -> bool {
        self.encoding == other.encoding
    }
}
