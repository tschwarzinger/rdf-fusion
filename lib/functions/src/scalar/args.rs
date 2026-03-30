use datafusion::logical_expr::ScalarFunctionArgs;
use rdf_fusion_encoding::{
    DowncastEncodingArrays, EncodingDatum, EncodingName, RdfFusionEncodings,
    TermEncoding, detect_encoding_from_types,
};
use rdf_fusion_model::DFResult;
use std::sync::Arc;

/// The arguments of invoking a [ScalarSparqlOp](crate::scalar::ScalarSparqlOp).
pub struct ScalarSparqlOpArgs<TEncoding: TermEncoding> {
    /// A reference to the encoding of the arguments.
    pub encoding: Arc<TEncoding>,
    /// A reference to the encodings.
    pub encodings: RdfFusionEncodings,
    /// The number of rows.
    ///
    /// This is important for nullary operations and scalar arguments.
    pub number_rows: usize,
    /// The arguments in a given encoding.
    pub args: Vec<EncodingDatum<TEncoding>>,
}

/// The arguments of invoking a SPARQL scalar function.
///
/// This type provides a unified way to access the arguments of a SPARQL function, regardless of
/// the encoding used.
pub struct ScalarSparqlFunctionArgs<'a> {
    /// The inner arguments.
    args: &'a ScalarFunctionArgs,
    /// The actual encoding used in the arguments
    downcast_args: Option<DowncastEncodingArrays>,
}

impl<'a> ScalarSparqlFunctionArgs<'a> {
    /// Creates a new [ScalarSparqlFunctionArgs] from the given DataFusion arguments.
    pub fn try_from_args(
        args: &'a ScalarFunctionArgs,
        encodings: &'a RdfFusionEncodings,
    ) -> DFResult<Self> {
        if args.args.is_empty() {
            // For empty, we default to TypedFamily
            return Ok(Self {
                args,
                downcast_args: None,
            });
        }

        let arrays = args
            .args
            .iter()
            .map(|arg| arg.to_array(args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;
        let downcast_args = DowncastEncodingArrays::try_from_arrays(encodings, &arrays)?;

        Ok(Self {
            args,
            downcast_args,
        })
    }

    /// Returns a reference to the inner DataFusion arguments.
    pub fn inner(&self) -> &ScalarFunctionArgs {
        self.args
    }

    /// TODO
    pub fn downcast_arrays(&self) -> Option<&DowncastEncodingArrays> {
        self.downcast_args.as_ref()
    }
}

/// Detects the encoding of the given arguments.
///
/// This function verifies that all arguments have the same encoding and returns the name of
/// the encoding.
pub fn detect_encoding(
    encodings: &RdfFusionEncodings,
    args: &ScalarFunctionArgs,
) -> DFResult<Option<EncodingName>> {
    let types = args
        .arg_fields
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    detect_encoding_from_types(encodings, &types)
}
