use crate::functions::name::FunctionName;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::EncodingName;
use std::fmt::Debug;
use std::sync::Arc;

/// A reference-counted pointer to an implementation of the `RdfFusionFunctionRegistry` trait.
///
/// This type alias is used throughout the codebase to pass around references to
/// function registries without tying code to specific implementations.
pub type RdfFusionFunctionRegistryRef = Arc<dyn RdfFusionFunctionRegistry>;

/// A registry for SPARQL functions that can create DataFusion UDFs and UDAFs.
///
/// This trait defines the interface for creating DataFusion user-defined functions
/// (UDFs) and user-defined aggregate functions (UDAFs) that implement SPARQL
/// function semantics.
///
/// # Additional Resources
/// - [SPARQL 1.1 Query Language - Functions](https://www.w3.org/TR/sparql11-query/#SparqlOps)
pub trait RdfFusionFunctionRegistry: Debug + Send + Sync {
    /// Returns the encodings supported by `function_name`.
    fn udf_supported_encodings(
        &self,
        function_name: &FunctionName,
    ) -> DFResult<Vec<EncodingName>>;

    /// Creates a [ScalarUDF].
    fn udf(&self, function_name: &FunctionName) -> DFResult<Arc<ScalarUDF>>;

    /// Creates a [AggregateUDF].
    fn udaf(&self, function_name: &FunctionName) -> DFResult<Arc<AggregateUDF>>;

    /// Register a [ScalarUDF].
    fn register_udf(&self, udf: ScalarUDF);

    /// Register a [AggregateUDF].
    fn register_udaf(&self, udaf: AggregateUDF);
}
