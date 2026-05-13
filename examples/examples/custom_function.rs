use anyhow::Context;
use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion::api::functions::FunctionName;
use rdf_fusion::common::{DFResult, NamedNode, RdfFormat};
use rdf_fusion::encoding::typed_family::{BooleanFamilyArray, DowncastTypedFamilyArray};
use rdf_fusion::encoding::{
    DowncastEncodingArgs, EncodingArray, EncodingName, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::functions::scalar::args::ScalarSparqlFunctionArgs;
use rdf_fusion::functions::scalar::signature::SparqlOpTypeSignatureBuilder;
use rdf_fusion::storage::rdf_files::RdfParserOptions;
use rdf_fusion::store::Store;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// This example shows how to register a custom SPARQL function that can be used by RDF Fusion.
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // Load data from a file.
    let store = Store::new_in_memory().await;
    let file = tokio::fs::File::open("./examples/data/spiderman.ttl")
        .await
        .context("Could not find spiderman.ttl")?;
    store
        .load_from_reader(file, RdfParserOptions::with_format(RdfFormat::Turtle))
        .await?;

    // Register custom function.
    let context = store.context();
    context
        .functions()
        .register_udf(ScalarUDF::new_from_impl(ContainsSpiderUDF::new(
            context.encodings().clone(),
        )));

    // Run SPARQL query.
    let query = "
    BASE <http://example.org/>
    PREFIX rel: <http://www.perceive.net/schemas/relationship/>

    SELECT ?subject ?predicate ?object
    WHERE {
        ?subject ?predicate ?object .
        FILTER(<http://example.org/containsSpider>(?object))
    }
    ";
    let result = store.query(query).await?;

    // Serialize result
    let mut result_buffer = Vec::new();
    result
        .write(&mut result_buffer, QueryResultsFormat::Csv)
        .await?;
    let result = String::from_utf8(result_buffer)?;

    // Print results.
    println!("Enemies of Spiderman:");
    print!("{result}");

    Ok(())
}

/// Checks whether a given *literal* contains the string `spider` (case-insensitive).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ContainsSpiderUDF {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl ContainsSpiderUDF {
    /// Creates a new [ContainsSpiderUDF].
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlOpTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_unary_arity()
            .build();
        Self {
            encodings,
            // How to call the function in SPARQL.
            name: FunctionName::Custom(NamedNode::new_unchecked(
                "http://example.org/containsSpider",
            ))
            .to_string(),
            // The signature of the function. For an explanation of Volatility, see the DataFusion
            // documentation. This basically means that the function always maps the same input to
            // the same output (as opposed to, for example, rand())
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl Debug for ContainsSpiderUDF {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContainsSpiderUDF")
            .field("name", &self.name)
            .finish()
    }
}

impl ScalarUDFImpl for ContainsSpiderUDF {
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
            _ => exec_err!("containsSpiderman only supports the TypedFamily encoding"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ScalarSparqlFunctionArgs::try_from_args(&args, &self.encodings)?;
        match args.downcast_arrays() {
            Some(DowncastEncodingArgs::TypedFamily(array)) => {
                let result = array.map_children_tf_unary(|child| {
                    match child.as_downcast_array() {
                        // If the input is null or a resource, return null. The output is a
                        // TypedFamilyEncoding array with nulls.
                        DowncastTypedFamilyArray::Null(_)
                        | DowncastTypedFamilyArray::Resource(_) => self
                            .encodings
                            .typed_family()
                            .create_null_array(child.to_array().len()),
                        // If the input is a string, check whether it contains the string. The
                        // output is a TypedFamilyEncoding array with booleans.
                        DowncastTypedFamilyArray::String(array) => {
                            let result: BooleanArray = array
                                .value_array()
                                .iter()
                                .map(|val: Option<&str>| {
                                    val.map(|v| v.to_lowercase().contains("spider"))
                                })
                                .collect();
                            self.encodings
                                .typed_family()
                                .create_array_from_family(BooleanFamilyArray::new(result))
                        }
                        // For all other families, return false.
                        _ => {
                            let result =
                                BooleanArray::from(vec![false; child.to_array().len()]);
                            self.encodings
                                .typed_family()
                                .create_array_from_family(BooleanFamilyArray::new(result))
                        }
                    }
                })?;

                Ok(ColumnarValue::Array(result.into_array_ref()))
            }
            _ => exec_err!("containsSpiderman was called with an unsupported encoding."),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_snapshot;
    use rdf_fusion::common::RdfFormat;
    use rdf_fusion::execution::results::QueryResultsFormat;
    use rdf_fusion::store::Store;
    use tokio::fs::File;

    #[tokio::test]
    async fn test_contains_spiderman() -> anyhow::Result<()> {
        let store = Store::new_in_memory().await;
        let context = store.context();

        let udf = ContainsSpiderUDF::new(context.encodings().clone());
        context
            .functions()
            .register_udf(ScalarUDF::new_from_impl(udf));

        let file_path = "./data/spiderman.ttl";
        let file = File::open(file_path)
            .await
            .with_context(|| format!("Failed to open test data at {}", file_path))?;

        store
            .load_from_reader(file, RdfParserOptions::with_format(RdfFormat::Turtle))
            .await?;

        let query = "
            BASE <http://example.org/>
            PREFIX rel: <http://www.perceive.net/schemas/relationship/>

            SELECT ?subject ?predicate ?object
            WHERE {
                ?subject ?predicate ?object .
                FILTER(<http://example.org/containsSpider>(?object))
            }
        ";

        let result_set = store.query(query).await?;
        let mut buffer = Vec::new();
        result_set
            .write(&mut buffer, QueryResultsFormat::Tsv)
            .await?;
        let output = String::from_utf8(buffer)?;

        assert_snapshot!(output, @r#"
        ?subject	?predicate	?object
        <http://example.org/#spiderman>	<http://xmlns.com/foaf/0.1/name>	"Spiderman"
        "#);

        Ok(())
    }
}
