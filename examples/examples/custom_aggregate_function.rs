use anyhow::Context;
use datafusion::arrow::array::{ArrayRef, Int32Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{downcast_value, exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use rdf_fusion::common::{NamedNode, RdfFormat};
use rdf_fusion::encoding::typed_family::{
    DowncastTypedFamilyArray, NumericFamily, NumericFamilyArray, TypedFamilyEncodingRef,
};
use rdf_fusion::encoding::{
    EncodingName, EncodingScalar, RdfFusionEncodings, TermEncoding,
    detect_encoding_from_types,
};
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::extensions::functions::FunctionName;
use rdf_fusion::storage::rdf_files::RdfFileScanOptions;
use rdf_fusion::store::Store;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// This example shows how to register a custom aggregate SPARQL function that can be used by RDF Fusion.
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // Load data from a file.
    let store = Store::new_in_memory();
    let file = tokio::fs::File::open("./examples/data/spiderman.ttl")
        .await
        .context("Could not find spiderman.ttl")?;
    store
        .load_from_reader(file, RdfFileScanOptions::with_format(RdfFormat::Turtle))
        .await?;

    // Register our custom aggregate function.
    let context = store.context();
    context
        .functions()
        .register_udaf(Arc::new(AggregateUDF::new_from_impl(CountSpiderUDAF::new(
            context.encodings().clone(),
        ))))?;

    // Run the SPARQL query.
    let query = "
    BASE <http://example.org/>
    PREFIX rel: <http://www.perceive.net/schemas/relationship/>

    SELECT (<countSpider>(?subject) AS ?spiderCount)
    WHERE {
        ?subject ?predicate ?object .
    }
    ";
    let result = store.query(query).await?;

    // Serialize the result
    let mut result_buffer = Vec::new();
    result
        .write(&mut result_buffer, QueryResultsFormat::Csv)
        .await?;
    let result = String::from_utf8(result_buffer)?;

    // Print the results.
    println!("Number of \"spider\":");
    print!("{result}");

    Ok(())
}

/// Counts how many times the given IRIs contain the substring "spider" (case-insensitive).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CountSpiderUDAF {
    encodings: RdfFusionEncodings,
    name: String,
    signature: Signature,
}

impl CountSpiderUDAF {
    /// Creates a new [CountSpiderUDAF].
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        let data_type = encodings.typed_family().data_type().clone();
        Self {
            encodings,
            // How to call the function in SPARQL.
            name: FunctionName::Custom(NamedNode::new_unchecked(
                "http://example.org/countSpider",
            ))
            .to_string(),
            // The signature of the function. For an explanation of Volatility, see the DataFusion
            // documentation. This basically means that the function always maps the same input to
            // the same output (as opposed to, for example, rand())
            signature: Signature::exact(vec![data_type.clone()], Volatility::Immutable),
        }
    }
}

impl Debug for CountSpiderUDAF {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountSpiderUDAF")
            .field("name", &self.name)
            .finish()
    }
}

impl AggregateUDFImpl for CountSpiderUDAF {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match detect_encoding_from_types(&self.encodings, arg_types)? {
            Some(EncodingName::TypedFamily) => {
                Ok(self.encodings.typed_family().data_type().clone())
            }
            _ => exec_err!("containsSpiderman only supports the TypedFamily encoding"),
        }
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SpiderAccumulator::new(Arc::clone(
            self.encodings.typed_family(),
        ))))
    }
}

/// A user-defined aggregate function (UDAF) has state across multiple rows, and thus we require a `struct` with that state.
#[derive(Debug)]
pub struct SpiderAccumulator {
    spiders: i32,
    encoding: TypedFamilyEncodingRef,
}

impl SpiderAccumulator {
    pub fn new(encoding: TypedFamilyEncodingRef) -> Self {
        Self {
            spiders: 0,
            encoding,
        }
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions to use them.
impl Accumulator for SpiderAccumulator {
    // This function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.spiders)])
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the number of IRIs containing "spider".
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let numeric_scalar = NumericFamilyArray::new_int_scalar(self.spiders);
        let typed_family_scalar =
            self.encoding.create_scalar_from_family::<NumericFamily>(
                numeric_scalar.to_scalar_value(),
            )?;
        Ok(typed_family_scalar.into_scalar_value())
    }

    // DataFusion calls this function to update the accumulator's state for a batch
    // of inputs rows. In this case we are looking for rows of IRIs containing "spider".
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = self.encoding.try_new_array(Arc::clone(&values[0]))?;
        for child in arr.non_empty_children() {
            match child.as_downcast_array() {
                DowncastTypedFamilyArray::Iri(array) => {
                    array.inner().iter().for_each(|item: Option<&str>| {
                        if item.is_some_and(|v| v.to_lowercase().contains("spider")) {
                            self.spiders += 1
                        }
                    });
                }
                _ => {
                    continue;
                }
            }
        }

        Ok(())
    }

    /// Merge the results from invocations of `state` into this accumulator's state.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        if states.len() != 1 {
            exec_datafusion_err!(
                "We expect an array of intermediate sums from other SpiderAccumulators."
            );
        }

        let arr = downcast_value!(states[0], Int32Array);
        if let Some(counts) = &compute::sum(arr) {
            self.spiders += *counts;
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self)
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
    async fn test_count_spider() -> anyhow::Result<()> {
        let store = Store::new_in_memory();

        let file = File::open("./data/spiderman.ttl")
            .await
            .context("Could not find spiderman.ttl")?;
        store
            .load_from_reader(file, RdfFileScanOptions::with_format(RdfFormat::Turtle))
            .await?;

        let context = store.context();
        context
            .functions()
            .register_udaf(Arc::new(AggregateUDF::new_from_impl(
                CountSpiderUDAF::new(context.encodings().clone()),
            )))?;

        let query = "
        BASE <http://example.org/>
        PREFIX rel: <http://www.perceive.net/schemas/relationship/>

        SELECT (<countSpider>(?subject) AS ?spiderCount )
        WHERE {
            ?subject ?predicate ?object .
        }
        ";
        let result = store.query(query).await?;

        let mut result_buffer = Vec::new();
        result
            .write(&mut result_buffer, QueryResultsFormat::Csv)
            .await?;
        let output = String::from_utf8(result_buffer)?;

        assert_snapshot!(output, @r#"
        spiderCount
        4
        "#);

        Ok(())
    }
}
