#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! This crate implements the SPARQL scalar and aggregate functions for [RDF Fusion](https://docs.rs/rdf-fusion/).
//!
//! While all SPARQL functions are implemented as DataFusion user-defined functions (UDFs),
//! we also provide additional support to simplify SPARQL function implementation.
//!
//! # Aggregate Functions
//!
//! Aggregate functions currently have limited support.
//! They only support typed value encoding and do not yet provide a SPARQL-specific trait to simplify development.
//! We plan to provide enhanced support for aggregate functions in the future.
//!
//! # Dispatch
//!
//! Dispatch functions are a toolkit designed to help implement "iterative" versions of SPARQL functions
//! that operate on standard Rust types.
//! However, this functionality may be removed in the future for the following reasons:
//! 1. It often reduces performance compared to directly working on the arrays.
//! 2. In its current form, it is incompatible with some planned future improvements.

pub mod aggregates;
pub mod registry;
pub mod scalar;

#[cfg(test)]
mod test_utils {
    use datafusion::arrow::array::{
        ArrayRef, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, RecordBatch, StringArray, StructArray, UInt8Array,
    };
    use datafusion::logical_expr::{Expr, ScalarUDF, col};
    use datafusion::prelude::DataFrame;
    use rdf_fusion_common::Decimal;
    use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
    use rdf_fusion_encoding::string::STRING_ENCODING;
    use rdf_fusion_encoding::typed_family::{
        DateTimeFamily, DateTimeFamilyArray, FamilyArray, NullFamilyArray, NumericFamily,
        NumericFamilyArrayBuilder, ResourceArrayBuilder, StringFamilyArray,
        TypedFamilyArrayBuilder, TypedFamilyEncoding, TypedFamilyEncodingRef,
        TypedFamilyId,
    };
    use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings};
    use std::sync::Arc;

    /// Creates a default instance of [`RdfFusionEncodings`] used in tests.
    pub(crate) fn create_default_encodings() -> RdfFusionEncodings {
        RdfFusionEncodings::new(
            Arc::clone(&PLAIN_TERM_ENCODING),
            Arc::new(TypedFamilyEncoding::default()),
            None,
            Arc::clone(&STRING_ENCODING),
        )
    }

    /// Creates a test vector with mixed types.
    ///
    /// Includes null, named nodes, blank nodes, and a few literals. This is not an exhaustive
    /// literal list.
    pub(crate) fn create_standard_test_vector(
        encoding: &TypedFamilyEncodingRef,
    ) -> ArrayRef {
        // Null
        let null_array = NullFamilyArray::new(1);

        // Resources
        let resources_array =
            ResourceArrayBuilder::new(vec![0, 1, 1].into(), vec![0, 0, 1].into())
                .with_named_nodes(StringArray::from_iter_values([
                    "http://example.com/test",
                ]))
                .with_blank_nodes(StringArray::from_iter_values([
                    "my-blank-node",
                    "123456",
                ]))
                .finish()
                .unwrap();

        // Numeric
        let numeric_array = NumericFamilyArrayBuilder::new(
            vec![
                NumericFamily::INTEGER_TYPE_ID,
                NumericFamily::FLOAT_TYPE_ID,
                NumericFamily::FLOAT_TYPE_ID,
                NumericFamily::DOUBLE_TYPE_ID,
                NumericFamily::DECIMAL_TYPE_ID,
                NumericFamily::INT_TYPE_ID,
            ]
            .into(),
            vec![0, 0, 1, 0, 0, 0].into(),
        )
        .with_integers(Arc::new(Int64Array::from(vec![10])))
        .with_floats(Arc::new(Float32Array::from(vec![10.0, 0.0])))
        .with_doubles(Arc::new(Float64Array::from(vec![20.0])))
        .with_decimals(Arc::new(
            Decimal128Array::from(vec![30_000_000_000_000_000_000_i128])
                .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
                .unwrap(),
        ))
        .with_ints(Arc::new(Int32Array::from(vec![40])))
        .finish()
        .unwrap();

        // Strings
        let strings_array = StringFamilyArray::try_new(
            StringArray::from(vec!["b1", "just a string", "hello", "123"]),
            StringArray::from(vec![None, None, Some("en"), None]),
        )
        .unwrap();

        // Date-Time
        let date_time_array = Arc::new(
            StructArray::try_new(
                DateTimeFamily::fields().clone(),
                vec![
                    Arc::new(UInt8Array::from(vec![0])) as ArrayRef,
                    Arc::new(
                        Decimal128Array::from(vec![
                            63808171200_000_000_000_000_000_000_i128,
                        ])
                        .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
                        .unwrap(),
                    ) as ArrayRef,
                    Arc::new(Int16Array::from(vec![0])) as ArrayRef,
                ],
                None,
            )
            .unwrap(),
        ) as ArrayRef;

        // Final array
        let resource_type_id = encoding
            .find_typed_family_type_id(TypedFamilyId::Resource)
            .unwrap();
        let numeric_type_id = encoding
            .find_typed_family_type_id(TypedFamilyId::Numeric)
            .unwrap();
        let string_type_id = encoding
            .find_typed_family_type_id(TypedFamilyId::String)
            .unwrap();
        let date_time_type_id = encoding
            .find_typed_family_type_id(TypedFamilyId::DateTime)
            .unwrap();
        let type_ids = vec![
            TypedFamilyEncoding::NULL_TYPE_ID,
            resource_type_id,
            resource_type_id,
            resource_type_id,
            numeric_type_id,
            numeric_type_id,
            numeric_type_id,
            numeric_type_id,
            numeric_type_id,
            numeric_type_id,
            string_type_id,
            string_type_id,
            string_type_id,
            string_type_id,
            date_time_type_id,
        ];
        let offset = vec![0, 0, 1, 2, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 0];
        TypedFamilyArrayBuilder::new(Arc::clone(encoding), type_ids, offset)
            .unwrap()
            .with_nulls(null_array)
            .unwrap()
            .with_family_array(Some(resources_array))
            .expect("Error adding resource array")
            .with_family_array(Some(numeric_array))
            .expect("Error adding numeric array")
            .with_family_array(Some(strings_array))
            .expect("Error adding string array")
            .with_family_array(Some(DateTimeFamilyArray::from_array_unchecked(
                date_time_array,
            )))
            .expect("Error adding date-time array")
            .finish()
            .expect("Failed to create test vector")
            .into_array_ref()
    }

    /// Creates an instance of the given builtin UDF.
    pub(crate) fn evaluate_function_for_test(
        test_vector: ArrayRef,
        udf: Arc<ScalarUDF>,
    ) -> DataFrame {
        evaluate_function_with_args_for_test(test_vector, udf, vec![col("input")])
    }

    /// Creates an instance of the given builtin UDF with custom arguments.
    pub(crate) fn evaluate_function_with_args_for_test(
        test_vector: ArrayRef,
        udf: Arc<ScalarUDF>,
        args: Vec<Expr>,
    ) -> DataFrame {
        let input = DataFrame::from_columns(vec![("input", test_vector)]).unwrap();
        input.select([col("input"), udf.call(args)]).unwrap()
    }

    /// Creates an instance of the given builtin UDF for binary functions.
    pub(crate) fn evaluate_binary_function_for_test(
        left: ArrayRef,
        right: ArrayRef,
        udf: Arc<ScalarUDF>,
    ) -> DataFrame {
        let input =
            DataFrame::from_columns(vec![("left", left), ("right", right)]).unwrap();
        input
            .select([
                col("left"),
                col("right"),
                udf.call(vec![col("left"), col("right")]),
            ])
            .unwrap()
    }

    /// Creates an instance of the given builtin UDF.
    /// Evaluates an aggregate function for testing.
    pub(crate) fn evaluate_aggregate_for_test(
        input: ArrayRef,
        udf: Arc<datafusion::logical_expr::AggregateUDF>,
    ) -> DataFrame {
        evaluate_aggregate_with_args_for_test(input, udf, vec![col("a")])
    }

    /// Evaluates an aggregate function for testing with custom arguments.
    pub(crate) fn evaluate_aggregate_with_args_for_test(
        input: ArrayRef,
        udf: Arc<datafusion::logical_expr::AggregateUDF>,
        args: Vec<Expr>,
    ) -> DataFrame {
        let df = DataFrame::from_columns(vec![("a", input)]).unwrap();
        df.aggregate(vec![], vec![udf.call(args)]).unwrap()
    }

    /// Creates an instance of the given builtin UDF.
    pub(crate) fn evaluate_function(
        input: RecordBatch,
        udf: Arc<ScalarUDF>,
        args: Vec<Expr>,
    ) -> DataFrame {
        let schema = input.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .zip(input.columns().iter().cloned())
            .collect::<Vec<_>>();

        let input = DataFrame::from_columns(columns).unwrap();
        let mut select_expr = schema
            .fields()
            .iter()
            .map(|f| col(f.name()))
            .collect::<Vec<_>>();
        select_expr.push(udf.call(args));

        input.select(select_expr).unwrap()
    }

    /// Creates a typed family array with a string array with the given values and languages.
    pub(crate) fn create_typed_family_strings_array(
        encodings: &RdfFusionEncodings,
        values: Vec<&str>,
        languages: Vec<Option<&str>>,
    ) -> ArrayRef {
        let strings_array = StringFamilyArray::try_new(
            StringArray::from(values),
            StringArray::from(languages),
        )
        .unwrap();
        let left = encodings
            .typed_family()
            .create_array_from_family(strings_array)
            .unwrap()
            .into_array_ref();
        left
    }
}
