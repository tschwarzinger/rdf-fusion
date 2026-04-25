use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::datatypes::Field;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use rdf_fusion_encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArrayElementBuilder,
};
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionRegistry,
};
use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
use rdf_fusion_model::{Integer, Literal, TermRef};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
enum BinaryScenario {
    AllInt,
}

impl BinaryScenario {
    fn create_args(&self, encodings: &RdfFusionEncodings) -> Vec<ColumnarValue> {
        match self {
            BinaryScenario::AllInt => {
                let mut left_builder = PlainTermArrayElementBuilder::with_capacity(8192);
                let mut right_builder = PlainTermArrayElementBuilder::with_capacity(8192);
                for i in 0..8192 {
                    match i % 3 {
                        1 => {
                            let left_lit = Literal::from(Integer::from(i).as_i64());
                            let right_lit = Literal::from(Integer::from(i).as_i64());
                            left_builder.append_term(TermRef::from(&left_lit));
                            right_builder.append_term(TermRef::from(&right_lit));
                        }
                        2 => {
                            let left_lit = Literal::from(Integer::from(i).as_i64());
                            let right_lit = Literal::from(Integer::from(i + 1).as_i64());
                            left_builder.append_term(TermRef::from(&left_lit));
                            right_builder.append_term(TermRef::from(&right_lit));
                        }
                        _ => {
                            let left_lit = Literal::from(Integer::from(i + 1).as_i64());
                            let right_lit = Literal::from(Integer::from(i).as_i64());
                            left_builder.append_term(TermRef::from(&left_lit));
                            right_builder.append_term(TermRef::from(&right_lit));
                        }
                    }
                }
                let left_plain = left_builder.finish();
                let right_plain = right_builder.finish();

                let typed_family = encodings.typed_family();
                let left_tf = typed_family
                    .cast_from_plain_term_array(&left_plain)
                    .unwrap();
                let right_tf = typed_family
                    .cast_from_plain_term_array(&right_plain)
                    .unwrap();

                vec![
                    ColumnarValue::Array(left_tf.into_array_ref()),
                    ColumnarValue::Array(right_tf.into_array_ref()),
                ]
            }
        }
    }
}

//TODO: write run for BuiltinName::SameTerm; add other scenarios
fn bench_all_binary(c: &mut Criterion) {
    let encodings = RdfFusionEncodings::new(
        Arc::clone(&PLAIN_TERM_ENCODING),
        Arc::new(TypedFamilyEncoding::default()),
        None,
        Arc::clone(&SORTABLE_TERM_ENCODING),
        Arc::clone(&STRING_ENCODING),
    );
    let registry = DefaultRdfFusionFunctionRegistry::new(encodings.clone());

    let runs = HashMap::from([
        (BuiltinName::Equal, vec![BinaryScenario::AllInt]),
        (BuiltinName::GreaterOrEqual, vec![BinaryScenario::AllInt]),
        (BuiltinName::GreaterThan, vec![BinaryScenario::AllInt]),
        (BuiltinName::LessOrEqual, vec![BinaryScenario::AllInt]),
        (BuiltinName::LessThan, vec![BinaryScenario::AllInt]),
    ]);

    for (my_built_in, scenarios) in runs {
        let implementation = registry.udf(&FunctionName::Builtin(my_built_in)).unwrap();

        for scenario in scenarios {
            bench_binary_function(c, &encodings, &implementation, scenario);
        }
    }
}

/// Runs a single `scenario` against the `function` to bench.
fn bench_binary_function(
    c: &mut Criterion,
    encodings: &RdfFusionEncodings,
    function: &ScalarUDF,
    scenario: BinaryScenario,
) {
    let args = scenario.create_args(encodings);
    let options = Arc::new(ConfigOptions::default());

    let input_field_left = Arc::new(Field::new(
        "left",
        encodings.typed_family().data_type().clone(),
        true,
    ));
    let input_field_right = Arc::new(Field::new(
        "right",
        encodings.typed_family().data_type().clone(),
        true,
    ));
    let return_field = Arc::new(Field::new(
        "result",
        encodings.typed_family().data_type().clone(),
        true,
    ));

    let name = format!("{}_{scenario:?}", function.name());
    c.bench_function(&name, |b| {
        b.iter(|| {
            let args = ScalarFunctionArgs {
                args: args.clone(),
                arg_fields: vec![input_field_left.clone(), input_field_right.clone()],
                number_rows: 8192,
                return_field: return_field.clone(),
                config_options: options.clone(),
            };
            function.invoke_with_args(args).unwrap();
        });
    });
}

criterion_group!(scalar_binary, bench_all_binary);
criterion_main!(scalar_binary);
