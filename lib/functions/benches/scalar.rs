use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::datatypes::Field;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use rdf_fusion_common::{BlankNode, Float, Integer, Literal, NamedNodeRef, TermRef};
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
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
enum UnaryScenario {
    AllNamedNodes,
    Mixed,
    AllBlank,
    AllInt,
    AllFloat,
    AllString,
}

impl UnaryScenario {
    fn create_args(&self, encodings: &RdfFusionEncodings) -> Vec<ColumnarValue> {
        let mut payload_builder = PlainTermArrayElementBuilder::with_capacity(8192);
        match self {
            UnaryScenario::AllNamedNodes => {
                for i in 0..8192 {
                    payload_builder.append_named_node(NamedNodeRef::new_unchecked(
                        format!("http://example.com/{i}").as_str(),
                    ));
                }
            }
            UnaryScenario::Mixed => {
                for i in 0..8192 {
                    match i % 4 {
                        0 => {
                            payload_builder.append_named_node(
                                NamedNodeRef::new_unchecked(
                                    format!("http://example.com/{i}").as_str(),
                                ),
                            );
                        }
                        1 => {
                            let lit = Literal::from(Integer::from(i).as_i64());
                            payload_builder.append_term(TermRef::from(&lit));
                        }
                        2 => {
                            let lit = Literal::from(f64::from(Float::from(i as i16)));
                            payload_builder.append_term(TermRef::from(&lit));
                        }
                        _ => {
                            payload_builder
                                .append_blank_node(BlankNode::default().as_ref());
                        }
                    }
                }
            }
            UnaryScenario::AllBlank => {
                for _ in 0..8192 {
                    payload_builder.append_blank_node(BlankNode::default().as_ref());
                }
            }
            UnaryScenario::AllInt => {
                for i in 0..8192 {
                    let lit = Literal::from(Integer::from(i).as_i64());
                    payload_builder.append_term(TermRef::from(&lit));
                }
            }
            UnaryScenario::AllFloat => {
                for i in 0..8192 {
                    let lit = Literal::from(f64::from(Float::from(i as i16)));
                    payload_builder.append_term(TermRef::from(&lit));
                }
            }
            UnaryScenario::AllString => {
                for i in 0..8192 {
                    let lit = Literal::new_simple_literal(format!("String number {i}"));
                    payload_builder.append_term(TermRef::from(&lit));
                }
            }
        }
        let plain_array = payload_builder.finish();
        let tf_array = encodings
            .typed_family()
            .cast_from_plain_term_array(&plain_array)
            .unwrap();
        vec![ColumnarValue::Array(tf_array.into_array_ref())]
    }
}

fn bench_all(c: &mut Criterion) {
    let encodings = RdfFusionEncodings::new(
        Arc::clone(&PLAIN_TERM_ENCODING),
        Arc::new(TypedFamilyEncoding::default()),
        None,
        Arc::clone(&STRING_ENCODING),
    );
    let registry = DefaultRdfFusionFunctionRegistry::new(encodings.clone());

    let runs = HashMap::from([
        (
            BuiltinName::IsIri,
            vec![UnaryScenario::AllNamedNodes, UnaryScenario::Mixed],
        ),
        (BuiltinName::IsLiteral, vec![UnaryScenario::Mixed]),
        (BuiltinName::IsNumeric, vec![UnaryScenario::Mixed]),
        (
            BuiltinName::IsBlank,
            vec![UnaryScenario::Mixed, UnaryScenario::AllBlank],
        ),
        (
            BuiltinName::Str,
            vec![
                UnaryScenario::Mixed,
                UnaryScenario::AllBlank,
                UnaryScenario::AllNamedNodes,
                UnaryScenario::AllString,
            ],
        ),
        (
            BuiltinName::CastFloat,
            vec![UnaryScenario::Mixed, UnaryScenario::AllInt],
        ),
        (BuiltinName::CastBoolean, vec![UnaryScenario::Mixed]),
        (
            BuiltinName::CastInteger,
            vec![UnaryScenario::Mixed, UnaryScenario::AllFloat],
        ),
        (BuiltinName::CastString, vec![UnaryScenario::Mixed]),
        (BuiltinName::CastDateTime, vec![UnaryScenario::Mixed]),
    ]);

    for (my_built_in, scenarios) in runs {
        let implementation = registry.udf(&FunctionName::Builtin(my_built_in)).unwrap();

        for scenario in scenarios {
            bench_unary_function(c, &encodings, &implementation, scenario);
        }
    }
}

/// Runs a single `scenario` against the `function` to bench.
fn bench_unary_function(
    c: &mut Criterion,
    encodings: &RdfFusionEncodings,
    function: &ScalarUDF,
    scenario: UnaryScenario,
) {
    let args = scenario.create_args(&encodings);
    let options = Arc::new(ConfigOptions::default());

    let input_field = Arc::new(Field::new(
        "input",
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
                arg_fields: vec![input_field.clone()],
                number_rows: 8192,
                return_field: return_field.clone(),
                config_options: options.clone(),
            };
            function.invoke_with_args(args).unwrap();
        });
    });
}

criterion_group!(scalar, bench_all);
criterion_main!(scalar);
