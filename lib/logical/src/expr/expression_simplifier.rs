use crate::expr::scalars::try_extract_scalar_term;
use crate::expr::unwrap_encoding_changes;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DFSchemaRef, plan_datafusion_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, lit};
use datafusion::optimizer::utils::NamePreserver;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use rdf_fusion_common::{DFResult, Term};
use rdf_fusion_encoding::plain_term::PlainTermScalar;
use rdf_fusion_encoding::{EncodingName, EncodingScalar, RdfFusionEncodings};
use rdf_fusion_extensions::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionRegistry,
};
use std::sync::Arc;

/// An optimizer rule that tries to optimize SPARQL expressions.
///
/// Currently, the following transformations are implemented:
/// - `IS_COMPATIBLE(A, B)` => `sameTerm(A, B)`, if A and B are not nullable
/// - `A = B` => `sameTerm(A, B)`, if possible
/// - `EFFECTIVE_BOOLEAN_VALUE(BOOLEAN_AS_TERM(X))` => `X`
#[derive(Debug)]
pub struct SimplifySparqlExpressionsRule {
    encodings: RdfFusionEncodings,
    function_registry: Arc<dyn RdfFusionFunctionRegistry>,
}

impl SimplifySparqlExpressionsRule {
    /// Creates a new [SimplifySparqlExpressionsRule].
    pub fn new(
        encodings: RdfFusionEncodings,
        function_registry: Arc<dyn RdfFusionFunctionRegistry>,
    ) -> Self {
        Self {
            encodings,
            function_registry,
        }
    }

    /// Rewrites the RDF Fusion built-ins in an [Expr].
    fn try_rewrite_expression(
        &self,
        expr: Expr,
        input_schema: &DFSchema,
    ) -> DFResult<Transformed<Expr>> {
        expr.transform_up(|expr| match expr {
            Expr::ScalarFunction(scalar_function) => {
                self.try_rewrite_scalar_function(scalar_function, input_schema)
            }
            _ => Ok(Transformed::no(expr)),
        })
    }

    /// Rewrites the RDF Fusion UDFs.
    fn try_rewrite_scalar_function(
        &self,
        scalar_function: ScalarFunction,
        input_schema: &DFSchema,
    ) -> DFResult<Transformed<Expr>> {
        let function_name = scalar_function.func.name();
        let builtin = BuiltinName::try_from(function_name);
        let Ok(builtin) = builtin else {
            return Ok(Transformed::no(Expr::ScalarFunction(scalar_function)));
        };

        match builtin {
            BuiltinName::IsCompatible => {
                try_replace_is_compatible_with_equality(scalar_function, input_schema)
            }
            BuiltinName::Equal => try_replace_equality_with_same_term(
                &self.encodings,
                self.function_registry.as_ref(),
                input_schema,
                scalar_function,
            ),
            BuiltinName::EffectiveBooleanValue => {
                try_replace_boolean_round_trip(scalar_function)
            }
            _ => Ok(Transformed::no(Expr::ScalarFunction(scalar_function))),
        }
    }
}

impl OptimizerRule for SimplifySparqlExpressionsRule {
    fn name(&self) -> &str {
        "simplify-sparql-expressions"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let schema = if !plan.inputs().is_empty() {
            DFSchemaRef::new(merge_schema(&plan.inputs()))
        } else if let LogicalPlan::TableScan(_) = &plan {
            // There is special handling in DF for this. We just bail out for now.
            return Ok(Transformed::no(plan));
        } else {
            Arc::new(DFSchema::empty())
        };

        // Changing the expression might lead to a name change in the schema.
        let name_preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = name_preserver.save(&expr);
            let expr = self.try_rewrite_expression(expr, &schema)?;
            Ok(Transformed::new_transformed(
                name.restore(expr.data),
                expr.transformed,
            ))
        })
    }
}

/// Replacing `IS_COMPATIBLE` with `=` is a crucial transformation for our queries as DataFusion's
/// built-in optimizers and join algorithms can handle the equality operator.
///
/// # DataFusion Native Approach
///
/// There is also a [ticket](https://github.com/apache/datafusion/issues/15891) that talks about
/// how DataFusion could natively support `IS_COMPATIBLE` semantics. However, this would be a
/// significant investment to actually support it in join algorithms etc.
fn try_replace_is_compatible_with_equality(
    scalar_function: ScalarFunction,
    input_schema: &DFSchema,
) -> DFResult<Transformed<Expr>> {
    let lhs_nullable = scalar_function.args[0].nullable(input_schema)?;
    let rhs_nullable = scalar_function.args[1].nullable(input_schema)?;

    if lhs_nullable || rhs_nullable {
        return Ok(Transformed::no(Expr::ScalarFunction(scalar_function)));
    }

    let [lhs, rhs] =
        TryInto::<[Expr; 2]>::try_into(scalar_function.args).map_err(|_| {
            plan_datafusion_err!("Unexpected number of args for IS_COMPATIBLE")
        })?;
    Ok(Transformed::yes(lhs.eq(rhs)))
}

/// In certain cases, an equality comparison can be changed to a `sameTerm` comparison.
/// This is only performed if we know that both sides can only be equal if they are the same
/// term.
///
/// For example, comparing to a known IRI literal allows replacing equality with `sameTerm`.
/// The rule is not applied if RDF lexical representation could differ.
///
/// Some examples:
/// - `?country = <Austria>` -> `sameTerm(?country, <Austria>)`
/// - `?value = "1"^^xsd:integer`, no optimization opportunity, as, for example, `"01"^^xsd:integer`
///   is also equal to the literal
fn try_replace_equality_with_same_term(
    encodings: &RdfFusionEncodings,
    registry: &dyn RdfFusionFunctionRegistry,
    schema: &DFSchema,
    scalar_function: ScalarFunction,
) -> DFResult<Transformed<Expr>> {
    let lhs_term = try_extract_scalar_term(encodings, &scalar_function.args[0]);
    let rhs_term = try_extract_scalar_term(encodings, &scalar_function.args[1]);

    let (term, other_expression) = match (lhs_term, rhs_term) {
        (Some(lhs_term), None)
            if lhs_term.is_named_node() || lhs_term.is_blank_node() =>
        {
            (lhs_term, &scalar_function.args[1])
        }
        (None, Some(rhs_term))
            if rhs_term.is_named_node() || rhs_term.is_blank_node() =>
        {
            (rhs_term, &scalar_function.args[0])
        }
        _ => return Ok(Transformed::no(Expr::ScalarFunction(scalar_function))),
    };

    let other_expression = unwrap_encoding_changes(other_expression);
    let field = other_expression.to_field(schema)?.1;
    let encoding = encodings
        .try_get_encoding_name(field.data_type())
        .ok_or_else(|| plan_datafusion_err!("Expected comparison with RDF terms"))?;

    if encoding == EncodingName::TypedFamily {
        return Ok(Transformed::no(Expr::ScalarFunction(scalar_function)));
    }

    replace_equality_with_same_term(encodings, registry, schema, term, other_expression)
}

/// Execute the replacement for [try_replace_equality_with_same_term] when all preconditions are
/// met. May swap the order of the arguments, but this is fine due to the commutativity of `=` and
/// `sameTerm`.
fn replace_equality_with_same_term(
    encodings: &RdfFusionEncodings,
    registry: &dyn RdfFusionFunctionRegistry,
    schema: &DFSchema,
    term: Term,
    other_expression: &Expr,
) -> DFResult<Transformed<Expr>> {
    let scalar = match encodings
        .try_get_encoding_name(other_expression.to_field(schema)?.1.data_type())
        .unwrap()
    {
        EncodingName::PlainTerm => encodings
            .plain_term()
            .encode_term(Ok(term.as_ref()))?
            .into_scalar_value(),
        EncodingName::ObjectId => {
            let Some(encoding) = encodings.object_id() else {
                return plan_err!("No Object ID mapping registerd.");
            };

            match encoding.encode_scalar(&PlainTermScalar::from(term.as_ref())) {
                Ok(scalar) => scalar.into_scalar_value(),
                Err(err) => plan_err!("Failed to encode term: {}", err)?,
            }
        }
        EncodingName::String => encodings
            .string_encoding()
            .encode_term(Ok(term.as_ref()))?
            .into_scalar_value(),
        EncodingName::TypedFamily => {
            unreachable!("Handled in caller")
        }
        EncodingName::Sortable => {
            unreachable!("Sortable encoding should not be encountered.")
        }
    };

    let boolean_as_term =
        registry.udf(&FunctionName::Builtin(BuiltinName::NativeBooleanAsTerm))?;
    Ok(Transformed::yes(
        boolean_as_term.call(vec![lit(scalar).eq(other_expression.clone())]),
    ))
}

/// Tries to replace EBV(BOOLEAN_AS_TERM(X)) with X. This can be a crucial optimization in query
/// plans that use such expressions in filters, as the conversion functions can hinder the optimizer
/// from pushing down parts of filters (e.g., A && B).
fn try_replace_boolean_round_trip(
    scalar_function: ScalarFunction,
) -> DFResult<Transformed<Expr>> {
    let (inner_built_in, args) = match &scalar_function.args[0] {
        Expr::ScalarFunction(inner_function) => {
            let built_in = BuiltinName::try_from(inner_function.func.name());
            let Ok(built_in) = built_in else {
                return Ok(Transformed::no(Expr::ScalarFunction(scalar_function)));
            };
            (built_in, &inner_function.args)
        }
        _ => return Ok(Transformed::no(Expr::ScalarFunction(scalar_function))),
    };

    match inner_built_in {
        BuiltinName::NativeBooleanAsTerm => {
            assert_eq!(
                args.len(),
                1,
                "Unexpected number of args for BOOLEAN_AS_TERM"
            );
            Ok(Transformed::yes(args[0].clone()))
        }
        _ => Ok(Transformed::no(Expr::ScalarFunction(scalar_function))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RdfFusionExprBuilderContext;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::{DFSchema, DFSchemaRef};
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan, LogicalPlanBuilder, col};
    use datafusion::optimizer::OptimizerContext;
    use insta::assert_snapshot;
    use rdf_fusion_common::{BlankNodeRef, Literal, NamedNodeRef, TermRef, VariableRef};
    use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
    use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
    use rdf_fusion_encoding::string::STRING_ENCODING;
    use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
    use rdf_fusion_encoding::{
        EncodingName, QuadStorageEncoding, RdfFusionEncodings, TermEncoding,
    };
    use rdf_fusion_extensions::RdfFusionContextView;
    use rdf_fusion_extensions::functions::FunctionName;
    use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;

    #[test]
    fn test_is_compatible_rewrite_when_not_nullable() {
        let schema = make_schema(EncodingName::PlainTerm, false, false);
        let rewritten = execute_test_for_builtin(&schema, BuiltinName::IsCompatible);
        assert_snapshot!(rewritten.data, @r"
        Projection: column1 = column2 AS IS_COMPATIBLE(column1,column2)
          EmptyRelation: rows=0
        ");
    }

    #[test]
    fn test_is_compatible_does_not_rewrite_when_nullable() {
        let schema = make_schema(EncodingName::PlainTerm, false, true);
        let rewritten = execute_test_for_builtin(&schema, BuiltinName::IsCompatible);
        assert_snapshot!(rewritten.data, @r"
        Projection: IS_COMPATIBLE(column1, column2)
          EmptyRelation: rows=0
        ");
    }

    #[test]
    fn test_equality_rewrite_to_same_term_iri() {
        let rewritten = run_literal_equality_test(TermRef::NamedNode(
            NamedNodeRef::new_unchecked("http://example.com/term"),
        ));
        assert_snapshot!(rewritten.data, @r"
        Projection: BOOLEAN_AS_TERM(Struct({term_type:0,value:http://example.com/term,data_type:,language_tag:}) = column1) AS EQ(ENC_TF(column1),ENC_TF(Struct({term_type:0,value:http://example.com/term,data_type:,language_tag:})))
          EmptyRelation: rows=0
        ");
    }

    #[test]
    fn test_equality_rewrite_to_same_term_blank_node() {
        let rewritten = run_literal_equality_test(TermRef::BlankNode(
            BlankNodeRef::new_unchecked("abc"),
        ));
        assert_snapshot!(rewritten.data, @r"
        Projection: BOOLEAN_AS_TERM(Struct({term_type:1,value:abc,data_type:,language_tag:}) = column1) AS EQ(ENC_TF(column1),ENC_TF(Struct({term_type:1,value:abc,data_type:,language_tag:})))
          EmptyRelation: rows=0
        ");
    }

    #[test]
    fn test_equality_rewrite_to_same_term_literal() {
        let rewritten =
            run_literal_equality_test(TermRef::Literal(Literal::from(1).as_ref()));
        assert_snapshot!(rewritten.data, @r"
        Projection: EQ(ENC_TF(column1), ENC_TF(Struct({term_type:2,value:1,data_type:http://www.w3.org/2001/XMLSchema#integer,language_tag:})))
          EmptyRelation: rows=0
        ");
    }

    #[test]
    fn test_equality_does_not_rewrite_when_not_applicable() {
        let schema = make_schema(EncodingName::TypedFamily, false, false);
        let rewritten = execute_test_for_builtin(&schema, BuiltinName::Equal);
        assert_snapshot!(rewritten.data, @r"
        Projection: EQ(column1, column2)
          EmptyRelation: rows=0
        ");
    }

    #[test]
    fn test_boolean_round_trip_rewrite() -> DFResult<()> {
        let context = create_context();
        let schema = make_schema(EncodingName::PlainTerm, false, true);
        let expr = RdfFusionExprBuilderContext::new(&context, &schema)
            .try_create_builder(col("column1"))?
            .not()?
            .build_effective_boolean_value()?;

        // Ensure the builder is not optimizing
        assert_eq!(
            expr.to_string(),
            "EBV(BOOLEAN_AS_TERM(NOT EBV(ENC_TF(column1))))"
        );

        let rewritten = execute_test_for_expr(&schema, expr);
        assert_snapshot!(rewritten.data, @r"
        Projection: NOT EBV(ENC_TF(column1)) AS EBV(BOOLEAN_AS_TERM(NOT EBV(ENC_TF(column1))))
          EmptyRelation: rows=0
        ");
        Ok(())
    }

    fn run_literal_equality_test(term: TermRef<'_>) -> Transformed<LogicalPlan> {
        let context = create_context();
        let schema = make_schema(EncodingName::PlainTerm, false, false);
        let builder_context = RdfFusionExprBuilderContext::new(&context, &schema);

        let literal = builder_context.literal(term).unwrap().build().unwrap();

        let expression = builder_context
            .variable(VariableRef::new_unchecked("column1"))
            .unwrap()
            .equal(literal)
            .unwrap()
            .build()
            .unwrap();

        let rewritten = execute_test_for_expr(&schema, expression);
        rewritten
    }

    fn execute_test_for_builtin(
        schema: &DFSchemaRef,
        builtin: BuiltinName,
    ) -> Transformed<LogicalPlan> {
        execute_test_for_builtin_with_args(
            schema,
            builtin,
            vec![col("column1"), col("column2")],
        )
    }

    fn execute_test_for_builtin_with_args(
        schema: &DFSchemaRef,
        builtin: BuiltinName,
        args: Vec<Expr>,
    ) -> Transformed<LogicalPlan> {
        let registry = create_context();
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: registry
                .functions()
                .udf(&FunctionName::Builtin(builtin))
                .unwrap(),
            args,
        });
        execute_test_for_expr(schema, expr)
    }

    fn execute_test_for_expr(
        schema: &DFSchemaRef,
        expr: Expr,
    ) -> Transformed<LogicalPlan> {
        let registry = create_context();
        let plan = create_plan(&schema)
            .project(vec![expr])
            .unwrap()
            .build()
            .unwrap();
        let rule = SimplifySparqlExpressionsRule::new(
            registry.encodings().clone(),
            registry.functions().clone(),
        );
        let rewritten = rule.rewrite(plan, &OptimizerContext::new()).unwrap();
        rewritten
    }

    fn create_context() -> RdfFusionContextView {
        let encodings = RdfFusionEncodings::new(
            Arc::clone(&PLAIN_TERM_ENCODING),
            Arc::new(TypedFamilyEncoding::default()),
            None,
            Arc::clone(&SORTABLE_TERM_ENCODING),
            Arc::clone(&STRING_ENCODING),
        );
        let registry = Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone()));
        RdfFusionContextView::new(registry, encodings, QuadStorageEncoding::PlainTerm)
    }

    fn make_schema(
        encoding: EncodingName,
        column1_nullable: bool,
        column2_nullable: bool,
    ) -> DFSchemaRef {
        let context = create_context();
        let data_type = match encoding {
            EncodingName::PlainTerm => context.encodings().plain_term().data_type(),
            EncodingName::TypedFamily => context.encodings().typed_family().data_type(),
            _ => panic!("Unsupported encoding"),
        };
        DFSchemaRef::new(
            DFSchema::try_from(Schema::new(vec![
                Field::new("column1", data_type.clone(), column1_nullable),
                Field::new("column2", data_type.clone(), column2_nullable),
            ]))
            .unwrap(),
        )
    }

    fn create_plan(schema: &DFSchemaRef) -> LogicalPlanBuilder {
        LogicalPlanBuilder::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::clone(schema),
        }))
    }
}
