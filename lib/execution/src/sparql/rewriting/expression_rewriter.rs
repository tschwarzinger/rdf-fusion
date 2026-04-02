use crate::sparql::rewriting::GraphPatternRewriter;
use datafusion::common::{internal_err, plan_err};
use datafusion::logical_expr::{Expr, Operator, lit, or};
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_logical::{RdfFusionExprBuilder, RdfFusionExprBuilderContext};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::Iri;
use rdf_fusion_model::sparql::algebra::{Expression, Function, GraphPattern};
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{DateTime, TermRef};
use rdf_fusion_model::{Literal, NamedNode};

pub(super) struct ExpressionRewriter<'rewriter> {
    graph_rewriter: &'rewriter GraphPatternRewriter,
    expr_builder_root: RdfFusionExprBuilderContext<'rewriter>,
    base_iri: Option<&'rewriter Iri<String>>,
}

impl<'rewriter> ExpressionRewriter<'rewriter> {
    /// Creates a new expression rewriter for a given schema.
    pub fn new(
        graph_rewriter: &'rewriter GraphPatternRewriter,
        expr_builder_root: RdfFusionExprBuilderContext<'rewriter>,
        base_iri: Option<&'rewriter Iri<String>>,
    ) -> Self {
        Self {
            graph_rewriter,
            expr_builder_root,
            base_iri,
        }
    }

    /// Rewrites an [Expression] to an [Expr] that computes an RDF term.
    pub fn rewrite(&self, expression: &Expression) -> DFResult<Expr> {
        self.rewrite_internal(expression)?.build()
    }

    /// Rewrites an [Expression] to an [Expr] that computes a native Boolean.
    pub fn rewrite_to_boolean(&self, expression: &Expression) -> DFResult<Expr> {
        self.rewrite_internal(expression)?
            .build_effective_boolean_value()
    }

    fn rewrite_internal(
        &self,
        expression: &Expression,
    ) -> DFResult<RdfFusionExprBuilder<'rewriter>> {
        match expression {
            Expression::Bound(var) => self
                .rewrite_internal(&Expression::Variable(var.clone()))?
                .bound(),
            Expression::Not(inner) => match inner.as_ref() {
                Expression::Exists(pattern) => self.rewrite_not_exists(pattern),
                _ => self.rewrite_internal(inner)?.not(),
            },
            Expression::Equal(lhs, rhs) => self
                .rewrite_internal(lhs)?
                .rdf_term_equal(self.rewrite(rhs)?),
            Expression::SameTerm(lhs, rhs) => {
                let boolean = self
                    .rewrite_internal(lhs)?
                    .build_same_term(self.rewrite(rhs)?)?;
                self.expr_builder_root.native_boolean_as_term(boolean)
            }
            Expression::Greater(lhs, rhs) => {
                self.rewrite_internal(lhs)?.greater_than(self.rewrite(rhs)?)
            }
            Expression::GreaterOrEqual(lhs, rhs) => self
                .rewrite_internal(lhs)?
                .greater_or_equal(self.rewrite(rhs)?),
            Expression::Less(lhs, rhs) => {
                self.rewrite_internal(lhs)?.less_than(self.rewrite(rhs)?)
            }
            Expression::LessOrEqual(lhs, rhs) => self
                .rewrite_internal(lhs)?
                .less_or_equal(self.rewrite(rhs)?),
            Expression::Literal(literal) => self
                .expr_builder_root
                .literal(TermRef::from(literal.as_ref())),
            Expression::Variable(var) => self.expr_builder_root.variable(var.as_ref()),
            Expression::FunctionCall(function, args) => {
                self.rewrite_function_call(function, args)
            }
            Expression::NamedNode(nn) => {
                self.expr_builder_root.literal(TermRef::from(nn.as_ref()))
            }
            Expression::Or(lhs, rhs) => self.logical_expression(Operator::Or, lhs, rhs),
            Expression::And(lhs, rhs) => self.logical_expression(Operator::And, lhs, rhs),
            Expression::In(lhs, rhs) => self.rewrite_in(lhs, rhs),
            Expression::Add(lhs, rhs) => {
                self.rewrite_internal(lhs)?.add(self.rewrite(rhs)?)
            }
            Expression::Subtract(lhs, rhs) => {
                self.rewrite_internal(lhs)?.sub(self.rewrite(rhs)?)
            }
            Expression::Multiply(lhs, rhs) => {
                self.rewrite_internal(lhs)?.mul(self.rewrite(rhs)?)
            }
            Expression::Divide(lhs, rhs) => {
                self.rewrite_internal(lhs)?.div(self.rewrite(rhs)?)
            }
            Expression::UnaryPlus(value) => self.rewrite_internal(value)?.unary_plus(),
            Expression::UnaryMinus(value) => self.rewrite_internal(value)?.unary_minus(),
            Expression::Exists(pattern) => self.rewrite_exists(pattern),
            Expression::If(test, if_true, if_false) => {
                self.rewrite_if(test, if_true, if_false)
            }
            Expression::Coalesce(args) => {
                let args = args
                    .iter()
                    .map(|arg| self.rewrite(arg))
                    .collect::<DFResult<Vec<_>>>()?;
                self.expr_builder_root.coalesce(args)
            }
        }
    }

    /// Rewrites a SPARQL function call.
    ///
    /// We assume here that the length of `args` matches the expected number of arguments.
    fn rewrite_function_call(
        &self,
        function: &Function,
        args: &[Expression],
    ) -> DFResult<RdfFusionExprBuilder<'rewriter>> {
        let args = args
            .iter()
            .map(|e| self.rewrite(e))
            .collect::<DFResult<Vec<_>>>()?;
        match function {
            // Functions on RDF Terms
            Function::IsIri => self.unary_args(args)?.is_iri(),
            Function::IsBlank => self.unary_args(args)?.is_blank(),
            Function::IsLiteral => self.unary_args(args)?.is_literal(),
            Function::IsNumeric => self.unary_args(args)?.is_numeric(),
            Function::Str => self.unary_args(args)?.str(),
            Function::Lang => self.unary_args(args)?.lang(),
            Function::Datatype => self.unary_args(args)?.datatype(),
            Function::Iri => self.unary_args(args)?.iri(self.base_iri),
            Function::BNode => match args.len() {
                0 => self.expr_builder_root.bnode(),
                1 => self.unary_args(args)?.bnode_from(),
                _ => internal_err!("Unexpected arity for BNode"),
            },
            Function::StrDt => {
                let (lhs, rhs) = self.binary_args(args)?;
                lhs.strdt(rhs)
            }
            Function::StrLang => {
                let (lhs, rhs) = self.binary_args(args)?;
                lhs.strlang(rhs)
            }
            Function::Uuid => self.expr_builder_root.uuid(),
            Function::StrUuid => self.expr_builder_root.str_uuid(),
            // Strings
            Function::StrLen => self.unary_args(args)?.strlen(),
            Function::SubStr => match args.len() {
                2 => {
                    let (lhs, rhs) = self.binary_args(args)?;
                    lhs.substr(rhs)
                }
                3 => {
                    let (arg0, arg1, arg2) = self.ternary_args(args)?;
                    arg0.substr_with_length(arg1, arg2)
                }
                _ => unreachable!("Unexpected number of args"),
            },
            Function::UCase => self.unary_args(args)?.ucase(),
            Function::LCase => self.unary_args(args)?.lcase(),
            Function::StrStarts => {
                let (lhs, rhs) = self.binary_args(args)?;
                lhs.str_starts(rhs)
            }
            Function::StrEnds => {
                let (lhs, rhs) = self.binary_args(args)?;
                lhs.str_ends(rhs)
            }
            Function::Contains => {
                let (lhs, rhs) = self.binary_args(args)?;
                lhs.contains(rhs)
            }
            Function::StrBefore => {
                let (lhs, rhs) = self.binary_args(args)?;
                lhs.str_before(rhs)
            }
            Function::StrAfter => {
                let (lhs, rhs) = self.binary_args(args)?;
                lhs.str_after(rhs)
            }
            Function::EncodeForUri => self.unary_args(args)?.encode_for_uri(),
            Function::Concat => self.expr_builder_root.concat(args),
            Function::LangMatches => {
                let (lhs, rhs) = self.binary_args(args)?;
                lhs.lang_matches(rhs)
            }
            Function::Regex => match args.len() {
                2 => {
                    let (lhs, rhs) = self.binary_args(args)?;
                    lhs.regex(rhs)
                }
                3 => {
                    let (arg0, arg1, arg2) = self.ternary_args(args)?;
                    arg0.regex_with_flags(arg1, arg2)
                }
                _ => unreachable!("Unexpected number of args"),
            },
            Function::Replace => match args.len() {
                3 => {
                    let (arg0, arg1, arg2) = self.ternary_args(args)?;
                    arg0.replace(arg1, arg2)
                }
                4 => {
                    let (arg0, arg1, arg2, arg3) = self.quarternary_args(args)?;
                    arg0.replace_with_flags(arg1, arg2, arg3)
                }
                _ => unreachable!("Unexpected number of args"),
            },
            // Numeric
            Function::Abs => self.unary_args(args)?.abs(),
            Function::Round => self.unary_args(args)?.round(),
            Function::Ceil => self.unary_args(args)?.ceil(),
            Function::Floor => self.unary_args(args)?.floor(),
            Function::Rand => self.expr_builder_root.rand(),
            // Dates & Durations
            Function::Year => self.unary_args(args)?.year(),
            Function::Month => self.unary_args(args)?.month(),
            Function::Day => self.unary_args(args)?.day(),
            Function::Hours => self.unary_args(args)?.hours(),
            Function::Minutes => self.unary_args(args)?.minutes(),
            Function::Seconds => self.unary_args(args)?.seconds(),
            Function::Timezone => self.unary_args(args)?.timezone(),
            Function::Tz => self.unary_args(args)?.tz(),
            Function::Now => {
                let literal = Literal::new_typed_literal(
                    DateTime::now().to_string(),
                    xsd::DATE_TIME,
                );
                self.expr_builder_root
                    .literal(TermRef::from(literal.as_ref()))
            }
            // Hashing
            Function::Md5 => self.unary_args(args)?.md5(),
            Function::Sha1 => self.unary_args(args)?.sha1(),
            Function::Sha256 => self.unary_args(args)?.sha256(),
            Function::Sha384 => self.unary_args(args)?.sha384(),
            Function::Sha512 => self.unary_args(args)?.sha512(),
            // Custom
            Function::Custom(nn) => self.rewrite_custom_function_call(nn, args),
        }
    }

    /// Rewrites a custom SPARQL function call
    #[allow(clippy::unused_self)]
    fn rewrite_custom_function_call(
        &self,
        function: &NamedNode,
        args: Vec<Expr>,
    ) -> DFResult<RdfFusionExprBuilder<'rewriter>> {
        if function == &xsd::BOOLEAN {
            return self.unary_args(args)?.cast_boolean();
        }

        if function == &xsd::INT {
            return self.unary_args(args)?.cast_int();
        }

        if function == &xsd::INTEGER {
            return self.unary_args(args)?.cast_integer();
        }

        if function == &xsd::FLOAT {
            return self.unary_args(args)?.cast_float();
        }

        if function == &xsd::DOUBLE {
            return self.unary_args(args)?.cast_double();
        }

        if function == &xsd::DECIMAL {
            return self.unary_args(args)?.cast_decimal();
        }

        if function == &xsd::DATE_TIME {
            return self.unary_args(args)?.cast_date_time();
        }

        if function == &xsd::STRING {
            return self.unary_args(args)?.cast_string();
        }

        self.expr_builder_root
            .try_create_builder_for_udf(&FunctionName::Custom(function.clone()), args)
    }

    /// Rewrites an IN expression to a list of equality checks. As the IN operation is equal to
    /// checking equality (using the "=" operator) this rewrite is sound.
    ///
    /// We cannot use the default DataFusion [Expr::InList] (without additional canonicalization) as
    /// the `=` is used.
    ///
    /// https://www.w3.org/TR/sparql11-query/#func-in
    fn rewrite_in(
        &self,
        lhs: &Expression,
        rhs: &[Expression],
    ) -> DFResult<RdfFusionExprBuilder<'rewriter>> {
        let lhs = self.rewrite_internal(lhs)?;
        let expressions = rhs
            .iter()
            .map(|e| {
                lhs.clone()
                    .rdf_term_equal(self.rewrite(e)?)?
                    .build_effective_boolean_value()
            })
            .collect::<DFResult<Vec<_>>>()?;

        let result = expressions
            .into_iter()
            .reduce(or)
            .unwrap_or_else(|| lit(false));
        self.expr_builder_root.native_boolean_as_term(result)
    }

    /// Rewrites an EXISTS expression
    fn rewrite_exists(
        &self,
        inner: &GraphPattern,
    ) -> DFResult<RdfFusionExprBuilder<'rewriter>> {
        let exists_plan = self.graph_rewriter.rewrite_with_existing_encoding(inner)?;
        self.expr_builder_root.exists(exists_plan)
    }

    /// Rewrites an NOT EXISTS expression
    fn rewrite_not_exists(
        &self,
        inner: &GraphPattern,
    ) -> DFResult<RdfFusionExprBuilder<'rewriter>> {
        let exists_plan = self.graph_rewriter.rewrite_with_existing_encoding(inner)?;
        self.expr_builder_root.not_exists(exists_plan)
    }

    /// Rewrites an IF expression to a case expression.
    fn rewrite_if(
        &self,
        test: &Expression,
        if_true: &Expression,
        if_false: &Expression,
    ) -> DFResult<RdfFusionExprBuilder<'rewriter>> {
        let test = self.rewrite_internal(test)?;
        let if_true = self.rewrite(if_true)?;
        let if_false = self.rewrite(if_false)?;
        test.sparql_if(if_true, if_false)
    }

    fn logical_expression(
        &self,
        operator: Operator,
        lhs: &Expression,
        rhs: &Expression,
    ) -> DFResult<RdfFusionExprBuilder<'rewriter>> {
        let lhs = self
            .rewrite_internal(lhs)?
            .build_effective_boolean_value()?;
        let rhs = self
            .rewrite_internal(rhs)?
            .build_effective_boolean_value()?;

        match operator {
            Operator::And => self.expr_builder_root.and(lhs, rhs),
            Operator::Or => self.expr_builder_root.sparql_or(lhs, rhs),
            _ => plan_err!("Unsupported logical expression: {}", &operator),
        }
    }

    /// Creates an expression builder from a DataFusion expression.
    ///
    /// This helper method wraps the expression in an RDF Fusion expression builder
    /// to enable RDF-specific operations on the expression.
    ///
    /// # Arguments
    /// * `expr` - The DataFusion expression to wrap
    fn expr_builder(&self, expr: Expr) -> DFResult<RdfFusionExprBuilder<'rewriter>> {
        self.expr_builder_root.try_create_builder(expr)
    }

    /// Processes arguments for a unary function.
    ///
    /// This helper method extracts a single argument from a vector of expressions
    /// and wraps it in an RDF Fusion expression builder.
    ///
    /// # Arguments
    /// * `args` - A vector containing exactly one expression
    ///
    /// # Returns
    /// An expression builder for the single argument
    fn unary_args(&self, args: Vec<Expr>) -> DFResult<RdfFusionExprBuilder<'rewriter>> {
        if let Ok([expr]) = TryInto::<[Expr; 1]>::try_into(args) {
            Ok(self.expr_builder(expr)?)
        } else {
            plan_err!("Unsupported argument list for unary function.")
        }
    }

    /// Processes arguments for a binary function.
    ///
    /// This helper method extracts two arguments from a vector of expressions,
    /// wraps the first in an RDF Fusion expression builder, and returns both.
    ///
    /// # Arguments
    /// * `args` - A vector containing exactly two expressions
    ///
    /// # Returns
    /// A tuple containing an expression builder for the first argument and the second argument
    fn binary_args(
        &self,
        args: Vec<Expr>,
    ) -> DFResult<(RdfFusionExprBuilder<'rewriter>, Expr)> {
        if let Ok([lhs, rhs]) = TryInto::<[Expr; 2]>::try_into(args) {
            let lhs = self.expr_builder(lhs)?;
            Ok((lhs, rhs))
        } else {
            plan_err!("Unsupported argument list for unary function.")
        }
    }

    /// Processes arguments for a ternary function.
    ///
    /// This helper method extracts three arguments from a vector of expressions,
    /// wraps the first in an RDF Fusion expression builder, and returns all three.
    ///
    /// # Arguments
    /// * `args` - A vector containing exactly three expressions
    ///
    /// # Returns
    /// A tuple containing an expression builder for the first argument and the second and third arguments
    fn ternary_args(
        &self,
        args: Vec<Expr>,
    ) -> DFResult<(RdfFusionExprBuilder<'rewriter>, Expr, Expr)> {
        if let Ok([arg0, arg1, arg2]) = TryInto::<[Expr; 3]>::try_into(args) {
            let arg0 = self.expr_builder(arg0)?;
            Ok((arg0, arg1, arg2))
        } else {
            plan_err!("Unsupported argument list for unary function.")
        }
    }

    /// Processes arguments for a quaternary function.
    ///
    /// This helper method extracts four arguments from a vector of expressions,
    /// wraps the first in an RDF Fusion expression builder, and returns all four.
    ///
    /// # Arguments
    /// * `args` - A vector containing exactly four expressions
    ///
    /// # Returns
    /// A tuple containing an expression builder for the first argument and the second, third, and fourth arguments
    fn quarternary_args(
        &self,
        args: Vec<Expr>,
    ) -> DFResult<(RdfFusionExprBuilder<'rewriter>, Expr, Expr, Expr)> {
        if let Ok([arg0, arg1, arg2, arg3]) = TryInto::<[Expr; 4]>::try_into(args) {
            let arg0 = self.expr_builder(arg0)?;
            Ok((arg0, arg1, arg2, arg3))
        } else {
            plan_err!("Unsupported argument list for unary function.")
        }
    }
}
