use crate::RdfFusionExprBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{
    Column, DFSchema, Spans, exec_datafusion_err, plan_datafusion_err, plan_err,
};
use datafusion::functions_aggregate::count::count;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;
use datafusion::logical_expr::{
    Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, ScalarUDF, Subquery, and,
    exists, lit, not_exists,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::{TermRef, ThinError, VariableRef};
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::{
    EncodingName, EncodingScalar, RdfFusionEncodings, TermEncoder,
};
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionRegistry,
};
use std::collections::HashSet;
use std::sync::Arc;

/// An expression builder for creating SPARQL expressions.
///
/// This is the builder context, which can be used to create expression builders. Each builder
/// context has an associated schema. This schema is used for, for example, inferring the type of
/// built expressions and is therefore crucial.
///
/// Furthermore, the context holds a reference to an [RdfFusionFunctionRegistry] that is used to
/// resolve the registered built-ins and user-defined functions.
#[derive(Debug, Clone, Copy)]
pub struct RdfFusionExprBuilderContext<'context> {
    /// Provides access to the RDF Fusion configuration.
    rdf_fusion_context: &'context RdfFusionContextView,
    /// The schema of the input data. Necessary for inferring the encodings of RDF terms.
    schema: &'context DFSchema,
}

impl<'context> RdfFusionExprBuilderContext<'context> {
    /// Creates a new expression builder context.
    pub fn new(
        rdf_fusion_context: &'context RdfFusionContextView,
        schema: &'context DFSchema,
    ) -> Self {
        Self {
            rdf_fusion_context,
            schema,
        }
    }

    /// Returns the schema of the input data.
    pub fn schema(&self) -> &DFSchema {
        self.schema
    }

    /// Returns a reference to the RDF Fusion context.
    pub fn rdf_fusion_context(&self) -> &RdfFusionContextView {
        self.rdf_fusion_context
    }

    /// Returns a reference to the used function registry.
    pub fn registry(&self) -> &dyn RdfFusionFunctionRegistry {
        self.rdf_fusion_context.functions().as_ref()
    }

    /// Returns a reference to the used object id encoding.
    pub fn encodings(&self) -> &'context RdfFusionEncodings {
        self.rdf_fusion_context.encodings()
    }

    /// Creates a new [RdfFusionExprBuilder] from an existing [Expr].
    pub fn try_create_builder(
        &self,
        expr: Expr,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        RdfFusionExprBuilder::try_new_from_context(*self, expr)
    }

    /// Creates a new [RdfFusionExprBuilder] from an existing [Expr].
    pub fn try_create_builder_for_udf(
        &self,
        name: &FunctionName,
        args: Vec<Expr>,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udf = self.rdf_fusion_context.functions().udf(name)?;

        if args.is_empty() {
            return self.try_create_builder(udf.call(vec![]));
        }

        // The function might only accept constant arguments which don't have to be an RDF term.
        let first_arg_encoding = self.get_encodings(&args[..1]);
        if first_arg_encoding.is_err() {
            return self.try_create_builder(udf.call(args));
        }

        let expr = self.apply_with_args_no_builder(name, args)?;
        self.try_create_builder(expr)
    }

    /// Creates a new expression that evaluates to the first argument that does not produce an
    /// error.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Coalesce](https://www.w3.org/TR/sparql11-query/#func-coalesce)
    pub fn coalesce(&self, args: Vec<Expr>) -> DFResult<RdfFusionExprBuilder<'context>> {
        self.apply_builtin(BuiltinName::Coalesce, args)
    }

    /// Creates an expression that generates a new blank node.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - BNODE](https://www.w3.org/TR/sparql11-query/#func-bnode)
    pub fn bnode(&self) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udf = self.create_builtin_udf(BuiltinName::BNode)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// Creates an expression that computes a fresh IRI from the `urn:uuid:` scheme.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - UUID](https://www.w3.org/TR/sparql11-query/#func-uuid)
    pub fn uuid(&self) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udf = self.create_builtin_udf(BuiltinName::Uuid)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// Creates an expression that computes a string literal representation of a UUID.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - STRUUID](https://www.w3.org/TR/sparql11-query/#func-struuid)
    pub fn str_uuid(&self) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udf = self.create_builtin_udf(BuiltinName::StrUuid)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// Creates an expression that concatenates the string representations of its arguments.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - CONCAT](https://www.w3.org/TR/sparql11-query/#func-concat)
    pub fn concat(&self, args: Vec<Expr>) -> DFResult<RdfFusionExprBuilder<'context>> {
        self.apply_builtin(BuiltinName::Concat, args)
    }

    /// Creates an expression that interleaves the bits of its inputs. This can be used for
    /// clustering similar values over multiple inputs.
    ///
    /// # Relevant Resources
    /// - <https://en.wikipedia.org/wiki/Z-order_curve>
    pub fn zorder(&self, args: Vec<Expr>) -> DFResult<Expr> {
        self.apply_with_args_no_builder(&FunctionName::Builtin(BuiltinName::ZOrder), args)
    }

    /// Creates an expression that computes a pseudo-random value between 0 and 1.
    ///
    /// The data type of the value is `xsd:double`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Rand](https://www.w3.org/TR/sparql11-query/#idp2130040)
    pub fn rand(&self) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udf = self.create_builtin_udf(BuiltinName::Rand)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// Creates an expression from a SPARQL variable.
    ///
    /// If the variable is not bound in the current context (i.e., not in the schema),
    /// the expression evaluates to a null literal.
    pub fn variable(
        &self,
        variable: VariableRef<'_>,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let column = Column::new_unqualified(variable.as_str());
        let expr = if self.schema.has_column(&column) {
            Expr::from(column)
        } else {
            let null = DefaultPlainTermEncoder.encode_term(ThinError::expected())?;
            lit(null.into_scalar_value())
        };

        self.try_create_builder(expr)
    }

    /// Creates a literal expression from an RDF term.
    ///
    /// The term is encoded using the [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding).
    pub fn literal<'lit>(
        &self,
        term: impl Into<TermRef<'lit>>,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let scalar = DefaultPlainTermEncoder.encode_term(Ok(term.into()))?;
        self.try_create_builder(lit(scalar.into_scalar_value()))
    }

    /// Creates a null literal expression.
    ///
    /// This is used to represent unbound variables or errors.
    pub fn null_literal(&'context self) -> DFResult<RdfFusionExprBuilder<'context>> {
        let scalar = DefaultPlainTermEncoder.encode_term(ThinError::expected())?;
        self.try_create_builder(lit(scalar.into_scalar_value()))
    }

    /// Creates a new exists filter operator. This operator takes another graph pattern (as a
    /// [LogicalPlan]) that is checked against the current graph pattern. Only solutions that have
    /// at least one compatible solution in the `EXISTS` graph pattern pass this filter.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - NOT EXISTS and EXISTS](https://www.w3.org/TR/sparql11-query/#func-filter-exists)
    pub fn exists(
        self,
        exists_plan: LogicalPlan,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        self.exists_impl(exists_plan, false)
    }

    /// Creates a new not exists filter operator. This operator takes another graph pattern (as a
    /// [LogicalPlan]) that is checked against the current graph pattern. Only solutions that have
    /// no compatible solution in the `EXISTS` graph pattern pass this filter.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - NOT EXISTS and EXISTS](https://www.w3.org/TR/sparql11-query/#func-filter-exists)
    pub fn not_exists(
        self,
        exists_plan: LogicalPlan,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        self.exists_impl(exists_plan, true)
    }

    fn exists_impl(
        self,
        exists_plan: LogicalPlan,
        is_not: bool,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let exists_pattern = LogicalPlanBuilder::new(exists_plan);
        let outer_schema = self.schema();

        let outer_keys: HashSet<_> = outer_schema
            .columns()
            .into_iter()
            .map(|c| c.name().to_owned())
            .collect();
        let exists_keys: HashSet<_> = exists_pattern
            .schema()
            .columns()
            .into_iter()
            .map(|c| c.name().to_owned())
            .collect();

        // This check is necessary to avoid a crash during query planning. However, we don't know
        // whether this is our fault or it is a bug in DataFusion.
        // TODO: Investigate why this causes issues and file an issue if necessary
        if outer_keys.is_disjoint(&exists_keys) {
            let group_expr: [Expr; 0] = [];
            let count = exists_pattern.aggregate(
                group_expr,
                [count(Expr::Literal(COUNT_STAR_EXPANSION, None))],
            )?;
            let subquery = Subquery {
                subquery: count.build()?.into(),
                outer_ref_columns: vec![],
                spans: Spans(vec![]),
            };

            return self
                .native_boolean_as_term(Expr::ScalarSubquery(subquery).gt(lit(0)));
        }

        // TODO: Investigate why we need this renaming and cannot refer to the unqualified column
        let projections = exists_pattern
            .schema()
            .columns()
            .into_iter()
            .map(|c| Expr::from(c.clone()).alias(format!("__inner__{}", c.name())))
            .collect::<Vec<_>>();
        let exists_pattern = exists_pattern.project(projections)?;
        let exists_expr_builder_root = RdfFusionExprBuilderContext::new(
            self.rdf_fusion_context(),
            exists_pattern.schema(),
        );

        let mut join_columns = outer_keys.intersection(&exists_keys).collect::<Vec<_>>();
        join_columns.sort(); // Stable output

        let compatible_filters = join_columns
            .into_iter()
            .map(|k| Self::build_exists_filter(exists_expr_builder_root, outer_schema, k))
            .collect::<DFResult<Vec<_>>>()?;
        let compatible_filter = compatible_filters
            .into_iter()
            .reduce(and)
            .unwrap_or_else(|| lit(true));

        let subquery = Arc::new(exists_pattern.filter(compatible_filter)?.build()?);
        if is_not {
            self.native_boolean_as_term(not_exists(subquery))
        } else {
            self.native_boolean_as_term(exists(subquery))
        }
    }

    fn build_exists_filter(
        expr_builder_ctx: RdfFusionExprBuilderContext,
        outer_schema: &DFSchema,
        k: &String,
    ) -> DFResult<Expr> {
        let outer_field = outer_schema.field_with_name(None, k).map_err(|_| {
            plan_datafusion_err!("Could not find column {} in schema.", k)
        })?;
        let inner_column = Column::new_unqualified(format!("__inner__{k}"));

        // If both fields are not nullable, we can use an equality.
        let outer_nullability = outer_field.is_nullable();
        let inner_nullability = expr_builder_ctx
            .schema()
            .field_with_name(None, inner_column.name())?
            .is_nullable();

        let outer_ref_column =
            expr_builder_ctx.try_create_builder(Expr::OuterReferenceColumn(
                Arc::clone(outer_field),
                Column::new_unqualified(k),
            ))?;
        let encoding = expr_builder_ctx
            .rdf_fusion_context
            .encodings()
            .try_get_encoding_name(outer_field.data_type())
            .ok_or_else(|| {
                plan_datafusion_err!("Field '' in filter expression is not an RDF term.")
            })?;
        let inner_expr = expr_builder_ctx
            .try_create_builder(Expr::Column(Column::new_unqualified(format!(
                "__inner__{k}"
            ))))?
            .with_encoding(encoding)?
            .build()?;

        if !outer_nullability && !inner_nullability {
            Ok(outer_ref_column.build()?.eq(inner_expr))
        } else {
            outer_ref_column.build_is_compatible(inner_expr)
        }
    }

    //
    // Native to Term
    //

    /// Creates an expression that converts a native `i64` value into an RDF term.
    ///
    /// This is done by encoding the literal and calling [BuiltinName::NativeInt64AsTerm].
    pub fn native_int64_as_term(
        &self,
        expr: Expr,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let field = expr.to_field(self.schema)?.1;
        if field.data_type() != &DataType::Int64 {
            return plan_err!(
                "Expected Int64 argument for {}, got {}",
                BuiltinName::NativeInt64AsTerm,
                field.data_type()
            );
        }

        let udf = self.create_builtin_udf(BuiltinName::NativeInt64AsTerm)?;
        self.try_create_builder(udf.call(vec![expr]))
    }

    /// Creates an expression that converts a native boolean value into an RDF term.
    ///
    /// This is done by encoding the literal and calling [BuiltinName::NativeBooleanAsTerm].
    pub fn native_boolean_as_term(
        &self,
        expr: Expr,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let field = expr.to_field(self.schema)?.1;
        if field.data_type() != &DataType::Boolean {
            return plan_err!(
                "Expected boolean arguments for {}, got {}",
                BuiltinName::NativeBooleanAsTerm,
                field.data_type()
            );
        }

        let udf = self.create_builtin_udf(BuiltinName::NativeBooleanAsTerm)?;
        self.try_create_builder(udf.call(vec![expr]))
    }

    //
    // Logical
    //

    /// Creates a SPARQL logical AND expression.
    ///
    /// This differs from a standard AND by its error treatment.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Logical-and](https://www.w3.org/TR/sparql11-query/#func-logical-and)
    /// - [SPARQL 1.1 - Filter Evaluation](https://www.w3.org/TR/sparql11-query/#evaluation)
    pub fn and(&self, lhs: Expr, rhs: Expr) -> DFResult<RdfFusionExprBuilder<'context>> {
        let lhs_field = lhs.to_field(self.schema)?.1;
        let rhs_field = rhs.to_field(self.schema)?.1;
        if lhs_field.data_type() != &DataType::Boolean
            || rhs_field.data_type() != &DataType::Boolean
        {
            return plan_err!(
                "Expected boolean arguments for and, got {} and {}",
                lhs_field.data_type(),
                rhs_field.data_type()
            );
        }

        self.native_boolean_as_term(lhs.and(rhs))
    }

    /// Creates a SPARQL logical OR expression.
    ///
    /// This differs from a standard OR by its error treatment.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Logical-or](https://www.w3.org/TR/sparql11-query/#func-logical-or)
    /// - [SPARQL 1.1 - Filter Evaluation](https://www.w3.org/TR/sparql11-query/#evaluation)
    pub fn sparql_or(
        self,
        lhs: Expr,
        rhs: Expr,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let lhs_field = lhs.to_field(self.schema)?.1;
        let rhs_field = rhs.to_field(self.schema)?.1;
        if lhs_field.data_type() != &DataType::Boolean
            || rhs_field.data_type() != &DataType::Boolean
        {
            return plan_err!(
                "Expected boolean arguments for and, got {} and {}",
                lhs_field.data_type(),
                rhs_field.data_type()
            );
        }

        self.native_boolean_as_term(lhs.or(rhs))
    }

    //
    // Built-Ins
    //

    /// Applies a built-in SPARQL aggregate function to a list of arguments.
    pub(crate) fn apply_builtin_udaf(
        &self,
        name: BuiltinName,
        args: Vec<Expr>,
        distinct: bool,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udaf = self.registry().udaf(&FunctionName::Builtin(name))?;

        // Currently, UDAFs are only supported for typed values (we have an exception for count).
        // In the future we should solve this similar to the scalar functions
        let args = match name {
            BuiltinName::Count => args,
            _ => args
                .into_iter()
                .map(|e| {
                    self.try_create_builder(e)?
                        .with_encoding(EncodingName::TypedFamily)?
                        .build()
                })
                .collect::<DFResult<Vec<_>>>()?,
        };

        let expr = Expr::AggregateFunction(AggregateFunction::new_udf(
            udaf,
            args,
            distinct,
            None,
            Vec::new(),
            None,
        ));

        self.try_create_builder(expr)
    }

    /// Applies a built-in SPARQL function to a list of arguments.
    pub(crate) fn apply_builtin(
        &self,
        name: BuiltinName,
        further_args: Vec<Expr>,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        self.apply_builtin_with_args(name, further_args)
    }

    /// Applies a built-in SPARQL function to a list of arguments, with additional arguments
    /// for the user-defined function itself.
    ///
    /// This can be used to support functions like `IRI` that requires a base IRI.
    pub(crate) fn apply_builtin_with_args(
        &self,
        name: BuiltinName,
        args: Vec<Expr>,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let expr = self.apply_with_args_no_builder(&FunctionName::Builtin(name), args)?;
        self.try_create_builder(expr)
    }

    /// Similar to [Self::apply_builtin_with_args] but does not wrap the resulting expression
    /// in a builder. Therefore, this method can be used to create expressions that do not evaluate
    /// to an RDF term.
    pub(crate) fn apply_with_args_no_builder(
        &self,
        name: &FunctionName,
        args: Vec<Expr>,
    ) -> DFResult<Expr> {
        let udf = self.rdf_fusion_context.functions().udf(name)?;
        let supported_encodings = self.registry().udf_supported_encodings(name)?;

        if supported_encodings.is_empty() {
            return plan_err!("No supported encodings for builtin '{}'", name);
        }

        let encodings = self.get_encodings(&args)?;
        let input_encoding = decide_input_encoding(&supported_encodings, &encodings)?;

        let args = args
            .into_iter()
            .map(|expr| {
                self.try_create_builder(expr)?
                    .with_encoding(input_encoding)?
                    .build()
            })
            .collect::<DFResult<Vec<_>>>()?;

        Ok(udf.call(args))
    }

    //
    // Helper Functions
    //

    pub(crate) fn create_builtin_udf(
        &self,
        name: BuiltinName,
    ) -> DFResult<Arc<ScalarUDF>> {
        self.registry().udf(&FunctionName::Builtin(name))
    }

    /// Gets all encodings of the given `args`.
    ///
    /// # Errors
    ///
    /// Returns an error if any [Expr] does not evaluate to an RDF term.
    pub(crate) fn get_encodings(&self, args: &[Expr]) -> DFResult<Vec<EncodingName>> {
        args.iter()
            .map(|expr| Ok(expr.to_field(self.schema)?.1))
            .map(|field| {
                field.and_then(|field| {
                    self.encodings()
                        .try_get_encoding_name(field.data_type())
                        .ok_or(exec_datafusion_err!(
                            "Data type is not an RDF term '{}'",
                            field
                        ))
                })
            })
            .collect::<DFResult<Vec<_>>>()
    }
}

/// Returns one of the given supported encodings based on the given actual encodings.
pub(crate) fn decide_input_encoding(
    supported_encodings: &[EncodingName],
    actual_encodings: &[EncodingName],
) -> DFResult<EncodingName> {
    if supported_encodings.is_empty() {
        return plan_err!("No supported encodings");
    }

    // If all arguments have a supported encoding we choose this one.
    for supported_encoding in supported_encodings {
        if actual_encodings.iter().all(|e| e == supported_encoding) {
            return Ok(*supported_encoding);
        }
    }

    // Otherwise we currently return the first encoding.
    supported_encodings
        .iter()
        .filter_map(|enc| match enc {
            // We cannot convert to the object ID encoding
            EncodingName::ObjectId => None,
            enc => Some(*enc),
        })
        .next()
        .ok_or_else(|| plan_datafusion_err!("No supported encodings"))
}
