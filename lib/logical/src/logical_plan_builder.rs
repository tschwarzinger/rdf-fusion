use crate::extend::ExtendNode;
use crate::join::{SparqlJoinNode, SparqlJoinType, compute_sparql_join_columns};
use crate::logical_plan_builder_context::RdfFusionLogicalPlanBuilderContext;
use crate::minus::MinusNode;
use crate::patterns::PatternNode;
use crate::{RdfFusionExprBuilder, RdfFusionExprBuilderContext};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, DFSchemaRef};
use datafusion::logical_expr::{
    Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder, Sort, SortExpr,
    UserDefinedLogicalNode, col,
};
use rdf_fusion_encoding::EncodingName;
use rdf_fusion_model::Variable;
use rdf_fusion_model::{DFResult, TermPattern};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A convenient builder for programmatically creating SPARQL queries.
///
/// # Example
///
/// The following example creates a query that selects the subject of each triple.
///
/// ```rust
/// # use std::sync::Arc;
/// # use datafusion::logical_expr::LogicalPlan;
/// # use rdf_fusion_extensions::RdfFusionContextView;
/// # use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
/// # use rdf_fusion_encoding::{QuadStorageEncoding, RdfFusionEncodings};
/// # use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
/// # use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
/// # use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;
/// # use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
/// # use rdf_fusion_model::{NamedNodePattern, TermPattern, TriplePattern, Variable};
/// # use rdf_fusion_logical::ActiveGraph;
/// # let encodings = RdfFusionEncodings::new(
/// #     Arc::clone(&PLAIN_TERM_ENCODING),
/// #     Arc::new(TypedFamilyEncoding::default()),
/// #     None,
/// #     Arc::clone(&SORTABLE_TERM_ENCODING)
/// # );
/// # let rdf_fusion_context = RdfFusionContextView::new(
/// #     Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone())),
/// #     encodings,
/// #     QuadStorageEncoding::PlainTerm
/// # );
///
/// let subject = Variable::new_unchecked("s");
/// let predicate = Variable::new_unchecked("p");
/// let object = Variable::new_unchecked("o");
///
/// let pattern = TriplePattern {
///     subject: TermPattern::Variable(subject.clone()),
///     predicate: NamedNodePattern::Variable(predicate),
///     object: TermPattern::Variable(object),
/// };
///
/// let builder_context = RdfFusionLogicalPlanBuilderContext::new(rdf_fusion_context);
/// let plan: LogicalPlan = builder_context
///     .create_pattern(ActiveGraph::DefaultGraph, None, pattern)
///     .project(&[subject])
///     .unwrap()
///     .build()
///     .unwrap();
///
/// # drop(plan)
/// ```
#[derive(Debug, Clone)]
pub struct RdfFusionLogicalPlanBuilder {
    /// The inner DataFusion [LogicalPlanBuilder].
    ///
    /// We do not use [LogicalPlan] directly as we want to leverage the convenience (and validation)
    /// that the [LogicalPlanBuilder] provides.
    plan_builder: LogicalPlanBuilder,
    /// The context for the builder.
    context: RdfFusionLogicalPlanBuilderContext,
}

impl RdfFusionLogicalPlanBuilder {
    /// Creates a new [RdfFusionLogicalPlanBuilder] with an existing `plan`.
    pub(crate) fn new(
        context: RdfFusionLogicalPlanBuilderContext,
        plan: Arc<LogicalPlan>,
    ) -> Self {
        let plan_builder = LogicalPlanBuilder::new_from_arc(plan);
        Self {
            plan_builder,
            context,
        }
    }

    /// Projects the current plan to a new set of variables.
    pub fn project(self, variables: &[Variable]) -> DFResult<Self> {
        let plan_builder = self.plan_builder.project(
            variables
                .iter()
                .map(|v| col(Column::new_unqualified(v.as_str()))),
        )?;
        Ok(Self {
            context: self.context.clone(),
            plan_builder,
        })
    }

    /// Applies a filter using `expression`.
    ///
    /// The filter expression is evaluated for each solution. If the effective boolean value of the
    /// expression is `true`, the solution is kept; otherwise, it is discarded.
    ///
    /// If the expression does not evaluate to a boolean, its effective boolean value is
    /// determined according to SPARQL rules.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Effective Boolean Value (EBV)](https://www.w3.org/TR/sparql11-query/#ebv)
    pub fn filter(self, expression: Expr) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let field = expression.to_field(self.schema())?.1;
        let expression = match field.data_type() {
            // If the expression already evaluates to a Boolean, we can use it directly.
            DataType::Boolean => expression,
            // Otherwise, obtain the EBV. This will trigger an error on an unknown encoding.
            _ => self
                .expr_builder(expression)?
                .build_effective_boolean_value()?,
        };

        Ok(Self {
            context: self.context.clone(),
            plan_builder: self.plan_builder.filter(expression)?,
        })
    }

    /// Extends the current plan with a new variable binding.
    pub fn extend(
        self,
        variable: Variable,
        expr: Expr,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let inner = self.plan_builder.build()?;
        let extend_node = ExtendNode::try_new(inner, variable, expr)?;
        Ok(Self {
            context: self.context.clone(),
            plan_builder: create_extension_plan(extend_node),
        })
    }

    /// Creates a join node of two logical plans that contain encoded RDF Terms.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Compatible Mappings](https://www.w3.org/TR/sparql11-query/#defn_algCompatibleMapping)
    pub fn join(
        self,
        rhs: LogicalPlan,
        join_type: SparqlJoinType,
        filter: Option<Expr>,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let context = self.context.clone();

        let (lhs, rhs) = self.align_encodings_of_common_columns(rhs)?;
        let join_node = SparqlJoinNode::try_new(
            context.encodings().clone(),
            lhs.build()?,
            rhs,
            filter,
            join_type,
        )?;
        Ok(Self {
            context,
            plan_builder: LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
                node: Arc::new(join_node),
            })),
        })
    }

    /// Creates a limit node that applies skip (`start`) and fetch (`length`) to `inner`.
    pub fn slice(
        self,
        start: usize,
        length: Option<usize>,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        Ok(Self {
            context: self.context.clone(),
            plan_builder: self.plan_builder.limit(start, length)?,
        })
    }

    /// Sorts the current plan by a given set of expressions.
    pub fn order_by(self, exprs: &[SortExpr]) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let exprs = exprs
            .iter()
            .map(|sort| self.ensure_sortable(sort))
            .collect::<DFResult<Vec<_>>>()?;

        let context = self.context.clone();
        let plan = LogicalPlan::Sort(Sort {
            input: Arc::new(self.build()?),
            expr: exprs,
            fetch: None,
        });

        Ok(Self {
            context,
            plan_builder: LogicalPlanBuilder::new(plan),
        })
    }

    /// Ensure that the [EncodingName::Sortable] is used.
    fn ensure_sortable(&self, e: &SortExpr) -> DFResult<SortExpr> {
        let expr = self
            .expr_builder(e.expr.clone())?
            .with_encoding(EncodingName::Sortable)?
            .build()?;
        Ok(SortExpr::new(expr, e.asc, e.nulls_first))
    }

    /// Creates a union of the current plan and another plan.
    pub fn union(self, rhs: LogicalPlan) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let context = self.context.clone();

        let (lhs, rhs) = self.align_encodings_of_common_columns(rhs)?;
        Ok(Self {
            context,
            plan_builder: lhs.plan_builder.union_by_name(rhs)?,
        })
    }

    /// Subtracts the results of another plan from the current plan.
    pub fn minus(self, rhs: LogicalPlan) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let minus_node = MinusNode::new(self.plan_builder.build()?, rhs);
        Ok(Self {
            context: self.context,
            plan_builder: create_extension_plan(minus_node),
        })
    }

    /// Groups the current plan by a set of variables and applies aggregate expressions.
    pub fn group(
        self,
        variables: &[Variable],
        aggregates: &[(Variable, Expr)],
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let group_expr = variables
            .iter()
            .map(|v| self.create_group_expr(v))
            .collect::<DFResult<Vec<_>>>()?;
        let aggr_expr = aggregates
            .iter()
            .map(|(v, e)| e.clone().alias(v.as_str()))
            .collect::<Vec<_>>();

        Ok(Self {
            context: self.context,
            plan_builder: self.plan_builder.aggregate(group_expr, aggr_expr)?,
        })
    }

    /// Creates an [Expr] that ensures that the grouped values uses an [EncodingName::PlainTerm]
    /// encoding.
    fn create_group_expr(&self, v: &Variable) -> DFResult<Expr> {
        Ok(self
            .expr_builder_root()
            .variable(v.as_ref())?
            .with_any_encoding(&[EncodingName::PlainTerm, EncodingName::ObjectId])?
            .build()?
            .alias(v.as_str()))
    }

    /// Removes duplicate solutions from the current plan.
    pub fn distinct(self) -> DFResult<RdfFusionLogicalPlanBuilder> {
        self.distinct_with_sort(Vec::new())
    }

    /// Removes duplicate solutions from the current plan, with additional sorting.
    pub fn distinct_with_sort(
        self,
        sorts: Vec<SortExpr>,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        if sorts.is_empty() {
            return Ok(Self {
                context: self.context,
                plan_builder: self.plan_builder.distinct()?,
            });
        }

        let schema = self.plan_builder.schema();
        let (on_expr, sorts) =
            create_distinct_on_expressions(self.expr_builder_root(), sorts)?;
        let select_expr = schema.columns().into_iter().map(col).collect();
        let sorts = if sorts.is_empty() { None } else { Some(sorts) };

        Ok(Self {
            context: self.context,
            plan_builder: self.plan_builder.distinct_on(on_expr, select_expr, sorts)?,
        })
    }

    /// Removes duplicate solutions from the current plan.
    pub fn pattern(
        self,
        pattern: Vec<Option<TermPattern>>,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let pattern_node = PatternNode::try_new(self.plan_builder.build()?, pattern)?;
        Ok(Self {
            context: self.context,
            plan_builder: LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
                node: Arc::new(pattern_node),
            })),
        })
    }

    /// Ensures all columns are encoded as plain terms.
    pub fn with_plain_terms(self) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let with_correct_encoding = self
            .schema()
            .columns()
            .into_iter()
            .map(|c| {
                let name = c.name().to_owned();
                let expr = self
                    .expr_builder(col(c))?
                    .with_encoding(EncodingName::PlainTerm)?
                    .build()?
                    .alias(name);
                Ok(expr)
            })
            .collect::<DFResult<Vec<_>>>()?;
        Ok(Self {
            context: self.context,
            plan_builder: self.plan_builder.project(with_correct_encoding)?,
        })
    }

    /// Returns the schema of the current plan.
    pub fn schema(&self) -> &DFSchemaRef {
        self.plan_builder.schema()
    }

    /// Returns the builder context.
    pub fn context(&self) -> &RdfFusionLogicalPlanBuilderContext {
        &self.context
    }

    /// Consumes the builder and returns the inner `LogicalPlanBuilder`.
    pub fn into_inner(self) -> LogicalPlanBuilder {
        self.plan_builder
    }

    /// Builds the `LogicalPlan`.
    pub fn build(self) -> DFResult<LogicalPlan> {
        self.plan_builder.build()
    }

    /// Returns a new [RdfFusionExprBuilderContext].
    pub fn expr_builder_root(&self) -> RdfFusionExprBuilderContext<'_> {
        let schema = self.schema().as_ref();
        self.context.expr_builder_context_with_schema(schema)
    }

    /// Returns a new [RdfFusionExprBuilder] for a given expression.
    pub fn expr_builder(&self, expr: Expr) -> DFResult<RdfFusionExprBuilder<'_>> {
        self.expr_builder_root().try_create_builder(expr)
    }

    /// Aligns all the encodings of the overlapping column (i.e., join columns) of the current
    /// graph pattern and `rhs`.
    fn align_encodings_of_common_columns(
        self,
        rhs: LogicalPlan,
    ) -> DFResult<(Self, LogicalPlan)> {
        let join_columns = compute_sparql_join_columns(
            self.context.encodings(),
            self.schema().as_ref(),
            rhs.schema().as_ref(),
        )?;

        if join_columns.is_empty() {
            return Ok((self, rhs));
        }

        let lhs_expr_builder =
            self.context.expr_builder_context_with_schema(self.schema());
        let rhs_expr_builder =
            self.context.expr_builder_context_with_schema(rhs.schema());

        let lhs_projections =
            build_projections_for_encoding_alignment(lhs_expr_builder, &join_columns)?;
        let lhs = match lhs_projections {
            None => self.plan_builder.build()?,
            Some(projections) => self.plan_builder.project(projections)?.build()?,
        };

        let rhs_projections =
            build_projections_for_encoding_alignment(rhs_expr_builder, &join_columns)?;
        let rhs = match rhs_projections {
            None => rhs,
            Some(projections) => {
                LogicalPlanBuilder::new(rhs).project(projections)?.build()?
            }
        };

        let context = self.context.clone();
        Ok((Self::new(context, Arc::new(lhs)), rhs))
    }
}

/// Creates new [Expr] that ensures that the encodings of the `join_column` align. If a join column
/// does not align, both columns in the left and right side are converted into the
/// [PlainTermEncoding].
fn build_projections_for_encoding_alignment(
    expr_builder_root: RdfFusionExprBuilderContext<'_>,
    join_columns: &HashMap<String, HashSet<EncodingName>>,
) -> DFResult<Option<Vec<Expr>>> {
    let projections = expr_builder_root
        .schema()
        .fields()
        .iter()
        .map(|f| {
            if let Some(encodings) = join_columns.get(f.name()) {
                let expr = col(Column::new_unqualified(f.name()));

                if encodings.len() > 1 {
                    let expr = expr_builder_root.try_create_builder(expr)?;
                    Ok(expr
                        .with_encoding(EncodingName::PlainTerm)?
                        .build()?
                        .alias(f.name()))
                } else {
                    Ok(expr)
                }
            } else {
                Ok(col(Column::new_unqualified(f.name())))
            }
        })
        .collect::<DFResult<Vec<_>>>()?;

    if projections.iter().all(|e| matches!(e, Expr::Column(_))) {
        Ok(None)
    } else {
        Ok(Some(projections))
    }
}

fn create_distinct_on_expressions(
    expr_builder_root: RdfFusionExprBuilderContext<'_>,
    mut sort_expr: Vec<SortExpr>,
) -> DFResult<(Vec<Expr>, Vec<SortExpr>)> {
    let mut on_expr = sort_expr
        .iter()
        .map(|se| se.expr.clone())
        .collect::<Vec<_>>();

    for column in expr_builder_root.schema().columns() {
        let expr = col(column.clone());
        let sortable_expr = expr_builder_root
            .try_create_builder(expr.clone())?
            .with_encoding(EncodingName::Sortable)?
            .build()?;

        // If, initially, the sortable expression is already part of on_expr we don't re-add it.
        if !on_expr.contains(&sortable_expr) {
            on_expr.push(expr.clone());
            sort_expr.push(SortExpr::new(expr, true, true))
        }
    }

    Ok((on_expr, sort_expr))
}

/// Creates a `LogicalPlanBuilder` from a user-defined logical node.
fn create_extension_plan(
    node: impl UserDefinedLogicalNode + 'static,
) -> LogicalPlanBuilder {
    LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}
