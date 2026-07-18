use crate::encoding::object_id::{DecodeObjectIdsNode, EncodeAsObjectIdNode};
use crate::extend::ExtendNode;
use crate::join::{SparqlJoinNode, SparqlJoinType, compute_sparql_join_columns};
use crate::logical_plan_builder_context::RdfFusionLogicalPlanBuilderContext;
use crate::minus::MinusNode;
use crate::patterns::PatternNode;
use crate::{RdfFusionExprBuilder, RdfFusionExprBuilderContext};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ExprSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, plan_datafusion_err, plan_err};
use datafusion::logical_expr::{
    Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder, Sort, SortExpr,
    UserDefinedLogicalNode, col,
};
use itertools::Itertools;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{DFResult, TermPattern};
use rdf_fusion_common::{RdfSortOrder, Variable};
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use std::collections::{BTreeSet, HashMap, HashSet};
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
/// # use rdf_fusion_encoding::string::STRING_ENCODING;
/// # use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
/// # use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;
/// # use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
/// # use rdf_fusion_common::{NamedNodePattern, TermPattern, TriplePattern, Variable};
/// # use rdf_fusion_logical::ActiveGraph;
/// # let encodings = RdfFusionEncodings::new(
/// #     Arc::clone(&PLAIN_TERM_ENCODING),
/// #     Arc::new(TypedFamilyEncoding::default()),
/// #     None,
/// #     Arc::clone(&STRING_ENCODING)
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
    /// Contains the decoded schema of the current logical plan.
    decoded_schema: DFSchemaRef,
    /// The context for the builder.
    context: RdfFusionLogicalPlanBuilderContext,
}

impl RdfFusionLogicalPlanBuilder {
    /// Creates a new [`RdfFusionLogicalPlanBuilder`] ensuring that the decoded schema is consistent
    /// with the current plan's schema.
    fn new_with_builder(
        context: RdfFusionLogicalPlanBuilderContext,
        plan_builder: LogicalPlanBuilder,
    ) -> Self {
        let decoded_schema = create_decoded_schema(
            plan_builder.schema(),
            context.encodings().object_id().map(|v| v.as_ref()),
        );
        Self {
            plan_builder,
            decoded_schema,
            context,
        }
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] with an existing `plan`.
    pub(crate) fn new(
        context: RdfFusionLogicalPlanBuilderContext,
        plan: Arc<LogicalPlan>,
    ) -> Self {
        let plan_builder = LogicalPlanBuilder::new_from_arc(plan);
        Self::new_with_builder(context, plan_builder)
    }

    /// Projects the current plan to a new set of variables.
    pub fn project(self, variables: &[Variable]) -> DFResult<Self> {
        let plan_builder = self.plan_builder.project(
            variables
                .iter()
                .map(|v| col(Column::new_unqualified(v.as_str()))),
        )?;
        Ok(Self::new_with_builder(self.context.clone(), plan_builder))
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
        let decoded = self.decode_for_exprs(std::slice::from_ref(&expression))?;
        let (_, field) = expression.to_field(decoded.schema())?;
        let expression = match field.data_type() {
            DataType::Boolean => expression,
            _ => decoded
                .expr_builder(expression)?
                .build_effective_boolean_value()?,
        };
        Ok(Self::new_with_builder(
            decoded.context.clone(),
            decoded.plan_builder.filter(expression)?,
        ))
    }

    /// Extends the current plan with a new variable binding.
    pub fn extend(
        self,
        variable: Variable,
        expr: Expr,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let decoded = self.decode_for_exprs(std::slice::from_ref(&expr))?;
        let inner = decoded.plan_builder.build()?;
        let extend_node = ExtendNode::try_new(inner, variable, expr)?;
        Ok(Self::new_with_builder(
            decoded.context.clone(),
            create_extension_plan(extend_node),
        ))
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
        let mut lhs = self;
        let mut rhs = rhs;
        let filter = filter;

        if let Some(f) = &filter {
            let lhs_decoded = lhs.decode_for_exprs(std::slice::from_ref(f))?;
            lhs = lhs_decoded;

            let rhs_builder = context.create(Arc::new(rhs));
            let rhs_decoded = rhs_builder.decode_for_exprs(std::slice::from_ref(f))?;
            rhs = rhs_decoded.build()?;
        }

        let (lhs, rhs) = lhs.align_encodings_of_common_columns(rhs)?;
        let join_node = SparqlJoinNode::try_new(
            context.encodings().clone(),
            lhs.build()?,
            rhs,
            filter,
            join_type,
        )?;

        Ok(Self::new_with_builder(
            context,
            LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
                node: Arc::new(join_node),
            })),
        ))
    }

    /// Creates a limit node that applies skip (`start`) and fetch (`length`) to `inner`.
    pub fn slice(
        self,
        start: usize,
        length: Option<usize>,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        Ok(Self::new_with_builder(
            self.context.clone(),
            self.plan_builder.limit(start, length)?,
        ))
    }

    /// Sorts the current plan by a given set of expressions.
    pub fn sort(self, expr: Vec<SortExpr>) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let all_columns: Vec<_> = self.schema().columns().into_iter().map(col).collect();
        let decoded = self.decode_for_exprs(&all_columns)?;

        let context = decoded.context.clone();
        let plan = LogicalPlan::Sort(Sort {
            input: Arc::new(decoded.build()?),
            expr,
            fetch: None,
        });

        Ok(Self::new_with_builder(
            context,
            LogicalPlanBuilder::new(plan),
        ))
    }

    /// Creates a union of the current plan and another plan.
    pub fn union(self, rhs: LogicalPlan) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let context = self.context.clone();

        let (lhs, rhs) = self.align_encodings_of_common_columns(rhs)?;
        Ok(Self::new_with_builder(
            context,
            lhs.plan_builder.union_by_name(rhs)?,
        ))
    }

    /// Subtracts the results of another plan from the current plan.
    pub fn minus(self, rhs: LogicalPlan) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let (lhs, rhs) = self.align_encodings_of_common_columns(rhs)?;
        let minus_node = MinusNode::new(lhs.plan_builder.build()?, rhs);
        Ok(Self::new_with_builder(
            lhs.context,
            create_extension_plan(minus_node),
        ))
    }

    /// Groups the current plan by a set of variables and applies aggregate expressions.
    pub fn group(
        self,
        variables: &[Variable],
        aggregates: &[(Variable, Expr)],
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let decode_exprs = aggregates.iter().map(|(_, e)| e.clone()).collect_vec();
        let decoded = self.decode_for_exprs(&decode_exprs)?;

        let group_expr = variables
            .iter()
            .map(|v| decoded.create_group_expr(v))
            .collect::<DFResult<Vec<_>>>()?;
        let aggr_expr = aggregates
            .iter()
            .map(|(v, e)| e.clone().alias(v.as_str()))
            .collect::<Vec<_>>();

        Ok(Self::new_with_builder(
            decoded.context,
            decoded.plan_builder.aggregate(group_expr, aggr_expr)?,
        ))
    }

    /// Creates an [Expr] that ensures that the grouped values uses an [EncodingName::PlainTerm]
    /// encoding.
    fn create_group_expr(&self, v: &Variable) -> DFResult<Expr> {
        Ok(self
            .expr_builder_root()
            .variable(v.as_ref())?
            .with_any_encoding(&[
                EncodingName::PlainTerm,
                EncodingName::String,
                EncodingName::ObjectId,
            ])?
            .build()?
            .alias(v.as_str()))
    }

    /// Removes duplicate solutions from the current plan.
    pub fn distinct(self) -> DFResult<RdfFusionLogicalPlanBuilder> {
        self.distinct_with_sort(Vec::new())
    }

    /// Removes duplicate solutions from the current plan, but might keep some.
    ///
    /// In this implementation, we treat `REDUCED` as `DISTINCT`.
    pub fn reduced(self) -> DFResult<RdfFusionLogicalPlanBuilder> {
        self.distinct()
    }

    /// Removes duplicate solutions from the current plan, with additional sorting.
    pub fn distinct_with_sort(
        self,
        sorts: Vec<SortExpr>,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let input = if sorts.is_empty() {
            self
        } else {
            let all_columns: Vec<_> =
                self.schema().columns().into_iter().map(col).collect();
            self.decode_for_exprs(&all_columns)?
        };

        if sorts.is_empty() {
            return Ok(Self::new_with_builder(
                input.context,
                input.plan_builder.distinct()?,
            ));
        }

        let schema = input.plan_builder.schema();
        let (on_expr, sorts) =
            create_distinct_on_expressions(input.expr_builder_root(), sorts)?;
        let select_expr = schema.columns().into_iter().map(col).collect();
        let sorts = if sorts.is_empty() { None } else { Some(sorts) };

        Ok(Self::new_with_builder(
            input.context,
            input
                .plan_builder
                .distinct_on(on_expr, select_expr, sorts)?,
        ))
    }

    /// Removes duplicate solutions from the current plan.
    pub fn pattern(
        self,
        pattern: Vec<Option<TermPattern>>,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let pattern_node = PatternNode::try_new(self.plan_builder.build()?, pattern)?;
        Ok(Self::new_with_builder(
            self.context,
            LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
                node: Arc::new(pattern_node),
            })),
        ))
    }

    /// Ensures all columns are encoded as the given encoding.
    pub fn with_encoding(
        self,
        encoding_name: EncodingName,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        if encoding_name == EncodingName::ObjectId {
            let object_id_encoding =
                self.context.encodings().object_id().ok_or_else(|| {
                    plan_datafusion_err!("Object ID encoding not configured")
                })?;
            let node = EncodeAsObjectIdNode::try_new(
                self.plan_builder.build()?,
                object_id_encoding.object_id_data_type(),
            )?;
            return Ok(Self::new_with_builder(
                self.context.clone(),
                create_extension_plan(node),
            ));
        }

        let columns = self.schema().columns();
        let exprs: Vec<Expr> = columns.iter().map(|c| col(c.clone())).collect();
        let builder = self.decode_for_exprs(&exprs)?;

        let schema = builder.schema();
        let mut proj_exprs = Vec::new();
        let mut needs_projection = false;
        for column in schema.columns() {
            let expr = builder
                .expr_builder(col(column.clone()))?
                .with_encoding(encoding_name)?
                .build()?;

            let is_column = matches!(&expr, Expr::Column(_));
            if !is_column {
                needs_projection = true;
            }

            proj_exprs.push(expr.alias(column.name.clone()));
        }

        if needs_projection {
            Ok(Self::new_with_builder(
                builder.context.clone(),
                builder.plan_builder.project(proj_exprs)?,
            ))
        } else {
            Ok(builder)
        }
    }

    /// Ensures all columns are encoded as plain terms.
    pub fn with_plain_terms(self) -> DFResult<RdfFusionLogicalPlanBuilder> {
        self.with_encoding(EncodingName::PlainTerm)
    }

    pub fn apply_rdf_sort_order(
        self,
        sort_order: &RdfSortOrder,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        if !self.has_quad_schema() {
            return plan_err!(
                "RDF sort order can only be applied to a plan that returns quads."
            );
        }

        match sort_order {
            RdfSortOrder::SparqlOrder(components) => {
                let sort_exprs: Vec<_> = components
                    .iter()
                    .map(|c| SortExpr::new(col(c.column_name()), true, true))
                    .collect();
                Ok(self.sort(sort_exprs)?)
            }
            RdfSortOrder::NativeOrder(components) => {
                let sort_exprs: Vec<_> = components
                    .iter()
                    .map(|c| SortExpr::new(col(c.column_name()), true, true))
                    .collect();
                let context = self.context().clone();
                let builder = self.into_inner().sort(sort_exprs)?;
                Ok(context.create(Arc::new(builder.build()?)))
            }
        }
    }

    /// Returns whether the current plan represents quads.
    fn has_quad_schema(&self) -> bool {
        let schema = self.schema();
        schema.has_column(&Column::new_unqualified(COL_GRAPH))
            && schema.has_column(&Column::new_unqualified(COL_SUBJECT))
            && schema.has_column(&Column::new_unqualified(COL_PREDICATE))
            && schema.has_column(&Column::new_unqualified(COL_OBJECT))
    }

    /// Returns the schema of the current plan.
    pub fn schema(&self) -> &DFSchemaRef {
        self.plan_builder.schema()
    }

    /// Returns the schema of the current plan.
    pub fn decoded_schema(&self) -> &DFSchemaRef {
        &self.decoded_schema
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
        let schema = self.decoded_schema().as_ref();
        self.context.expr_builder_context_with_schema(schema)
    }

    /// Returns a new [RdfFusionExprBuilder] for a given expression.
    pub fn expr_builder(&self, expr: Expr) -> DFResult<RdfFusionExprBuilder<'_>> {
        self.expr_builder_root().try_create_builder(expr)
    }

    /// Aligns all the encodings of the overlapping column (i.e., join columns) of the current
    /// graph pattern and `rhs`.
    fn align_encodings_of_common_columns(
        mut self,
        mut rhs: LogicalPlan,
    ) -> DFResult<(Self, LogicalPlan)> {
        let join_columns = compute_sparql_join_columns(
            self.context.encodings(),
            self.schema().as_ref(),
            rhs.schema().as_ref(),
        )?;

        if join_columns.is_empty() {
            return Ok((self, rhs));
        }

        // Before doing alignment projections, decode any ObjectId column if it is mixed
        // with other encodings.
        let mut lhs_decoded_cols = Vec::new();
        let mut rhs_decoded_cols = Vec::new();

        for (col_name, encodings) in &join_columns {
            if encodings.len() > 1 && encodings.contains(&EncodingName::ObjectId) {
                let column = Column::new_unqualified(col_name);
                // Check LHS
                if let Ok(field) = self.schema().field_from_column(&column) {
                    if matches!(
                        field.data_type(),
                        DataType::Int32 | DataType::Int64 | DataType::FixedSizeBinary(_)
                    ) {
                        lhs_decoded_cols.push(col(column.clone()));
                    }
                }
                // Check RHS
                if let Ok(field) = rhs.schema().field_from_column(&column) {
                    if matches!(
                        field.data_type(),
                        DataType::Int32 | DataType::Int64 | DataType::FixedSizeBinary(_)
                    ) {
                        rhs_decoded_cols.push(col(column));
                    }
                }
            }
        }

        if !lhs_decoded_cols.is_empty() {
            let new_self = self.decode_for_exprs(&lhs_decoded_cols)?;
            self = new_self;
        }
        if !rhs_decoded_cols.is_empty() {
            let rhs_builder = self.context.create(Arc::new(rhs));
            let rhs_builder = rhs_builder.decode_for_exprs(&rhs_decoded_cols)?;
            rhs = rhs_builder.build()?;
        }

        // Recompute join_columns since we might have changed the encodings
        let join_columns = compute_sparql_join_columns(
            self.context.encodings(),
            self.schema().as_ref(),
            rhs.schema().as_ref(),
        )?;

        if join_columns.is_empty() {
            let context = self.context.clone();
            let lhs = self.plan_builder.build()?;
            return Ok((Self::new(context, Arc::new(lhs)), rhs));
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

    /// Decodes all columns that are still Object IDs if they are needed in `exprs`,
    /// and returns the updated plan builder.
    pub fn decode_for_exprs(self, exprs: &[Expr]) -> DFResult<Self> {
        let Some(encoding) = self.context.encodings().object_id() else {
            return Ok(self);
        };

        // Collect referenced columns from the given expressions.
        let mut referenced_columns = BTreeSet::new();
        for expr in exprs {
            collect_referenced_columns(expr, &mut referenced_columns)?;
        }

        // Determine which of those columns are ObjectId typed.
        let schema = self.schema();
        let mut columns_to_decode = Vec::new();
        for col in referenced_columns {
            if let Ok(field) = schema.field_from_column(&col) {
                if field.data_type() == encoding.data_type() {
                    columns_to_decode.push(col);
                }
            }
        }

        if columns_to_decode.is_empty() {
            return Ok(self);
        }

        let decode_node =
            DecodeObjectIdsNode::try_new(self.plan_builder.build()?, columns_to_decode)?;
        let new_builder = Self::new_with_builder(
            self.context.clone(),
            LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
                node: Arc::new(decode_node),
            })),
        );

        Ok(new_builder)
    }
}

/// Creates new [Expr] that ensures that the encodings of the `join_column` align. If a join column
/// does not align, both columns in the left and right side are converted into the
/// [`PlainTermEncoding`](rdf_fusion_encoding::plain_term::PlainTermEncoding).
pub(crate) fn build_projections_for_encoding_alignment(
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
            .build_as_sortable_bytes()?;

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

/// Creates the decoded schema, replacing the object id type with the plain term encoding type.
fn create_decoded_schema(
    schema: &DFSchemaRef,
    object_id_encoding: Option<&ObjectIdEncoding>,
) -> DFSchemaRef {
    let Some(encoding) = object_id_encoding else {
        return Arc::clone(schema);
    };

    let mut fields = Vec::new();
    let mut modified = false;
    for (qualifier, field) in schema.iter() {
        if field.data_type() == encoding.data_type() {
            let decoded_type = PLAIN_TERM_ENCODING.data_type();
            fields.push((
                qualifier.cloned(),
                Arc::new(field.as_ref().clone().with_data_type(decoded_type.clone())),
            ));
            modified = true;
        } else {
            fields.push((qualifier.cloned(), Arc::clone(field)));
        }
    }

    if modified {
        Arc::new(
            DFSchema::new_with_metadata(fields, DFSchema::metadata(schema).clone())
                .expect("Failed to create decoded schema"),
        )
    } else {
        Arc::clone(schema)
    }
}

fn collect_referenced_columns(
    expr: &Expr,
    columns: &mut BTreeSet<Column>,
) -> DFResult<()> {
    expr.apply(|e| {
        match e {
            Expr::Column(c) => {
                columns.insert(c.clone());
            }
            Expr::OuterReferenceColumn(_, c) => {
                columns.insert(c.clone());
            }
            Expr::Exists(_) => {}
            Expr::InSubquery(datafusion::logical_expr::expr::InSubquery {
                expr, ..
            }) => {
                collect_referenced_columns(expr, columns)?;
            }
            Expr::ScalarSubquery(_) => {}
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}
