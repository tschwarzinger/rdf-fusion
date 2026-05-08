use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::common::{DFSchema, DFSchemaRef, plan_datafusion_err, plan_err};
use datafusion::logical_expr::{
    Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingName, RdfFusionEncodings};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Represents the type of join operation in SPARQL query processing.
///
/// This enum defines the different types of joins that can be performed
/// when combining solution mappings in SPARQL queries.
///
/// # Additional Resources
/// - [SPARQL 1.1 Query Language - Basic Graph Patterns](https://www.w3.org/TR/sparql11-query/#BasicGraphPatterns)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SparqlJoinType {
    /// An inner join that only includes solution mappings that are compatible.
    ///
    /// This is the standard join operation in SPARQL, where only solutions
    /// that have compatible values for shared variables are included in the result.
    Inner,
    /// A left outer join that preserves all solution mappings from the left side.
    ///
    /// This corresponds to the OPTIONAL keyword in SPARQL, where all solutions
    /// from the left pattern are preserved, with NULL values for variables
    /// that don't have compatible solutions in the right pattern.
    Left,
}

impl Display for SparqlJoinType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SparqlJoinType::Inner => write!(f, "Inner"),
            SparqlJoinType::Left => write!(f, "Left"),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SparqlJoinNode {
    encodings: RdfFusionEncodings,
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    filter: Option<Expr>,
    join_type: SparqlJoinType,
    schema: DFSchemaRef,
}

impl SparqlJoinNode {
    /// Creates a new SPARQL join node with the specified inputs, filter, and join type.
    ///
    /// This constructor validates that the inputs are compatible for joining according
    /// to SPARQL semantics, and that any filter expression is a boolean expression.
    ///
    /// # Arguments
    /// * `lhs` - The left-hand side logical plan
    /// * `rhs` - The right-hand side logical plan
    /// * `filter` - An optional filter expression to apply to the join
    /// * `join_type` - The type of join to perform (inner or left)
    ///
    /// # Returns
    /// A new `SparqlJoinNode` if the inputs are valid, or an error otherwise
    ///
    /// # Additional Resources
    /// - [SPARQL 1.1 Query Language - Filters](https://www.w3.org/TR/sparql11-query/#expressions)
    pub fn try_new(
        encodings: RdfFusionEncodings,
        lhs: LogicalPlan,
        rhs: LogicalPlan,
        filter: Option<Expr>,
        join_type: SparqlJoinType,
    ) -> DFResult<Self> {
        validate_inputs(&encodings, &lhs, &rhs)?;
        let schema = compute_schema(join_type, &lhs, &rhs)?;

        if let Some(filter) = &filter {
            let field = filter.to_field(&schema)?.1;
            if field.data_type() != &DataType::Boolean {
                return plan_err!("Filter must be a boolean expression.");
            }
        }

        Ok(Self {
            encodings,
            lhs,
            rhs,
            filter,
            join_type,
            schema,
        })
    }

    /// Returns a reference to the left-hand side logical plan of the join.
    ///
    /// This is the first input to the join operation.
    pub fn lhs(&self) -> &LogicalPlan {
        &self.lhs
    }

    /// Returns a reference to the right-hand side logical plan of the join.
    ///
    /// This is the second input to the join operation.
    pub fn rhs(&self) -> &LogicalPlan {
        &self.rhs
    }

    /// Returns a reference to the optional filter expression applied to the join.
    ///
    /// If present, this filter is applied after the join operation to further
    /// restrict the results.
    pub fn filter(&self) -> Option<&Expr> {
        self.filter.as_ref()
    }

    /// Returns the type of join operation (inner or left).
    ///
    /// This determines how solution mappings are combined and whether
    /// unmatched solutions from the left side are preserved.
    pub fn join_type(&self) -> SparqlJoinType {
        self.join_type
    }

    /// Consumes the join node and returns its components.
    ///
    /// This method is useful when you need to take ownership of the
    /// components of the join node, such as during transformation or
    /// optimization of the logical plan.
    ///
    /// # Returns
    /// A tuple containing the left plan, right plan, optional filter, and join type
    pub fn destruct(self) -> (LogicalPlan, LogicalPlan, Option<Expr>, SparqlJoinType) {
        (self.lhs, self.rhs, self.filter, self.join_type)
    }
}

impl fmt::Debug for SparqlJoinNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for SparqlJoinNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for SparqlJoinNode {
    fn name(&self) -> &str {
        "SparqlJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.lhs(), self.rhs()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        match &self.filter {
            None => vec![],
            Some(filter) => vec![filter.clone()],
        }
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let filter = self
            .filter
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        write!(f, "SparqlJoin: {} {}", self.join_type, &filter)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if exprs.len() > 1 {
            return plan_err!("SparqlJoinNode must not have more than one expression.");
        }

        let input_len = inputs.len();
        let Ok([lhs, rhs]) = TryInto::<[LogicalPlan; 2]>::try_into(inputs) else {
            return plan_err!(
                "SparqlJoinNode must have exactly two inputs, actual: {input_len}"
            );
        };

        let filter = exprs.first().cloned();
        Self::try_new(self.encodings.clone(), lhs, rhs, filter, self.join_type)
    }
}

/// Validates whether the two inputs are valid.
///
/// The following invariants are checked:
/// - Join variables must have the PlainTerm or ObjectId encoding.
#[allow(clippy::expect_used)]
fn validate_inputs(
    encodings: &RdfFusionEncodings,
    lhs: &LogicalPlan,
    rhs: &LogicalPlan,
) -> DFResult<()> {
    let join_column = compute_sparql_join_columns(encodings, lhs.schema(), rhs.schema())?;

    for (field_name, encodings) in join_column {
        if encodings.len() > 1 {
            return plan_err!("Join column '{field_name}' has multiple encodings.");
        }

        let encoding = encodings
            .into_iter()
            .next()
            .expect("Length already checked");
        if !matches!(
            encoding,
            EncodingName::PlainTerm | EncodingName::ObjectId | EncodingName::String
        ) {
            return plan_err!(
                "Join column '{field_name}' must be in the PlainTermEncoding, ObjectIdEncoding or StringEncoding."
            );
        }
    }

    Ok(())
}

/// Computes the schema for a SPARQL join operation based on the join type.
///
/// This function creates a new schema by merging the schemas of the left and right inputs.
/// For left joins, it ensures that fields from the right input are marked as nullable,
/// since they may not have values for all solutions from the left input.
///
/// # Arguments
/// * `join_type` - The type of join operation (inner or left)
/// * `lhs` - The left-hand side logical plan
/// * `rhs` - The right-hand side logical plan
///
/// # Returns
/// A new schema that combines fields from both input schemas
///
/// # Additional Resources
/// - [SPARQL 1.1 Query Language - Basic Graph Patterns](https://www.w3.org/TR/sparql11-query/#BasicGraphPatterns)
fn compute_schema(
    join_type: SparqlJoinType,
    lhs: &LogicalPlan,
    rhs: &LogicalPlan,
) -> DFResult<DFSchemaRef> {
    Ok(match join_type {
        SparqlJoinType::Inner => {
            let mut new_schema = lhs.schema().as_ref().clone();
            new_schema.merge(rhs.schema());
            Arc::new(new_schema)
        }
        SparqlJoinType::Left => {
            let optional_rhs_fields = rhs
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect::<Fields>();
            let rhs_schema = DFSchema::from_unqualified_fields(
                optional_rhs_fields,
                rhs.schema().metadata().clone(),
            )?;

            let mut new_schema = lhs.schema().as_ref().clone();
            new_schema.merge(&rhs_schema);
            Arc::new(new_schema)
        }
    })
}

/// Computes the columns that are being joined in a SPARQL join operation.
///
/// This function identifies the common columns between the left and right schemas
/// and returns a mapping of column names to their RDF Term encodings.
///
/// # Errors
///
/// This function returns an error if a join column is not an RDF Term. It does *not*
/// produce an error if they have a different encoding. It is up to the caller to
/// handle this situation.
pub fn compute_sparql_join_columns(
    encodings: &RdfFusionEncodings,
    lhs: &DFSchema,
    rhs: &DFSchema,
) -> DFResult<HashMap<String, HashSet<EncodingName>>> {
    /// Extracts the encoding of a field.
    ///
    /// It is expected that `name` is part of `schema`.
    #[allow(clippy::expect_used, reason = "Local function, Guarantees met below")]
    fn extract_encoding(
        encodings: &RdfFusionEncodings,
        schema: &DFSchema,
        name: &str,
    ) -> DFResult<EncodingName> {
        let field = schema
            .field_with_unqualified_name(name)
            .expect("Field name stems from the set of fields.");
        encodings
            .try_get_encoding_name(field.data_type())
            .ok_or(plan_datafusion_err!(
                "Field '{}' must be an RDF Term.",
                name
            ))
    }

    let lhs_fields = lhs
        .fields()
        .iter()
        .map(|f| f.name().to_owned())
        .collect::<HashSet<_>>();
    let rhs_fields = rhs
        .fields()
        .iter()
        .map(|f| f.name().to_owned())
        .collect::<HashSet<_>>();

    let mut result = HashMap::new();
    for field_name in lhs_fields.intersection(&rhs_fields) {
        let lhs_encoding = extract_encoding(encodings, lhs, field_name)?;
        let rhs_encoding = extract_encoding(encodings, rhs, field_name)?;
        result.insert(
            field_name.clone(),
            vec![lhs_encoding, rhs_encoding].into_iter().collect(),
        );
    }

    Ok(result)
}
