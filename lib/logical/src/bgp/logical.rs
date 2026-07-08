use crate::encoding::object_id::{
    InvalidDecodingColumnError, validate_columns_to_decode,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, ExprSchema};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use itertools::Itertools;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;
use thiserror::Error;

/// A logical node that represents a Basic Graph Pattern (BGP).
///
/// A BGP is a collection of quad patterns that are joined together. This node groups these patterns
/// to allow for joint optimization and planning, such as join ordering based on statistics.
#[derive(PartialEq, Eq, Hash)]
pub struct BgpNode {
    /// The patterns in the BGP.
    pub patterns: Vec<LogicalPlan>,
    /// The schema of the result.
    pub schema: DFSchemaRef,
    /// The filters to apply.
    pub filters: Vec<Expr>,
    /// The projection to apply.
    pub projection: Option<Vec<Column>>,
    /// The columns that need decoding.
    pub columns_to_decode: Vec<Column>,
}

impl BgpNode {
    /// Creates a new [BgpNode].
    ///
    /// Columns referenced in the filters will be automatically added to `columns_to_decode`.
    pub fn try_new(
        patterns: Vec<LogicalPlan>,
        filters: Vec<Expr>,
        projection: Option<Vec<Column>>,
        columns_to_decode: Vec<Column>,
    ) -> Result<Self, BgpNodeCreationError> {
        let merged_schema = if patterns.is_empty() {
            DFSchema::empty()
        } else {
            let mut schema = patterns[0].schema().as_ref().clone();
            for pattern in patterns.iter().skip(1) {
                schema.merge(pattern.schema().as_ref())
            }
            schema
        };

        let mut dedup_columns_to_decode = BTreeSet::new();
        dedup_columns_to_decode.extend(columns_to_decode);
        dedup_columns_to_decode.extend(extract_referenced_columns_if_object_id(
            &merged_schema,
            &filters,
        )?);
        let columns_to_decode = dedup_columns_to_decode.into_iter().collect::<Vec<_>>();

        validate_columns_to_decode(&merged_schema, &columns_to_decode)?;

        let schema =
            compute_schema(merged_schema, projection.as_deref(), &columns_to_decode)?;

        Ok(Self {
            patterns,
            schema,
            filters,
            projection,
            columns_to_decode,
        })
    }
}

/// Finds all columns that are referenced in the given expressions.
fn extract_referenced_columns_if_object_id(
    schema: &DFSchema,
    exprs: &[Expr],
) -> Result<Vec<Column>, BgpNodeCreationError> {
    let mut columns = BTreeSet::new();

    for expr in exprs {
        let _ = expr.apply(|e| {
            if let Expr::Column(c) = e {
                columns.insert(c.clone());
            }
            Ok::<_, DataFusionError>(TreeNodeRecursion::Continue)
        });
    }

    columns
        .into_iter()
        .flat_map(|c| match schema.field_from_column(&c) {
            Ok(field) => {
                if matches!(
                    field.data_type(),
                    DataType::Int32 | DataType::Int64 | DataType::FixedSizeBinary(_)
                ) {
                    Some(Ok(c))
                } else {
                    None
                }
            }
            Err(_) => {
                if c.name.ends_with("__oid") {
                    None
                } else {
                    Some(Err(BgpNodeCreationError::InvalidFilterExpr(c)))
                }
            }
        })
        .collect()
}

impl fmt::Debug for BgpNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for BgpNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for BgpNode {
    fn name(&self) -> &str {
        "BasicGraphPattern"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.patterns.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.filters.clone()
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BasicGraphPattern: ")?;

        if let Some(projection) = &self.projection {
            write!(
                f,
                "projection=[{}], ",
                projection.iter().map(|c| &c.name).format(", ")
            )?;
        }

        if !self.columns_to_decode.is_empty() {
            write!(
                f,
                "columns_to_decode=[{}], ",
                self.columns_to_decode.iter().map(|c| &c.name).format(", ")
            )?;
        }

        if !self.filters.is_empty() {
            write!(f, "filters=[{}]", self.filters.iter().format(", "))?;
        }

        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Self::try_new(
            inputs,
            exprs,
            self.projection.clone(),
            self.columns_to_decode.clone(),
        )?)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

/// Computes the schema of the BGP node.
fn compute_schema(
    merged_schema: DFSchema,
    projection: Option<&[Column]>,
    columns_to_decode: &[Column],
) -> Result<DFSchemaRef, BgpNodeCreationError> {
    let projected = match projection {
        None => merged_schema,
        Some(columns) => {
            let fields = columns
                .iter()
                .map(|c| {
                    Ok((
                        c.relation.clone(),
                        Arc::clone(merged_schema.field_from_column(c)?),
                    ))
                })
                .collect::<DFResult<Vec<_>>>()
                .map_err(|_| BgpNodeCreationError::InvalidColumnsToDecode)?;
            DFSchema::new_with_metadata(fields, merged_schema.metadata().clone())
                .expect("Schema should be a valid subset of the other schema")
        }
    };

    let decoded = if !columns_to_decode.is_empty() {
        let mut fields = projected.fields().to_vec();
        for to_decode in columns_to_decode {
            let Ok(idx) = projected.index_of_column(to_decode) else {
                continue; // Could be projected away
            };

            let new_field = fields[idx]
                .as_ref()
                .clone()
                .with_data_type(PLAIN_TERM_ENCODING.data_type().clone());
            fields[idx] = Arc::new(new_field);
        }
        let qualified = projected
            .columns()
            .iter()
            .map(|c| c.relation.clone())
            .zip(fields)
            .collect();
        DFSchema::new_with_metadata(qualified, projected.metadata().clone())
            .expect("Should be valid as projected was valid")
    } else {
        projected
    };

    Ok(Arc::new(decoded))
}

#[derive(Debug, Error)]
#[error("Could not create BGP node: {}")]
pub enum BgpNodeCreationError {
    #[error("An invalid column name was given for decoding")]
    InvalidColumnsToDecode,
    #[error("{0}")]
    InvalidDecodingColumn(String),
    #[error("A filter expression references column '{0}', which does not exist.")]
    InvalidFilterExpr(Column),
}

impl From<InvalidDecodingColumnError> for BgpNodeCreationError {
    fn from(value: InvalidDecodingColumnError) -> Self {
        BgpNodeCreationError::InvalidDecodingColumn(value.to_string())
    }
}

impl From<BgpNodeCreationError> for DataFusionError {
    fn from(value: BgpNodeCreationError) -> Self {
        DataFusionError::Plan(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};
    use std::sync::Arc;

    #[test]
    fn test_bgp_node_schema_merge() -> DFResult<()> {
        let lp1 = create_pattern("s", "http://example.org/p1", "o1");
        let lp2 = create_pattern("s", "http://example.org/p2", "o2");

        let bgp = BgpNode::try_new(vec![lp1, lp2], vec![], None, vec![])?;

        assert_eq!(bgp.schema.fields().len(), 3);
        assert!(bgp.schema.field_with_unqualified_name("s").is_ok());
        assert!(bgp.schema.field_with_unqualified_name("o1").is_ok());
        assert!(bgp.schema.field_with_unqualified_name("o2").is_ok());

        Ok(())
    }

    #[test]
    fn test_filter_columns_are_extracted_for_decoding() -> DFResult<()> {
        let lp1 = create_pattern("s", "http://example.org/p1", "o1");
        let filters = vec![col("s").eq(lit("test_subject")), col("o1").gt(lit(10))];
        let bgp = BgpNode::try_new(vec![lp1], filters, None, vec![])?;

        let decoded_names: Vec<_> = bgp
            .columns_to_decode
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        assert_eq!(decoded_names.len(), 2);
        assert!(decoded_names.contains(&"s"));
        assert!(decoded_names.contains(&"o1"));

        Ok(())
    }

    #[test]
    fn test_filter_column_extraction_deduplicates() -> DFResult<()> {
        let lp1 = create_pattern("s", "http://example.org/p1", "o1");
        let filters = vec![col("o1").gt(lit(10)), col("o1").lt(lit(50))];
        let bgp = BgpNode::try_new(vec![lp1], filters, None, vec![])?;

        let decoded_names: Vec<_> = bgp
            .columns_to_decode
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        assert_eq!(decoded_names.len(), 1);
        assert!(decoded_names.contains(&"o1"));

        Ok(())
    }

    #[test]
    fn test_filter_column_extraction_deduplicates_with_given_column_to_decode()
    -> DFResult<()> {
        let lp1 = create_pattern("s", "http://example.org/p1", "o1");
        let filters = vec![col("o1").gt(lit(10))];
        let bgp = BgpNode::try_new(
            vec![lp1],
            filters,
            None,
            vec![Column::new_unqualified("o1")],
        )?;

        let decoded_names: Vec<_> = bgp
            .columns_to_decode
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        assert_eq!(decoded_names.len(), 1);
        assert!(decoded_names.contains(&"o1"));

        Ok(())
    }

    /// Helper function to extract repetitive quad pattern initialization.
    fn create_pattern(
        subject_var: &str,
        _predicate_uri: &str,
        object_var: &str,
    ) -> LogicalPlan {
        use datafusion::arrow::datatypes::{DataType, Field};
        use std::collections::HashMap;
        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    (
                        None,
                        Arc::new(Field::new(subject_var, DataType::Int64, false)),
                    ),
                    (
                        None,
                        Arc::new(Field::new(object_var, DataType::Int64, false)),
                    ),
                ],
                HashMap::new(),
            )
            .unwrap(),
        );

        LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema,
        })
    }
}
