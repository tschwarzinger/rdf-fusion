use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::scan_instructions::{
    MemIndexScanPredicate, MemIndexScanPredicateSource,
};
use datafusion::common::{ScalarValue, exec_datafusion_err, exec_err};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal,
};
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::sync::Arc;
use thiserror::Error;

/// Represents the set of supported operations for [MemStoragePredicateExpr]s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PredicateExprOperator {
    Gt,
    GtEq,
    Lt,
    LtEq,
    Eq,
}

impl PredicateExprOperator {
    /// Flips the operator. For example, `>` becomes ´<`.
    pub fn flip(self) -> Self {
        match self {
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Eq => Self::Eq,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
#[error("Unsupported operator.")]
pub struct UnsupportedOperatorError;

impl TryFrom<Operator> for PredicateExprOperator {
    type Error = UnsupportedOperatorError;

    fn try_from(value: Operator) -> Result<Self, Self::Error> {
        Ok(match value {
            Operator::Lt => PredicateExprOperator::Lt,
            Operator::LtEq => PredicateExprOperator::LtEq,
            Operator::Gt => PredicateExprOperator::Gt,
            Operator::GtEq => PredicateExprOperator::GtEq,
            Operator::Eq => PredicateExprOperator::Eq,
            _ => return Err(UnsupportedOperatorError),
        })
    }
}

/// Represents a predicate that has been pushed down and is supported by our implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MemStoragePredicateExpr {
    /// The filter is always true.
    True,
    /// A reference to a column.
    Column(Arc<str>),
    /// An object id.
    ObjectId(EncodedObjectId),
    /// Applies a [PredicateExprOperator] to a column and a scalar.
    Binary(Arc<str>, PredicateExprOperator, EncodedObjectId),
    /// Checks that a column is between two object ids.
    Between(Arc<str>, EncodedObjectId, EncodedObjectId),
    /// Holds a dynamic filter that will be evaluated during query execution.
    Dynamic(Arc<DynamicFilterPhysicalExpr>),
}

impl MemStoragePredicateExpr {
    /// Tries to create a [MemStoragePredicateExpr] from an arbitrary [PhysicalExpr].
    ///
    /// If [None] is returned, the expression was not supported.
    pub fn try_from(expr: &Arc<dyn PhysicalExpr>) -> Option<Self> {
        // We explicitly do not copy the `Arc` as the filter might be considered as unused.
        if expr
            .as_any()
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .is_some()
        {
            let cloned = Arc::clone(expr)
                .with_new_children(
                    expr.children().iter().map(|c| Arc::clone(*c)).collect(),
                )
                .expect("Old children are valid");
            let cast = (cloned as Arc<dyn Any + Send + Sync>)
                .downcast::<DynamicFilterPhysicalExpr>()
                .expect("Type checked above");

            return Some(MemStoragePredicateExpr::Dynamic(cast));
        }

        try_rewrite_datafusion_expr(expr)
    }

    /// Returns the name of the column that this predicate operates on.
    ///
    /// Will be [None] for expressions that do not have a column name.
    pub fn column(&self) -> Option<&str> {
        match self {
            Self::Column(column) => Some(column.as_ref()),
            Self::Binary(column, _, _) => Some(column.as_ref()),
            Self::Between(column, _, _) => Some(column.as_ref()),
            _ => None,
        }
    }

    /// Returns the [MemIndexScanPredicate] that implements this expression. For dynamic
    /// expressions, the current snapshot will be used.
    ///
    /// Returns [None] if the expression always evaluates to true.
    /// Returns [Err] if the expression is not a predicate.
    pub fn to_scan_predicate(&self) -> DFResult<Option<MemIndexScanPredicate>> {
        use MemIndexScanPredicate::*;

        Ok(match self {
            MemStoragePredicateExpr::True => None,

            MemStoragePredicateExpr::Binary(_, operator, value) => Some(match operator {
                PredicateExprOperator::Gt => {
                    let Some(next) = value.next() else {
                        return Ok(Some(False));
                    };
                    Between(next, EncodedObjectId::MAX)
                }
                PredicateExprOperator::GtEq => Between(*value, EncodedObjectId::MAX),
                PredicateExprOperator::Lt => {
                    let Some(previous) = value.previous() else {
                        return Ok(Some(False));
                    };
                    Between(EncodedObjectId::MIN, previous)
                }
                PredicateExprOperator::LtEq => Between(EncodedObjectId::MIN, *value),
                PredicateExprOperator::Eq => In(BTreeSet::from([*value])),
            }),
            MemStoragePredicateExpr::Between(_, from, to) => Some(Between(*from, *to)),

            // For dynamic expressions, we use the current snapshot.
            MemStoragePredicateExpr::Dynamic(dynamic_expr) => dynamic_expr
                .snapshot()?
                .and_then(|expr| MemStoragePredicateExpr::try_from(&expr))
                .and_then(|expr| expr.to_scan_predicate().transpose())
                .transpose()?,

            MemStoragePredicateExpr::Column(_) | MemStoragePredicateExpr::ObjectId(_) => {
                return exec_err!("Expression is not a predicate.");
            }
        })
    }
}

/// Tries to rewrite an arbitrary DataFusion [PhysicalExpr] into a supported
/// [MemStoragePredicateExpr].
pub fn try_rewrite_datafusion_expr(
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<MemStoragePredicateExpr> {
    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
        return Some(MemStoragePredicateExpr::Column(
            column.name().to_owned().into(),
        ));
    }

    if let Some(lit) = expr.as_any().downcast_ref::<Literal>() {
        return match lit.value() {
            ScalarValue::FixedSizeBinary(_, Some(value)) => {
                Some(MemStoragePredicateExpr::ObjectId(
                    EncodedObjectId::from_4_byte_slice(value.as_ref()),
                ))
            }
            ScalarValue::Boolean(Some(true)) => Some(MemStoragePredicateExpr::True),
            _ => None,
        };
    }

    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        return match binary.op() {
            Operator::Eq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq => {
                let left = try_rewrite_datafusion_expr(binary.left())?;
                let right = try_rewrite_datafusion_expr(binary.right())?;

                let op = PredicateExprOperator::try_from(*binary.op()).ok()?;

                let (column, literal, op) = match (left, right) {
                    (
                        MemStoragePredicateExpr::Column(left),
                        MemStoragePredicateExpr::ObjectId(right),
                    ) => (left, right, op),
                    (
                        MemStoragePredicateExpr::ObjectId(right),
                        MemStoragePredicateExpr::Column(left),
                    ) => (left, right, op.flip()),
                    _ => return None,
                };

                Some(MemStoragePredicateExpr::Binary(column, op, literal))
            }
            Operator::And => try_rewrite_and_expr(binary),
            _ => return None,
        };
    }

    None
}

/// Rewrites a logical and expression into a [MemStoragePredicateExpr].
///
/// If only one side of the and can be rewritten, the other side will be ignored. This approximates
/// the actual filter condition as an additional AND clause can only become stricter. This helps
/// also with handling dynamic filter pushdowns that include ranges and `IN` expressions.
fn try_rewrite_and_expr(binary: &BinaryExpr) -> Option<MemStoragePredicateExpr> {
    let left = try_rewrite_datafusion_expr(binary.left());
    let right = try_rewrite_datafusion_expr(binary.right());

    match (left, right) {
        (None, None) => None,
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (Some(left), Some(right)) => {
            let MemStoragePredicateExpr::Binary(lhs_column, _, _) = &left else {
                return None;
            };
            let MemStoragePredicateExpr::Binary(rhs_column, _, _) = &right else {
                return None;
            };

            if lhs_column != rhs_column {
                return None;
            }

            let left = left.to_scan_predicate().ok().flatten()?;
            let right = right.to_scan_predicate().ok().flatten()?;
            match left.try_and_with(&right)? {
                MemIndexScanPredicate::Between(from, to) => Some(
                    MemStoragePredicateExpr::Between(Arc::clone(lhs_column), from, to),
                ),
                _ => None,
            }
        }
    }
}

/// A Wrapper around a [DynamicFilterPhysicalExpr] that implements [MemIndexScanPredicateSource].
#[derive(Debug)]
pub struct DynamicFilterScanPredicateSource(Arc<DynamicFilterPhysicalExpr>);

impl DynamicFilterScanPredicateSource {
    /// Creates a new [DynamicFilterScanPredicateSource].
    pub fn new(expr: Arc<DynamicFilterPhysicalExpr>) -> Self {
        Self(expr)
    }
}

impl MemIndexScanPredicateSource for DynamicFilterScanPredicateSource {
    fn current_predicate_expr(&self) -> DFResult<MemStoragePredicateExpr> {
        let expr = self.0.current()?;
        try_rewrite_datafusion_expr(&expr)
            .ok_or_else(|| exec_datafusion_err!("Unsupported predicate."))
    }
}

impl Display for DynamicFilterScanPredicateSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let generation = self.0.snapshot_generation();
        let Ok(predicate) = self.current_predicate_expr() else {
            return write!(
                f,
                "DynamicFilter [ Generation {generation}; Cannot obtain current predicate expr. ]"
            );
        };

        let column = predicate.column().unwrap_or("Unknown");
        let predicate = predicate
            .to_scan_predicate()
            .map(|expr| {
                expr.map(|expr| expr.to_string())
                    .unwrap_or_else(|| "true".to_string())
            })
            .unwrap_or_else(|_| "Cannot create scan predicate".to_string());
        write!(
            f,
            "DynamicFilter [ Generation {generation}; {column} {predicate} ]"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use MemIndexScanPredicate::*;

    #[test]
    fn test_column_predicate() {
        let expr = column_expr("subject");
        let result = try_rewrite_datafusion_expr(&expr);

        assert!(matches!(result, Some(MemStoragePredicateExpr::Column(_))));
    }

    #[test]
    fn test_literal_object_id() {
        let expr = literal_binary(42);
        let result = try_rewrite_datafusion_expr(&expr);

        assert!(matches!(result, Some(MemStoragePredicateExpr::ObjectId(_))));
    }

    #[test]
    fn test_true_literal() {
        let expr = literal_bool(true);
        let result = try_rewrite_datafusion_expr(&expr);

        assert!(matches!(result, Some(MemStoragePredicateExpr::True)));
    }

    #[test]
    fn test_equal_predicate() {
        let left = column_expr("subject");
        let right = literal_binary(123);
        let expr =
            Arc::new(BinaryExpr::new(left, Operator::Eq, right)) as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_datafusion_expr(&expr);

        assert!(matches!(
            result,
            Some(MemStoragePredicateExpr::Binary(
                _,
                PredicateExprOperator::Eq,
                _
            ))
        ));
    }

    #[test]
    fn test_between_predicate() {
        let gt_expr = Arc::new(BinaryExpr::new(
            column_expr("subject"),
            Operator::Gt,
            literal_binary(123),
        )) as Arc<dyn PhysicalExpr>;
        let lt_expr = Arc::new(BinaryExpr::new(
            column_expr("subject"),
            Operator::LtEq,
            literal_binary(456),
        )) as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(gt_expr, Operator::And, lt_expr))
            as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_datafusion_expr(&expr).unwrap();
        let MemStoragePredicateExpr::Between(column, from, to) = result else {
            panic!("Unexpected expr.")
        };

        assert_eq!(column.as_ref(), "subject");
        assert_eq!(from.as_bytes(), 124u32.to_be_bytes());
        assert_eq!(to.as_bytes(), 456u32.to_be_bytes());
    }

    #[test]
    fn test_between_predicate_wrong_column_name() {
        let gt_expr = Arc::new(BinaryExpr::new(
            column_expr("subject"),
            Operator::Gt,
            literal_binary(123),
        )) as Arc<dyn PhysicalExpr>;
        let lt_expr = Arc::new(BinaryExpr::new(
            column_expr("predicate"),
            Operator::Lt,
            literal_binary(456),
        )) as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(gt_expr, Operator::And, lt_expr))
            as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_datafusion_expr(&expr);

        assert!(matches!(result, None));
    }

    #[test]
    fn test_unsupported_operator() {
        let left = column_expr("subject");
        let right = literal_binary(123);
        let expr = Arc::new(BinaryExpr::new(left, Operator::Plus, right))
            as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_datafusion_expr(&expr);

        assert!(result.is_none());
    }

    #[test]
    fn test_to_scan_predicate_true_returns_none() {
        let result = MemStoragePredicateExpr::True.to_scan_predicate().unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_to_scan_predicate_eq_produces_in_predicate() {
        let obj_id = EncodedObjectId::from(100);
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::Eq,
            obj_id,
        );

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(In(BTreeSet::from([obj_id])))
        );
    }

    #[test]
    fn test_to_scan_predicate_gt_produces_between() {
        let obj_id = EncodedObjectId::from(100);
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::Gt,
            obj_id,
        );

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(Between(EncodedObjectId::from(101), EncodedObjectId::MAX))
        );
    }

    #[test]
    fn test_to_scan_predicate_geq_produces_between() {
        let obj_id = EncodedObjectId::from(100);
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::GtEq,
            obj_id,
        );

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(Between(EncodedObjectId::from(100), EncodedObjectId::MAX))
        );
    }

    #[test]
    fn test_to_scan_predicate_lt_produces_between() {
        let value = EncodedObjectId::from(100);
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::Lt,
            value,
        );

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(Between(EncodedObjectId::MIN, EncodedObjectId::from(99)))
        );
    }

    #[test]
    fn test_to_scan_predicate_leq_produces_between() {
        let value = EncodedObjectId::from(100);
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::LtEq,
            value,
        );

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(Between(EncodedObjectId::MIN, EncodedObjectId::from(100)))
        );
    }

    #[test]
    fn test_to_scan_predicate_between_expr_produces_between() {
        use crate::memory::object_id::EncodedObjectId;
        let from = EncodedObjectId::from(10);
        let to = EncodedObjectId::from(20);
        let expr = MemStoragePredicateExpr::Between(Arc::from("col"), from, to);

        assert_eq!(expr.to_scan_predicate().unwrap(), Some(Between(from, to)));
    }

    #[test]
    fn test_to_scan_predicate_gt_with_max_value_returns_false() {
        use crate::memory::object_id::EncodedObjectId;
        let max_id = EncodedObjectId::MAX;
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::Gt,
            max_id,
        );

        assert_eq!(expr.to_scan_predicate().unwrap(), Some(False));
    }

    #[test]
    fn test_to_scan_predicate_lt_with_min_value_returns_false() {
        use crate::memory::object_id::EncodedObjectId;
        let min_id = EncodedObjectId::MIN;
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::Lt,
            min_id,
        );

        assert_eq!(expr.to_scan_predicate().unwrap(), Some(False));
    }

    #[test]
    fn test_to_scan_predicate_column_is_err() {
        let expr = MemStoragePredicateExpr::Column(Arc::from("col"));
        let result = expr.to_scan_predicate();
        assert!(result.is_err());
    }

    #[test]
    fn test_to_scan_predicate_objectid_is_err() {
        use crate::memory::object_id::EncodedObjectId;
        let expr = MemStoragePredicateExpr::ObjectId(EncodedObjectId::from(1));
        let result = expr.to_scan_predicate();
        assert!(result.is_err());
    }

    fn column_expr(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, 0))
    }

    fn literal_binary(value: u32) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::FixedSizeBinary(
            4,
            Some(value.to_be_bytes().to_vec()),
        )))
    }

    fn literal_bool(value: bool) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Boolean(Some(value))))
    }
}
