use datafusion::common::{Column, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use itertools::Itertools;
use rdf_fusion_common::DFResult;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

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
}

impl BgpNode {
    /// Creates a new [BgpNode].
    pub fn new(
        patterns: Vec<LogicalPlan>,
        schema: DFSchemaRef,
        filters: Vec<Expr>,
        projection: Option<Vec<Column>>,
    ) -> Self {
        Self {
            patterns,
            schema,
            filters,
            projection,
        }
    }
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

        if !self.filters.is_empty() {
            write!(f, "filters=[{}], ", self.filters.iter().format(", "))?;
        }

        if let Some(projection) = &self.projection {
            write!(f, "projection={projection:?}, ")?;
        }

        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Self::new(
            inputs,
            Arc::clone(&self.schema),
            exprs,
            self.projection.clone(),
        ))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active_graph::ActiveGraph;
    use crate::quad_pattern::QuadPatternNode;
    use rdf_fusion_common::{
        NamedNode, NamedNodePattern, TermPattern, TriplePattern, Variable,
    };
    use rdf_fusion_encoding::QuadStorageEncoding;
    use std::sync::Arc;

    #[test]
    fn test_bgp_node_schema_merge() -> DFResult<()> {
        let encoding = QuadStorageEncoding::PlainTerm;
        let p1 = QuadPatternNode::new(
            encoding.clone(),
            ActiveGraph::DefaultGraph,
            None,
            TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked("s")),
                predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                    "http://example.org/p1",
                )),
                object: TermPattern::Variable(Variable::new_unchecked("o1")),
            },
        );
        let p2 = QuadPatternNode::new(
            encoding.clone(),
            ActiveGraph::DefaultGraph,
            None,
            TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked("s")),
                predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                    "http://example.org/p2",
                )),
                object: TermPattern::Variable(Variable::new_unchecked("o2")),
            },
        );

        let lp1 = LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(p1),
        });
        let lp2 = LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(p2),
        });

        let mut schema = lp1.schema().as_ref().clone();
        schema.merge(lp2.schema());

        let bgp = BgpNode::new(vec![lp1, lp2], Arc::new(schema), vec![], None);

        assert_eq!(bgp.schema.fields().len(), 3);
        assert!(bgp.schema.field_with_unqualified_name("s").is_ok());
        assert!(bgp.schema.field_with_unqualified_name("o1").is_ok());
        assert!(bgp.schema.field_with_unqualified_name("o2").is_ok());

        Ok(())
    }
}
