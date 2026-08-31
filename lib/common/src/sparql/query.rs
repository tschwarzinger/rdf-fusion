use crate::sparql::TriplePattern;
use datafusion::logical_expr::LogicalPlan;

/// Defines the query variant of an [`RdfFusionQuery`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryVariant {
    Ask,
    Select,
    Construct { template: Vec<TriplePattern> },
    Describe { template: Vec<TriplePattern> },
}

/// Represents a parsed SPARQL Query that can be executed by RDF Fusion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RdfFusionQuery {
    plan: LogicalPlan,
    variant: QueryVariant,
}

impl RdfFusionQuery {
    /// Creates a new [`RdfFusionQuery`].
    pub fn new(plan: LogicalPlan, variant: QueryVariant) -> Self {
        Self { plan, variant }
    }

    /// Returns a reference to the inner [`LogicalPlan`].
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.plan
    }

    /// Returns the variant of this query.
    pub fn variant(&self) -> &QueryVariant {
        &self.variant
    }
}

impl std::fmt::Display for RdfFusionQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match &self.variant {
            QueryVariant::Ask => "Ask",
            QueryVariant::Select => "Select",
            QueryVariant::Construct { .. } => "Construct",
            QueryVariant::Describe { .. } => "Describe",
        };
        writeln!(f, "Variant: {name}")?;

        let template = match &self.variant {
            QueryVariant::Ask | QueryVariant::Select => None,
            QueryVariant::Construct { template }
            | QueryVariant::Describe { template } => Some(template),
        };
        if let Some(template) = template {
            writeln!(f, "Template:")?;
            for triple_pattern in template {
                writeln!(f, "  {triple_pattern}")?;
            }
        }

        write!(f, "Plan:\n{}", self.plan.display_indent())
    }
}
