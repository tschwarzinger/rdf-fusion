use crate::rdf_files::rdf::RdfFileScanOptions;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_common::RdfInputSource;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;

/// A logical node that parses an RDF file.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ParseRdfFileNode {
    pub source: RdfInputSource,
    pub options: RdfFileScanOptions,
    pub schema: DFSchemaRef,
}

impl ParseRdfFileNode {
    pub fn new(
        source: RdfInputSource,
        options: RdfFileScanOptions,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            source,
            options,
            schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for ParseRdfFileNode {
    fn name(&self) -> &str {
        "ParseRdfFile"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParseRdfFile: source={:?}, format={}",
            self.source, self.options.format
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(self.clone())
    }
}

impl PartialOrd for ParseRdfFileNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ParseRdfFileNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source
            .cmp(&other.source)
            .then_with(|| self.options.cmp(&other.options))
    }
}

impl Debug for ParseRdfFileNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParseRdfFile")
            .field("source", &self.source)
            .field("options", &self.options)
            .finish()
    }
}
