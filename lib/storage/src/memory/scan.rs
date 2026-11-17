//! Scanning the quads of a [`MemoryQuadStorageSnapshot`] that match the constants of a pattern.

use crate::memory::MemoryQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result as DFResult, Statistics};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::coop::make_cooperative;
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, SchedulingType,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream, StatisticsArgs,
};
use rdf_fusion_common::config::RdfFusionSessionConfigExt;
use rdf_fusion_common::{
    ActiveGraph, BlankNodeMatchingMode, GraphName, GraphNameRef, NamedNode,
    NamedNodePattern, NamedOrBlankNode, QuadPattern, QuadRef, Term, TermPattern, TermRef,
};
use rdf_fusion_encoding::plain_term::PlainTermArrayElementBuilder;
use rdf_fusion_encoding::{EncodingArray, QuadStorageEncoding};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// The constants of a [`QuadPattern`] that can be looked up in the indexes of a dataset.
#[derive(Debug, Clone, Default)]
pub(super) struct PatternConstants {
    pub subject: Option<NamedOrBlankNode>,
    pub predicate: Option<NamedNode>,
    pub object: Option<Term>,
    pub graph: Option<GraphName>,
}

impl PatternConstants {
    /// Extracts the constants of `pattern`. Returns [`None`] if the pattern cannot match any quad.
    pub fn try_new(pattern: &QuadPattern) -> Option<Self> {
        let mode = pattern.blank_node_mode;
        let subject = match constant(&pattern.triple_pattern.subject, mode) {
            None => None,
            Some(Term::NamedNode(node)) => Some(node.into()),
            Some(Term::BlankNode(node)) => Some(node.into()),
            Some(Term::Literal(_)) => return None, // A literal is never a subject.
        };
        let predicate = match &pattern.triple_pattern.predicate {
            NamedNodePattern::NamedNode(node) => Some(node.clone()),
            NamedNodePattern::Variable(_) => None,
        };
        let object = constant(&pattern.triple_pattern.object, mode);
        let graph = match &pattern.active_graph {
            ActiveGraph::DefaultGraph => Some(GraphName::DefaultGraph),
            ActiveGraph::Union(graphs) => match graphs.as_slice() {
                [] => return None,
                [graph] => Some(graph.clone()),
                _ => None,
            },
            ActiveGraph::AllGraphs | ActiveGraph::AnyNamedGraph => None,
        };
        Some(Self {
            subject,
            predicate,
            object,
            graph,
        })
    }
}

impl Display for PatternConstants {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let components = [
            ("graph", self.graph.as_ref().map(ToString::to_string)),
            ("subject", self.subject.as_ref().map(ToString::to_string)),
            (
                "predicate",
                self.predicate.as_ref().map(ToString::to_string),
            ),
            ("object", self.object.as_ref().map(ToString::to_string)),
        ];
        let components = components
            .into_iter()
            .filter_map(|(name, value)| Some(format!("{name}={}", value?)))
            .collect::<Vec<_>>();
        write!(f, "{}", components.join(", "))
    }
}

/// Returns the constant term of `pattern`, or [`None`] if it matches any term.
fn constant(pattern: &TermPattern, mode: BlankNodeMatchingMode) -> Option<Term> {
    match pattern {
        TermPattern::NamedNode(node) => Some(node.clone().into()),
        TermPattern::Literal(literal) => Some(literal.clone().into()),
        TermPattern::BlankNode(node) if mode == BlankNodeMatchingMode::Filter => {
            Some(node.clone().into())
        }
        TermPattern::BlankNode(_) | TermPattern::Variable(_) => None,
    }
}

/// A table of the quads of a snapshot that match the constants of a pattern.
///
/// Other parts of the pattern (e.g., repeated variables or an active graph with multiple graphs)
/// are not considered and must be filtered by the caller.
#[derive(Debug)]
pub(super) struct MemoryQuadTable {
    snapshot: MemoryQuadStorageSnapshot,
    /// [`None`] if the pattern cannot match any quad.
    constants: Option<PatternConstants>,
}

impl MemoryQuadTable {
    pub fn new(
        snapshot: MemoryQuadStorageSnapshot,
        constants: Option<PatternConstants>,
    ) -> Self {
        Self {
            snapshot,
            constants,
        }
    }
}

#[async_trait]
impl TableProvider for MemoryQuadTable {
    fn schema(&self) -> SchemaRef {
        quad_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..quad_schema().fields().len()).collect());
        // Scans below the small scan threshold must report their size, such that they can be
        // executed eagerly.
        let small_scan_threshold = state
            .config()
            .rdf_fusion_options_or_default()
            .execution
            .small_scan_threshold;
        let statistics_limit = small_scan_threshold.max(STATISTICS_SIZE_LIMIT);
        Ok(Arc::new(MemoryQuadScanExec::try_new(
            self.snapshot.clone(),
            self.constants.clone(),
            projection,
            limit,
            state.config().batch_size(),
            statistics_limit,
        )?))
    }
}

/// Scans the quads of a snapshot that match the constants of a pattern.
///
/// The quads are looked up in the indexes of the snapshot and converted to Arrow (in the plain
/// term encoding) during execution, one batch at a time. Only the projected columns are created.
#[derive(Debug)]
pub(super) struct MemoryQuadScanExec {
    snapshot: MemoryQuadStorageSnapshot,
    /// [`None`] if the pattern cannot match any quad.
    constants: Option<PatternConstants>,
    /// The indices of the quad columns (graph, subject, predicate, object) to produce.
    projection: Vec<usize>,
    /// The maximum number of quads to produce.
    limit: Option<usize>,
    batch_size: usize,
    schema: SchemaRef,
    statistics: Arc<Statistics>,
    properties: Arc<PlanProperties>,
}

impl MemoryQuadScanExec {
    fn try_new(
        snapshot: MemoryQuadStorageSnapshot,
        constants: Option<PatternConstants>,
        projection: Vec<usize>,
        limit: Option<usize>,
        batch_size: usize,
        statistics_limit: usize,
    ) -> DFResult<Self> {
        let schema = Arc::new(quad_schema().project(&projection)?);
        let statistics = Arc::new(compute_statistics(
            &snapshot,
            constants.as_ref(),
            &projection,
            limit,
            statistics_limit,
            &schema,
        ));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);
        Ok(Self {
            snapshot,
            constants,
            projection,
            limit,
            batch_size: batch_size.max(1),
            schema,
            statistics,
            properties: Arc::new(properties),
        })
    }
}

impl DisplayAs for MemoryQuadScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        let constants = match &self.constants {
            Some(constants) => constants.to_string(),
            None => "<no match>".to_owned(),
        };
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MemoryQuadScanExec: constants=[{constants}]")?;
                if let Some(limit) = self.limit {
                    write!(f, ", limit={limit}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => writeln!(f, "constants=[{constants}]"),
        }
    }
}

impl ExecutionPlan for MemoryQuadScanExec {
    fn name(&self) -> &str {
        "MemoryQuadScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let snapshot = self.snapshot.clone();
        let constants = self.constants.clone();
        let projection = self.projection.clone();
        let limit = self.limit.unwrap_or(usize::MAX);
        let batch_size = self.batch_size;
        let schema = Arc::clone(&self.schema);

        // The stream owns the snapshot, so the index lookup can borrow it across batches.
        let stream = async_stream::stream! {
            let Some(constants) = constants else {
                return;
            };
            let mut builder =
                ProjectedQuadsBuilder::new(&projection, batch_size, Arc::clone(&schema));
            for quad in snapshot.quads_for_pattern(&constants).take(limit) {
                builder.append(quad);
                if builder.is_full() {
                    yield builder.finish();
                }
            }
            if !builder.is_empty() {
                yield builder.finish();
            }
        };

        let stream = RecordBatchStreamAdapter::new(Arc::clone(&self.schema), stream);
        Ok(make_cooperative(Box::pin(stream)))
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> DFResult<Arc<Statistics>> {
        Ok(Arc::clone(&self.statistics))
    }
}

/// Builds record batches with the projected quad columns.
struct ProjectedQuadsBuilder<'p> {
    projection: &'p [usize],
    builders: Vec<PlainTermArrayElementBuilder>,
    len: usize,
    capacity: usize,
    schema: SchemaRef,
}

impl<'p> ProjectedQuadsBuilder<'p> {
    fn new(projection: &'p [usize], capacity: usize, schema: SchemaRef) -> Self {
        Self {
            projection,
            builders: Self::new_builders(projection.len(), capacity),
            len: 0,
            capacity,
            schema,
        }
    }

    fn new_builders(count: usize, capacity: usize) -> Vec<PlainTermArrayElementBuilder> {
        (0..count)
            .map(|_| PlainTermArrayElementBuilder::with_capacity(capacity))
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    fn append(&mut self, quad: QuadRef<'_>) {
        for (column, builder) in self.projection.iter().zip(&mut self.builders) {
            match *column {
                0 => builder.append_graph_name(quad.graph_name),
                1 => builder.append_named_or_blank_node(quad.subject),
                2 => builder.append_named_node(quad.predicate),
                _ => builder.append_term(quad.object),
            }
        }
        self.len += 1;
    }

    /// Returns the batch of the appended quads and resets the builder.
    fn finish(&mut self) -> DFResult<RecordBatch> {
        let builders = std::mem::replace(
            &mut self.builders,
            Self::new_builders(self.projection.len(), self.capacity),
        );
        let columns = builders
            .into_iter()
            .map(|builder| builder.finish().into_array_ref())
            .collect::<Vec<ArrayRef>>();
        // The row count is required if no column is projected (e.g., for `COUNT(*)`).
        let options = RecordBatchOptions::new().with_row_count(Some(self.len));
        self.len = 0;
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &options,
        )?)
    }
}

/// The schema of the quads (graph, subject, predicate, object) in the plain term encoding.
fn quad_schema() -> SchemaRef {
    Arc::clone(QuadStorageEncoding::PlainTerm.quad_schema().inner())
}

/// The estimated size (in bytes) above which a scan reports no statistics, unless the small scan
/// threshold is larger.
const STATISTICS_SIZE_LIMIT: usize = 1024 * 1024;

/// Estimates the statistics of a scan by counting the matching quads (without converting them).
///
/// Counting stops once the estimated size of the matching quads exceeds `statistics_limit`. Such
/// large scans report no statistics. Hence, they are never executed eagerly during planning, and
/// the BGP planner joins them after the scans with statistics, in the order of the BGP.
///
/// Non-empty scans report inexact statistics. Otherwise, DataFusion's `AggregateStatistics` rule
/// replaces aggregates (e.g., `COUNT`) with plain literals that are not RDF terms.
fn compute_statistics(
    snapshot: &MemoryQuadStorageSnapshot,
    constants: Option<&PatternConstants>,
    projection: &[usize],
    limit: Option<usize>,
    statistics_limit: usize,
    schema: &SchemaRef,
) -> Statistics {
    let unknown = Statistics::new_unknown(schema);
    let empty = || {
        Statistics::new_unknown(schema)
            .with_num_rows(Precision::Exact(0))
            .with_total_byte_size(Precision::Exact(0))
    };
    let Some(constants) = constants else {
        return empty();
    };

    let mut num_rows = 0;
    // The size of the projected columns, which is reported.
    let mut num_bytes = 0;
    // The size of all columns, which limits the counting. Also limits scans without columns
    // (e.g., for `COUNT(*)`).
    let mut num_quad_bytes = 0;
    let quads = snapshot
        .quads_for_pattern(constants)
        .take(limit.unwrap_or(usize::MAX));
    for quad in quads {
        let sizes = quad_sizes(quad);
        num_rows += 1;
        num_bytes += projection
            .iter()
            .map(|column| sizes[*column])
            .sum::<usize>();
        num_quad_bytes += sizes.iter().sum::<usize>();
        if num_quad_bytes > statistics_limit {
            return unknown;
        }
    }

    if num_rows == 0 {
        empty()
    } else {
        unknown
            .with_num_rows(Precision::Inexact(num_rows))
            .with_total_byte_size(Precision::Inexact(num_bytes))
    }
}

/// Estimates the sizes of the components (graph, subject, predicate, object) of `quad` in the plain
/// term encoding.
fn quad_sizes(quad: QuadRef<'_>) -> [usize; 4] {
    let graph = match quad.graph_name {
        GraphNameRef::NamedNode(node) => estimated_size(node.into()),
        GraphNameRef::BlankNode(node) => estimated_size(node.into()),
        GraphNameRef::DefaultGraph => TERM_OVERHEAD,
    };
    [
        graph,
        estimated_size(quad.subject.into()),
        estimated_size(quad.predicate.into()),
        estimated_size(quad.object),
    ]
}

/// The estimated size of a term in the plain term encoding, in addition to its strings (term type,
/// offsets, and validity).
const TERM_OVERHEAD: usize = 16;

/// Estimates the size of `term` in the plain term encoding.
fn estimated_size(term: TermRef<'_>) -> usize {
    let strings = match term {
        TermRef::NamedNode(node) => node.as_str().len(),
        TermRef::BlankNode(node) => node.as_str().len(),
        TermRef::Literal(literal) => {
            literal.value().len()
                + literal.language().map_or(0, str::len)
                + literal.datatype().as_str().len()
        }
    };
    strings + TERM_OVERHEAD
}
