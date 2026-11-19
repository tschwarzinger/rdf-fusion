//! Scanning the quads of a [`MemoryQuadStorageSnapshot`] that match a [`QuadPattern`].

use crate::memory::MemoryQuadStorageSnapshot;
use datafusion::arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result as DFResult, Statistics};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::coop::make_cooperative;
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, SchedulingType,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream, StatisticsArgs,
};
use rdf_fusion_common::{
    ActiveGraph, BlankNodeMatchingMode, GraphName, NamedNode, NamedNodePattern,
    NamedOrBlankNode, QuadComponent, QuadPattern, QuadRef, Term, TermPattern,
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
    /// Returns the lookups that together find the quads that match the constants of `pattern`.
    ///
    /// Returns one lookup per distinct graph of an [`ActiveGraph::Union`], and no lookup if the
    /// pattern cannot match any quad.
    fn lookups(pattern: &QuadPattern) -> Vec<Self> {
        let mode = pattern.blank_node_mode;
        let subject = match constant(&pattern.triple_pattern.subject, mode) {
            None => None,
            Some(Term::NamedNode(node)) => Some(node.into()),
            Some(Term::BlankNode(node)) => Some(node.into()),
            Some(Term::Literal(_)) => return Vec::new(), // A literal is never a subject.
        };
        let predicate = match &pattern.triple_pattern.predicate {
            NamedNodePattern::NamedNode(node) => Some(node.clone()),
            NamedNodePattern::Variable(_) => None,
        };
        let object = constant(&pattern.triple_pattern.object, mode);

        let graphs = match &pattern.active_graph {
            ActiveGraph::DefaultGraph => vec![Some(GraphName::DefaultGraph)],
            ActiveGraph::Union(graphs) => {
                let mut distinct = Vec::with_capacity(graphs.len());
                for graph in graphs {
                    if !distinct.contains(graph) {
                        distinct.push(graph.clone());
                    }
                }
                distinct.into_iter().map(Some).collect()
            }
            ActiveGraph::AllGraphs | ActiveGraph::AnyNamedGraph => vec![None],
        };
        graphs
            .into_iter()
            .map(|graph| Self {
                subject: subject.clone(),
                predicate: predicate.clone(),
                object: object.clone(),
                graph,
            })
            .collect()
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
        write!(f, "[{}]", components.join(", "))
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

/// How a [`MemoryQuadScanExec`] finds the quads that match the constants and the active graph of a
/// [`QuadPattern`] and which columns it produces for them.
///
/// Repeated variables are not checked by the scan (see [`plan_quad_pattern_scan`]).
#[derive(Debug, Clone)]
pub(super) struct MemoryQuadScan {
    /// The index lookups. The scan matches no quad if there is none.
    lookups: Vec<PatternConstants>,
    /// Whether quads in the default graph are skipped (for [`ActiveGraph::AnyNamedGraph`]).
    named_graphs_only: bool,
    /// The produced columns: the component and the name of the column.
    columns: Vec<(QuadComponent, String)>,
    /// The estimated number of matching quads (see [`estimate_num_rows`]).
    estimated_rows: usize,
}

impl MemoryQuadScan {
    /// Creates a scan for the constants and the active graph of `pattern` that produces `columns`.
    fn new(pattern: &QuadPattern, columns: Vec<(QuadComponent, String)>) -> Self {
        Self {
            lookups: PatternConstants::lookups(pattern),
            named_graphs_only: pattern.active_graph == ActiveGraph::AnyNamedGraph,
            columns,
            estimated_rows: estimate_num_rows(pattern),
        }
    }

    /// Returns the schema of the produced columns.
    fn schema(&self) -> SchemaRef {
        let quad_schema = quad_schema();
        let fields = self
            .columns
            .iter()
            .map(|(component, name)| {
                quad_schema
                    .field(component.gspo_quad_table())
                    .clone()
                    .with_name(name)
            })
            .collect::<Vec<_>>();
        Arc::new(Schema::new_with_metadata(
            fields,
            quad_schema.metadata().clone(),
        ))
    }
}

impl Display for MemoryQuadScan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.lookups.is_empty() {
            write!(f, "constants=<no match>")?;
        } else {
            let lookups = self
                .lookups
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            write!(f, "constants={}", lookups.join(", "))?;
        }
        if self.named_graphs_only {
            write!(f, ", named_graphs_only")?;
        }
        let columns = self
            .columns
            .iter()
            .map(|(component, name)| format!("{} as {name}", component.column_name()))
            .collect::<Vec<_>>();
        write!(f, ", columns=[{}]", columns.join(", "))
    }
}

/// Plans the scan of the quads of `snapshot` that match `pattern`, producing the columns of
/// [`QuadPattern::compute_projected_components`], projected to `projection`.
///
/// Patterns with repeated variables (e.g., `?x <p> ?x`) are rare. Hence, they are not checked by
/// the [`MemoryQuadScanExec`]. Instead, the scan produces all components, which are filtered by a
/// [`FilterExec`] and projected by a [`ProjectionExec`].
pub(super) fn plan_quad_pattern_scan(
    snapshot: MemoryQuadStorageSnapshot,
    pattern: &QuadPattern,
    projection: Option<Vec<usize>>,
    batch_size: usize,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let columns = pattern.compute_projected_components();
    let columns = match projection {
        Some(projection) => projection
            .into_iter()
            .map(|idx| columns[idx].clone())
            .collect(),
        None => columns,
    };

    let equalities = repeated_variables(pattern);
    if equalities.is_empty() {
        let scan = MemoryQuadScan::new(pattern, columns);
        return Ok(Arc::new(MemoryQuadScanExec::new(
            snapshot, scan, batch_size,
        )));
    }

    let all_components = QuadComponent::all()
        .into_iter()
        .map(|component| (component, component.column_name().to_owned()))
        .collect();
    let scan = MemoryQuadScan::new(pattern, all_components);
    let scan = Arc::new(MemoryQuadScanExec::new(snapshot, scan, batch_size));

    let column = |component: QuadComponent| -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(
            component.column_name(),
            component.gspo_quad_table(),
        ))
    };
    // The default graph is null and, hence, never equal to a term.
    let predicate = equalities
        .into_iter()
        .map(|(lhs, rhs)| {
            Arc::new(BinaryExpr::new(column(lhs), Operator::Eq, column(rhs)))
                as Arc<dyn PhysicalExpr>
        })
        .reduce(|lhs, rhs| Arc::new(BinaryExpr::new(lhs, Operator::And, rhs)))
        .expect("There is at least one equality");
    let filter = Arc::new(FilterExec::try_new(predicate, scan)?);

    let projection = columns
        .into_iter()
        .map(|(component, name)| ProjectionExpr::new(column(component), name))
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(projection, filter)?))
}

/// Returns the pairs of components of `pattern` that are bound to the same variable.
fn repeated_variables(pattern: &QuadPattern) -> Vec<(QuadComponent, QuadComponent)> {
    let patterns = [
        pattern
            .graph_variable
            .as_ref()
            .map(|v| TermPattern::Variable(v.clone())),
        Some(pattern.triple_pattern.subject.clone()),
        Some(pattern.triple_pattern.predicate.clone().into()),
        Some(pattern.triple_pattern.object.clone()),
    ];

    let mut first_occurrences: Vec<(&str, QuadComponent)> = Vec::new();
    let mut equalities = Vec::new();
    for (pattern_part, component) in patterns.iter().zip(QuadComponent::all()) {
        let name = match pattern_part {
            Some(TermPattern::Variable(variable)) => variable.as_str(),
            Some(TermPattern::BlankNode(node))
                if pattern.blank_node_mode == BlankNodeMatchingMode::Variable =>
            {
                node.as_str()
            }
            _ => continue,
        };
        match first_occurrences.iter().find(|(other, _)| *other == name) {
            Some((_, first)) => equalities.push((*first, component)),
            None => first_occurrences.push((name, component)),
        }
    }
    equalities
}

/// Returns the quads of `snapshot` that match `scan`.
fn matching_quads<'a>(
    snapshot: &'a MemoryQuadStorageSnapshot,
    scan: &'a MemoryQuadScan,
) -> impl Iterator<Item = QuadRef<'a>> + Send + 'a {
    scan.lookups
        .iter()
        .flat_map(|constants| snapshot.quads_for_pattern(constants))
        .filter(|quad| !scan.named_graphs_only || !quad.graph_name.is_default_graph())
}

/// Scans the quads of a snapshot that match a [`QuadPattern`] (see [`MemoryQuadScan`]).
///
/// The quads are looked up in the indexes of the snapshot and converted to Arrow (in the plain
/// term encoding) during execution, one batch at a time. Only the columns of the pattern's
/// variables are created.
#[derive(Debug)]
pub(super) struct MemoryQuadScanExec {
    snapshot: MemoryQuadStorageSnapshot,
    scan: MemoryQuadScan,
    batch_size: usize,
    schema: SchemaRef,
    statistics: Arc<Statistics>,
    properties: Arc<PlanProperties>,
}

impl MemoryQuadScanExec {
    pub fn new(
        snapshot: MemoryQuadStorageSnapshot,
        scan: MemoryQuadScan,
        batch_size: usize,
    ) -> Self {
        let schema = scan.schema();
        let statistics = Arc::new(scan.statistics(&schema));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);
        Self {
            snapshot,
            scan,
            batch_size: batch_size.max(1),
            schema,
            statistics,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for MemoryQuadScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MemoryQuadScanExec: {}", self.scan)
            }
            DisplayFormatType::TreeRender => writeln!(f, "{}", self.scan),
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
        let scan = self.scan.clone();
        let batch_size = self.batch_size;
        let schema = Arc::clone(&self.schema);

        // The stream owns the snapshot, so the index lookup can borrow it across batches.
        let stream = async_stream::stream! {
            let components = scan
                .columns
                .iter()
                .map(|(component, _)| *component)
                .collect::<Vec<_>>();
            let mut builder =
                ProjectedQuadsBuilder::new(&components, batch_size, Arc::clone(&schema));
            for quad in matching_quads(&snapshot, &scan) {
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

/// Builds record batches with the given quad components as columns.
struct ProjectedQuadsBuilder<'p> {
    components: &'p [QuadComponent],
    builders: Vec<PlainTermArrayElementBuilder>,
    len: usize,
    capacity: usize,
    schema: SchemaRef,
}

impl<'p> ProjectedQuadsBuilder<'p> {
    fn new(components: &'p [QuadComponent], capacity: usize, schema: SchemaRef) -> Self {
        Self {
            components,
            builders: Self::new_builders(components.len(), capacity),
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
        for (component, builder) in self.components.iter().zip(&mut self.builders) {
            match component {
                QuadComponent::GraphName => builder.append_graph_name(quad.graph_name),
                QuadComponent::Subject => {
                    builder.append_named_or_blank_node(quad.subject)
                }
                QuadComponent::Predicate => builder.append_named_node(quad.predicate),
                QuadComponent::Object => builder.append_term(quad.object),
            }
        }
        self.len += 1;
    }

    /// Returns the batch of the appended quads and resets the builder.
    fn finish(&mut self) -> DFResult<RecordBatch> {
        let builders = std::mem::replace(
            &mut self.builders,
            Self::new_builders(self.components.len(), self.capacity),
        );
        let columns = builders
            .into_iter()
            .map(|builder| builder.finish().into_array_ref())
            .collect::<Vec<ArrayRef>>();
        // The row count is required if no column is produced (e.g., for `COUNT(*)`).
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

/// The estimated size of a term in the plain term encoding.
const ESTIMATED_TERM_SIZE: usize = 64;

impl MemoryQuadScan {
    /// Returns static statistics that are derived from the pattern without looking at the quads.
    ///
    /// Non-empty scans report inexact statistics. Otherwise, DataFusion's `AggregateStatistics`
    /// rule replaces aggregates (e.g., `COUNT`) with plain literals that are not RDF terms.
    fn statistics(&self, schema: &SchemaRef) -> Statistics {
        let unknown = Statistics::new_unknown(schema);
        if self.lookups.is_empty() {
            return unknown
                .with_num_rows(Precision::Exact(0))
                .with_total_byte_size(Precision::Exact(0));
        }
        let num_rows = self.estimated_rows.saturating_mul(self.lookups.len());
        let num_bytes = num_rows
            .saturating_mul(self.columns.len().max(1))
            .saturating_mul(ESTIMATED_TERM_SIZE);
        unknown
            .with_num_rows(Precision::Inexact(num_rows))
            .with_total_byte_size(Precision::Inexact(num_bytes))
    }
}

/// Estimates the number of quads that match `pattern` (per graph lookup) based on which components
/// are bound. Uses the same heuristic as Oxigraph's query optimizer (`sparopt`).
fn estimate_num_rows(pattern: &QuadPattern) -> usize {
    let bound = pattern.bound_components();
    let subject = bound.contains(&QuadComponent::Subject);
    let predicate = bound.contains(&QuadComponent::Predicate);
    let object = bound.contains(&QuadComponent::Object);
    match (subject, predicate, object) {
        (true, true, true) => 1,
        (true, true, false) => 10,
        (true, false, true) => 2,
        (false, true, true) => 1_000,
        (true, false, false) => 100,
        (false, false, false) => 1_000_000_000,
        (false, true, false) => 1_000_000,
        (false, false, true) => 10_000,
    }
}
