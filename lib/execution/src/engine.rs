use crate::planner::RdfFusionPlanner;
use crate::results::QueryResults;
use crate::sparql::error::QueryEvaluationError;
use crate::sparql::{
    OptimizationLevel, QueryExplanation, QueryOptions, RdfFusionQuery, RdfFusionUpdate,
    UpdateOptions, create_optimizer_rules, create_pyhsical_optimizer_rules,
    evaluate_query,
};
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::functions_aggregate::first_last::FirstValue;
use datafusion::logical_expr::AggregateUDF;
use datafusion::prelude::{SessionConfig, SessionContext};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
use rdf_fusion_encoding::{QuadStorageEncoding, RdfFusionEncodings};
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::functions::{
    RdfFusionFunctionRegistry, RdfFusionFunctionRegistryRef,
};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
use rdf_fusion_logical::{ActiveGraph, RdfFusionLogicalPlanBuilderContext};
use rdf_fusion_model::{DFResult, NamedOrBlankNodeRef};
use rdf_fusion_model::{GraphName, GraphNameRef, NamedNodeRef, QuadRef, TermRef};
use std::sync::Arc;

/// Represents a connection to an instance of an RDF Fusion engine.
///
/// An RDF Fusion instance consists of:
/// - A [SessionContext]. This is the primary interaction point with DataFusion.
/// - An [RdfFusionFunctionRegistry] that holds the currently registered RDF Fusion built-ins.
/// - A reference to a quad storage.
#[derive(Clone)]
pub struct RdfFusionContext {
    /// The DataFusion [SessionContext].
    ctx: SessionContext,
    /// Holds references to the registered built-in functions.
    functions: RdfFusionFunctionRegistryRef,
    /// Encoding configurations
    encodings: RdfFusionEncodings,
    /// The storage that backs this instance.
    storage: Arc<dyn QuadStorage>,
}

impl RdfFusionContext {
    /// Creates a new [RdfFusionContext] with the default configuration and the given `storage`.
    pub fn new(
        config: SessionConfig,
        runtime_env: Arc<RuntimeEnv>,
        storage: Arc<dyn QuadStorage>,
    ) -> Self {
        let object_id_encoding = match storage.encoding() {
            QuadStorageEncoding::PlainTerm => None,
            QuadStorageEncoding::ObjectId(encoding) => Some(Arc::clone(&encoding)),
        };
        let encodings = RdfFusionEncodings::new(
            Arc::clone(&PLAIN_TERM_ENCODING),
            Arc::new(TypedFamilyEncoding::default()),
            object_id_encoding,
            Arc::clone(&SORTABLE_TERM_ENCODING),
        );

        let registry: Arc<dyn RdfFusionFunctionRegistry> =
            Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone()));

        let context_view = RdfFusionContextView::new(
            Arc::clone(&registry),
            encodings.clone(),
            storage.encoding(),
        );

        let optimizer_rules =
            create_optimizer_rules(context_view.clone(), OptimizationLevel::Full);
        let physical_optimizer_rules =
            create_pyhsical_optimizer_rules(OptimizationLevel::Full);

        let state = SessionStateBuilder::new()
            .with_query_planner(Arc::new(RdfFusionPlanner::new(
                context_view,
                Arc::clone(&storage),
            )))
            .with_aggregate_functions(vec![AggregateUDF::from(FirstValue::new()).into()])
            .with_optimizer_rules(optimizer_rules)
            .with_physical_optimizer_rules(physical_optimizer_rules)
            .with_runtime_env(runtime_env)
            .with_config(config)
            .build();

        let session_context = SessionContext::from(state);
        Self {
            ctx: session_context,
            functions: registry,
            encodings,
            storage,
        }
    }

    /// Creates a new [RdfFusionContextView] on this context. The resulting view should be passed
    /// around in the RDF Fusion ecosystem to access the current configuration without directly
    /// depending on the [RdfFusionContext].
    pub fn create_view(&self) -> RdfFusionContextView {
        RdfFusionContextView::new(
            Arc::clone(&self.functions),
            self.encodings.clone(),
            self.storage.encoding(),
        )
    }

    /// Provides a reference to the [SessionContext].
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Returns a reference to the used [RdfFusionFunctionRegistry].
    pub fn functions(&self) -> &RdfFusionFunctionRegistryRef {
        &self.functions
    }

    /// Returns a reference to the used [RdfFusionEncodings].
    pub fn encodings(&self) -> &RdfFusionEncodings {
        &self.encodings
    }

    /// Provides access to the [QuadStorage] of this instance for writing operations.
    pub fn storage(&self) -> &Arc<dyn QuadStorage> {
        &self.storage
    }

    //
    // Querying
    //

    /// Checks whether `quad` is contained in the instance.
    pub async fn contains(&self, quad: &QuadRef<'_>) -> DFResult<bool> {
        let active_graph_info = graph_name_to_active_graph(Some(quad.graph_name));
        let pattern_plan = self.plan_builder_context().create_matching_quads(
            active_graph_info,
            Some(quad.subject.into_owned()),
            Some(quad.predicate.into_owned()),
            Some(quad.object.into_owned()),
        );

        let count = DataFrame::new(self.ctx.state(), pattern_plan.build()?)
            .count()
            .await?;

        Ok(count > 0)
    }

    /// Used for obtaining a [RdfFusionLogicalPlanBuilderContext] for this RDF Fusion instance.
    fn plan_builder_context(&self) -> RdfFusionLogicalPlanBuilderContext {
        RdfFusionLogicalPlanBuilderContext::new(self.create_view())
    }

    /// Returns the number of quads in the instance.
    pub async fn len(&self) -> DFResult<usize> {
        self.storage
            .len()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))
    }

    /// Returns a stream of all quads that match the given pattern.
    pub async fn quads_for_pattern(
        &self,
        graph_name: Option<GraphNameRef<'_>>,
        subject: Option<NamedOrBlankNodeRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
    ) -> DFResult<SendableRecordBatchStream> {
        let active_graph_info = graph_name_to_active_graph(graph_name);
        let pattern_plan = self
            .plan_builder_context()
            .create_matching_quads(
                active_graph_info,
                subject.map(NamedOrBlankNodeRef::into_owned),
                predicate.map(NamedNodeRef::into_owned),
                object.map(TermRef::into_owned),
            )
            .with_plain_terms()?;

        let result = DataFrame::new(self.ctx.state(), pattern_plan.build()?)
            .execute_stream()
            .await?;
        Ok(result)
    }

    /// Evaluates a SPARQL [RdfFusionQuery] over the instance.
    pub async fn execute_query(
        &self,
        query: &RdfFusionQuery,
        options: QueryOptions,
    ) -> Result<(QueryResults, QueryExplanation), QueryEvaluationError> {
        Box::pin(evaluate_query(
            self,
            self.plan_builder_context(),
            query,
            options,
        ))
        .await
    }

    /// Evaluates a SPARQL [`Update`] over the instance.
    pub async fn execute_update(
        &self,
        query: &RdfFusionUpdate,
        _options: UpdateOptions,
    ) -> Result<(), QueryEvaluationError> {
        self.storage
            .execute_update(&query.inner)
            .await
            .expect("TODO");
        Ok(())
    }
}

fn graph_name_to_active_graph(graph_name: Option<GraphNameRef<'_>>) -> ActiveGraph {
    let Some(graph_name) = graph_name else {
        return ActiveGraph::AllGraphs;
    };

    match graph_name {
        GraphNameRef::NamedNode(nn) => {
            ActiveGraph::Union(vec![GraphName::NamedNode(nn.into_owned())])
        }
        GraphNameRef::BlankNode(bnode) => {
            ActiveGraph::Union(vec![GraphName::BlankNode(bnode.into_owned())])
        }
        GraphNameRef::DefaultGraph => ActiveGraph::DefaultGraph,
    }
}
