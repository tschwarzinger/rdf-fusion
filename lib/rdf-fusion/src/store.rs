//! API to access an [RDF dataset](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-dataset).
//!
//! The entry point of the module is the [`Store`] struct.
//!
//! Usage example:
//! ```
//! use rdf_fusion::model::*;
//! use rdf_fusion::store::Store;
//! use rdf_fusion::execution::results::QueryResults;
//! use futures::StreamExt;
//!
//! # tokio_test::block_on(async {
//! let store = Store::new_in_memory().await;
//!
//! // insertion
//! let ex = NamedNode::new("http://example.com")?;
//! let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
//! store.insert(&quad).await?;
//!
//! // quad filter
//! let results = store.quads_for_pattern(None, None, None, None).await?
//!     .try_collect_to_vec().await?;
//! assert_eq!(vec![quad], results);
//!
//! // SPARQL query
//! if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }").await? {
//!     assert_eq!(solutions.next().await.unwrap()?.get("s"), Some(&ex.into()));
//! };
//! # Result::<_, Box<dyn std::error::Error>>::Ok(())
//! # }).unwrap();
//! ```

use crate::error::{LoaderError, SerializerError};
use datafusion::logical_expr::col;
use futures::StreamExt;
use oxrdfio::RdfSerializer;
use rdf_fusion_encoding::quads_to_plain_term_dataframe;
use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
use rdf_fusion_execution::ingest::{RdfParserOptions, RdfParserTableProvider};
use rdf_fusion_execution::results::{QuadStream, QueryResults, QuerySolutionStream};
use rdf_fusion_execution::sparql::error::QueryEvaluationError;
use rdf_fusion_execution::sparql::{
    QueryExplanation, QueryOptions, RdfFusionQuery, RdfFusionUpdate, UpdateOptions,
};
use rdf_fusion_execution::{RdfFusionContext, RdfFusionContextBuilder};
use rdf_fusion_model::StorageError;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::{
    GraphNameRef, NamedNodeRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef,
    TermRef, Variable,
};
use rdf_fusion_storage::delta::DeltaQuadStorageBuilder;
use std::sync::{Arc, LazyLock};
use tokio::io::AsyncRead;

static QUAD_VARIABLES: LazyLock<Arc<[Variable]>> = LazyLock::new(|| {
    Arc::new([
        Variable::new_unchecked("graph"),
        Variable::new_unchecked("subject"),
        Variable::new_unchecked("predicate"),
        Variable::new_unchecked("object"),
    ])
});

/// An [RDF dataset](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-dataset) store.
///
/// The store can be updated and queried using [SPARQL](https://www.w3.org/TR/sparql11-query).
///
/// Usage example:
/// ```
/// use rdf_fusion::model::*;
/// use rdf_fusion::execution::results::QueryResults;
/// use rdf_fusion::store::Store;
/// use futures::StreamExt;
///
/// # tokio_test::block_on(async {
/// let store = Store::new_in_memory().await;
///
/// // insertion
/// let ex = NamedNode::new("http://example.com")?;
/// let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
/// store.insert(&quad).await?;
///
/// // quad filter
/// let results = store.quads_for_pattern(None, None, None, None).await?.try_collect_to_vec().await?;
/// assert_eq!(vec![quad], results);
///
/// // SPARQL query
/// if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }").await? {
///     assert_eq!(solutions.next().await.unwrap()?.get("s"), Some(&ex.into()));
/// };
///
/// Result::<_, Box<dyn std::error::Error>>::Ok(())
/// # }).unwrap();
/// ```
#[derive(Clone)]
pub struct Store {
    context: RdfFusionContext,
}

impl Store {
    /// Creates a [Store] with the given [RdfFusionContext].
    pub fn new(context: RdfFusionContext) -> Store {
        Self { context }
    }

    /// Creates a [`Store`] with an in-memory storage.
    ///
    /// For more control over the query engine and the storage backend, see [`Self::new`] and
    /// [`RdfFusionContextBuilder`] and the implementation of the used quad storage (e.g.
    /// [`DeltaQuadStorageBuilder`]).
    pub async fn new_in_memory() -> Store {
        let delta_storage =
            DeltaQuadStorageBuilder::new(Arc::new(TypedFamilyEncoding::default()))
                .with_location("memory://")
                .build()
                .await
                .expect("Default in-memory works");

        let context = RdfFusionContextBuilder::new(delta_storage)
            .with_single_partition_session_config()
            .build()
            .expect("Default in-memory works. Session config is set.");
        Self::new(context)
    }

    /// Returns a reference to the underlying [RdfFusionContext].
    pub fn context(&self) -> &RdfFusionContext {
        &self.context
    }

    /// Executes a [SPARQL](https://www.w3.org/TR/sparql11-query/) query.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::execution::results::QueryResults;
    /// use rdf_fusion::store::Store;
    /// use futures::StreamExt;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new_in_memory().await;
    ///
    /// // insertions
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// store.insert(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph)).await?;
    ///
    /// // SPARQL query
    /// if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }").await? {
    ///     assert_eq!(
    ///         solutions.next().await.unwrap()?.get("s"),
    ///         Some(&ex.into_owned().into())
    ///     );
    /// }
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn query(
        &self,
        query: impl TryInto<
            RdfFusionQuery,
            Error = impl Into<QueryEvaluationError> + std::fmt::Debug,
        >,
    ) -> Result<QueryResults, QueryEvaluationError> {
        self.query_opt(query, QueryOptions::default()).await
    }

    /// Executes a [SPARQL 1.1 query](https://www.w3.org/TR/sparql11-query/) with some options.
    ///
    /// Usage example with a custom function serializing terms to N-Triples:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::execution::results::QueryResults;
    /// use rdf_fusion::execution::sparql::QueryOptions;
    /// use rdf_fusion::store::Store;
    /// use futures::StreamExt;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new_in_memory().await;
    /// if let QueryResults::Solutions(mut solutions) = store.query_opt(
    ///     "SELECT (STR(1) AS ?nt) WHERE {}",
    ///     QueryOptions::default(),
    /// ).await? {
    ///     assert_eq!(
    ///         solutions.next().await.unwrap()?.get("nt"),
    ///         Some(&Literal::from("1").into())
    ///     );
    /// }
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn query_opt(
        &self,
        query: impl TryInto<
            RdfFusionQuery,
            Error = impl Into<QueryEvaluationError> + std::fmt::Debug,
        >,
        options: QueryOptions,
    ) -> Result<QueryResults, QueryEvaluationError> {
        self.explain_query_opt(query, options).await.map(|(r, _)| r)
    }

    /// Executes a [SPARQL 1.1 query](https://www.w3.org/TR/sparql11-query/) with some options and
    /// returns a query explanation with some statistics (if enabled with the `with_stats` parameter).
    ///
    /// <div class="warning">If you want to compute statistics you need to exhaust the results iterator before having a look at them.</div>
    ///
    /// Usage example serialising the explanation with statistics in JSON:
    /// ```
    /// use rdf_fusion::store::Store;
    /// use rdf_fusion::execution::sparql::QueryOptions;
    /// use rdf_fusion::execution::results::QueryResults;
    /// use futures::StreamExt;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new_in_memory().await;
    /// if let (QueryResults::Solutions(mut solutions), _explanation) = store.explain_query_opt(
    ///     "SELECT ?s WHERE { VALUES ?s { 1 2 3 } }",
    ///     QueryOptions::default(),
    /// ).await? {
    ///     // We make sure to have read all the solutions
    ///     while let Some(_) = solutions.next().await { }
    ///     // TODO
    ///     // let mut buf = Vec::new();
    ///     // explanation.write_in_json(&mut buf)?;
    /// }
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn explain_query_opt(
        &self,
        query: impl TryInto<
            RdfFusionQuery,
            Error = impl Into<QueryEvaluationError> + std::fmt::Debug,
        >,
        options: QueryOptions,
    ) -> Result<(QueryResults, QueryExplanation), QueryEvaluationError> {
        let query = query.try_into();
        match query {
            Ok(query) => self.context.execute_query(&query, options).await,
            Err(err) => Err(err.into()),
        }
    }

    /// Retrieves quads with a filter on each quad component
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new_in_memory().await;
    ///
    /// // insertion
    /// let ex = NamedNode::new("http://example.com")?;
    /// let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
    /// store.insert(&quad).await?;
    ///
    /// // quad filter by object
    /// let results = store
    ///     .quads_for_pattern(None, None, Some((&ex).into()), None).await?
    ///     .try_collect_to_vec().await?;
    /// assert_eq!(vec![quad], results);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn quads_for_pattern(
        &self,
        subject: Option<NamedOrBlankNodeRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
        graph_name: Option<GraphNameRef<'_>>,
    ) -> Result<QuadStream, QueryEvaluationError> {
        let record_batch_stream = self
            .context
            .quads_for_pattern(graph_name, subject, predicate, object)
            .await?;
        let solution_stream =
            QuerySolutionStream::try_new(QUAD_VARIABLES.clone(), record_batch_stream)?;
        QuadStream::try_new(solution_stream).map_err(QueryEvaluationError::InternalError)
    }

    /// Returns all the quads contained in the store.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new_in_memory().await;
    ///
    /// // insertion
    /// let ex = NamedNode::new("http://example.com")?;
    /// let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
    /// store.insert(&quad).await?;
    ///
    /// // quad filter by object
    /// let results = store.stream().await?.try_collect_to_vec().await?;
    /// assert_eq!(vec![quad], results);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn stream(&self) -> Result<QuadStream, QueryEvaluationError> {
        self.quads_for_pattern(None, None, None, None).await
    }

    /// Checks if this store contains a given quad.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, ex);
    ///
    /// let store = Store::new_in_memory().await;
    /// assert!(!store.contains(quad).await?);
    ///
    /// store.insert(quad).await?;
    /// assert!(store.contains(quad).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn contains<'a>(
        &self,
        quad: impl Into<QuadRef<'a>>,
    ) -> Result<bool, QueryEvaluationError> {
        let quad = quad.into();
        self.context
            .contains(&quad)
            .await
            .map_err(QueryEvaluationError::from)
    }

    /// Returns the number of quads in the store.
    ///
    /// <div class="warning">This function executes a full scan.</div>
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let store = Store::new_in_memory().await;
    /// store.insert(QuadRef::new(ex, ex, ex, ex)).await?;
    /// store.insert(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph)).await?;
    /// assert_eq!(2, store.len().await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn len(&self) -> Result<usize, QueryEvaluationError> {
        self.context.len().await.map_err(QueryEvaluationError::from)
    }

    /// Returns if the store is empty.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new_in_memory().await;
    /// assert!(store.is_empty().await?);
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// store.insert(QuadRef::new(ex, ex, ex, ex)).await?;
    /// assert!(!store.is_empty().await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn is_empty(&self) -> Result<bool, QueryEvaluationError> {
        Ok(self.len().await? == 0)
    }

    /// Executes a [SPARQL 1.1 update](https://www.w3.org/TR/sparql11-update/).
    ///
    /// Usage example:
    /// ```
    /// // use rdf-fusion::model::*;
    /// // use rdf-fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// // TODO #7: Implement Update
    /// // let store = Store::new_in_memory().await;
    /// // insertion
    /// // store
    /// //    .update("INSERT DATA { <http://example.com> <http://example.com> <http://example.com> }").await?;
    ///
    /// // we inspect the store contents
    /// // let ex = NamedNodeRef::new("http://example.com")?;
    /// // assert!(store.contains(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph)).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn update(
        &self,
        update: impl TryInto<RdfFusionUpdate, Error = impl Into<QueryEvaluationError>>,
    ) -> Result<(), QueryEvaluationError> {
        self.update_opt(update, UpdateOptions).await
    }

    /// Executes a [SPARQL 1.1 update](https://www.w3.org/TR/sparql11-update/) with some options.
    ///
    /// ```
    /// // use rdf-fusion::store::Store;
    /// // use rdf-fusion::sparql::QueryOptions;
    ///
    /// # tokio_test::block_on(async {
    /// // TODO #7: Implement Update
    /// // let store = Store::new_in_memory().await;
    /// // store.update_opt(
    /// //    "INSERT { ?s <http://example.com/n-triples-representation> ?n } WHERE { ?s ?p ?o BIND(<http://www.w3.org/ns/formats/N-Triples>(?s) AS ?nt) }",
    /// //    QueryOptions::default()
    /// //).await?;
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn update_opt(
        &self,
        update: impl TryInto<RdfFusionUpdate, Error = impl Into<QueryEvaluationError>>,
        options: impl Into<UpdateOptions>,
    ) -> Result<(), QueryEvaluationError> {
        let query = update.try_into();
        match query {
            Ok(query) => self.context.execute_update(&query, options.into()).await,
            Err(err) => Err(err.into()),
        }
    }

    /// Loads a RDF file under into the store.
    ///
    /// This function is atomic, quite slow and memory hungry.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::store::Store;
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::io::RdfFormat;
    /// use rdf_fusion_execution::ingest::RdfParserOptions;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new_in_memory().await;
    ///
    /// // insert a dataset file (former load_dataset method)
    /// let file = b"<http://example.com> <http://example.com> <http://example.com> <http://example.com/g> .";
    /// store.load_from_reader(file.as_ref(), RdfParserOptions::with_format(RdfFormat::NQuads)).await?;
    ///
    /// // insert a graph file (former load_graph method)
    /// let file = b"<> <> <> .";
    /// store.load_from_reader(
    ///     file.as_ref(),
    ///     RdfParserOptions::with_format(RdfFormat::Turtle)
    ///         .with_base_iri("http://example.com".to_owned())?
    ///         .without_named_graphs() // No named graphs allowed in the input
    ///         .with_default_graph(NamedNodeRef::new("http://example.com/g2")?), // we put the file default graph inside of a named graph
    /// ).await?;
    ///
    /// // we inspect the store contents
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// assert!(store.contains(QuadRef::new(ex, ex, ex, NamedNodeRef::new("http://example.com/g")?)).await?);
    /// assert!(store.contains(QuadRef::new(ex, ex, ex, NamedNodeRef::new("http://example.com/g2")?)).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn load_from_reader(
        &self,
        reader: impl AsyncRead + Unpin + Send + 'static,
        options: RdfParserOptions,
    ) -> Result<(), LoaderError> {
        let iri = options.base_iri.clone();
        let table_provider =
            RdfParserTableProvider::new(reader, options.with_rename_blank_nodes(true))
                .map_err(|e| LoaderError::InvalidBaseIri {
                    iri: iri
                        .map(|i| i.to_string())
                        .expect("Iri Parser Errors requires base iri"),
                    error: e,
                })?;
        let quads = self
            .context
            .session_context()
            .read_table(Arc::new(table_provider))
            .expect("TODo")
            .select([
                col(COL_GRAPH).alias(COL_GRAPH),
                col(COL_SUBJECT).alias(COL_SUBJECT),
                col(COL_PREDICATE).alias(COL_PREDICATE),
                col(COL_OBJECT).alias(COL_OBJECT),
            ])
            .expect("TODO");
        self.context
            .storage()
            .insert(quads)
            .await
            .map(|_| ())
            .map_err(LoaderError::from)
    }

    /// Adds a quad to this store.
    ///
    /// Returns `true` if the quad was not already in the store, if the underlying storage
    /// layer supports it. If the storage layer does not support it, this method returns [`None`].
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph);
    ///
    /// let store = Store::new_in_memory().await;
    /// store.insert(quad).await?;
    /// store.insert(quad).await?; // Inserting a quad twice handles deduplication.
    ///
    /// assert!(store.contains(quad).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn insert<'a>(
        &self,
        quad: impl Into<QuadRef<'a>>,
    ) -> Result<Option<bool>, StorageError> {
        self.context
            .storage()
            .insert(quads_to_plain_term_dataframe(
                self.context.session_context(),
                &[quad.into().into_owned()],
            ))
            .await
            .map(|inserted| inserted.map(|inserted| inserted > 0))
    }

    /// Atomically adds a set of quads to this store.
    pub async fn extend(
        &self,
        quads: impl IntoIterator<Item = impl Into<Quad>>,
    ) -> Result<(), StorageError> {
        let quads = quads.into_iter().map(Into::into).collect::<Vec<_>>();
        self.context
            .storage()
            .insert(quads_to_plain_term_dataframe(
                self.context.session_context(),
                &quads,
            ))
            .await?;
        Ok(())
    }

    /// Removes a quad from this store.
    ///
    /// Returns `true` if the quad was in the store and has been removed, if the underlying storage
    /// layer supports it. If the storage layer does not support it, this method returns [`None`].
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph);
    ///
    /// let store = Store::new_in_memory().await;
    /// store.insert(quad).await?;
    /// store.remove(quad).await?;
    /// store.remove(quad).await?; // Removing a quad that does not exist does nothing.
    ///
    /// assert!(!store.contains(quad).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn remove<'a>(
        &self,
        quad: impl Into<QuadRef<'a>>,
    ) -> Result<Option<bool>, StorageError> {
        self.context
            .storage()
            .remove(quads_to_plain_term_dataframe(
                self.context.session_context(),
                &[quad.into().into_owned()],
            ))
            .await
    }

    /// Dumps the store into a file.
    ///
    /// ```
    /// use rdf_fusion::store::Store;
    /// use rdf_fusion::io::RdfFormat;
    /// use rdf_fusion_execution::ingest::RdfParserOptions;
    ///
    /// let file =
    ///     "<http://example.com> <http://example.com> <http://example.com> <http://example.com> .\n"
    ///         .as_bytes();
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new_in_memory().await;
    /// store.load_from_reader(file, RdfParserOptions::with_format(RdfFormat::NQuads)).await?;
    ///
    /// let buffer = store.dump_to_writer(RdfFormat::NQuads, Vec::new()).await?;
    /// assert_eq!(file, buffer.as_slice());
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn dump_to_writer<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        serializer: impl Into<RdfSerializer>,
        writer: W,
    ) -> Result<W, SerializerError> {
        let serializer = serializer.into();
        if !serializer.format().supports_datasets() {
            return Err(SerializerError::DatasetFormatExpected(serializer.format()));
        }
        let mut serializer = serializer.for_tokio_async_writer(writer);
        let mut stream = self.stream().await?;
        while let Some(quad) = stream.next().await {
            serializer.serialize_quad(&quad?).await?;
        }
        Ok(serializer.finish().await?)
    }

    /// Dumps a store graph into a file.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::io::{RdfFormat};
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    /// use rdf_fusion_execution::ingest::RdfParserOptions;
    ///
    /// let file = "<http://example.com> <http://example.com> <http://example.com> .\n".as_bytes();
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new_in_memory().await;
    /// store.load_from_reader(file.as_ref(), RdfParserOptions::with_format(RdfFormat::NTriples)).await?;
    ///
    /// let mut buffer = Vec::new();
    /// store.dump_graph_to_writer(GraphNameRef::DefaultGraph, RdfFormat::NTriples, &mut buffer).await?;
    /// assert_eq!(file, buffer.as_slice());
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn dump_graph_to_writer<'a, W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        from_graph_name: impl Into<GraphNameRef<'a>>,
        serializer: impl Into<RdfSerializer>,
        writer: W,
    ) -> Result<W, SerializerError> {
        let mut serializer = serializer.into().for_tokio_async_writer(writer);
        let mut stream = self
            .quads_for_pattern(None, None, None, Some(from_graph_name.into()))
            .await?;
        while let Some(quad) = stream.next().await {
            serializer.serialize_triple(quad?.as_ref()).await?;
        }
        Ok(serializer.finish().await?)
    }

    /// Returns all the store named graphs.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNode::new("http://example.com")?;
    /// let store = Store::new_in_memory().await;
    /// store.insert(QuadRef::new(&ex, &ex, &ex, &ex)).await?;
    /// store.insert(QuadRef::new(&ex, &ex, &ex, GraphNameRef::DefaultGraph)).await?;
    /// assert_eq!(
    ///     vec![NamedOrBlankNode::from(ex)],
    ///     store.named_graphs().await?
    /// );
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        self.context
            .storage()
            .named_graphs(&self.context.session_context().state())
            .await
    }

    /// Checks if the store contains a given graph
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::{NamedNode, QuadRef};
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNode::new("http://example.com")?;
    /// let store = Store::new_in_memory().await;
    /// store.insert(QuadRef::new(&ex, &ex, &ex, &ex)).await?;
    /// assert!(store.contains_named_graph(&ex).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn contains_named_graph<'a>(
        &self,
        graph_name: impl Into<NamedOrBlankNodeRef<'a>>,
    ) -> Result<bool, QueryEvaluationError> {
        self.context
            .storage()
            .contains_named_graph(
                &self.context.session_context().state(),
                graph_name.into(),
            )
            .await
            .map_err(Into::into)
    }

    /// Inserts a graph into this store.
    ///
    /// Returns `true` if the graph was not already in the store.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::NamedNodeRef;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let store = Store::new_in_memory().await;
    /// store.insert_named_graph(ex).await?;
    ///
    /// assert_eq!(
    ///     store.named_graphs().await?,
    ///     vec![ex.into_owned().into()]
    /// );
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn insert_named_graph<'a>(
        &self,
        graph_name: impl Into<NamedOrBlankNodeRef<'a>>,
    ) -> Result<Option<bool>, StorageError> {
        self.context
            .storage()
            .insert_named_graph(
                &self.context.session_context().state(),
                graph_name.into(),
            )
            .await
    }

    /// Clears a graph from this store.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::{NamedNodeRef, QuadRef};
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, ex);
    /// let store = Store::new_in_memory().await;
    /// store.insert(quad).await?;
    /// assert_eq!(1, store.len().await?);
    ///
    /// store.clear_graph(ex).await?;
    /// assert!(store.is_empty().await?);
    /// assert_eq!(1, store.named_graphs().await?.len());
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn clear_graph<'a>(
        &self,
        graph_name: impl Into<GraphNameRef<'a>>,
    ) -> Result<(), StorageError> {
        self.context
            .storage()
            .clear_graph(&self.context.session_context().state(), graph_name.into())
            .await
    }

    /// Removes a graph from this store.
    ///
    /// Returns `true` if the graph was in the store and has been removed.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::{NamedNodeRef, QuadRef};
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, ex);
    /// let store = Store::new_in_memory().await;
    /// store.insert(quad).await?;
    /// assert_eq!(1, store.len().await?);
    ///
    /// assert!(store.remove_named_graph(ex).await?.unwrap());
    /// assert!(store.is_empty().await?);
    /// assert_eq!(0, store.named_graphs().await?.len());
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn remove_named_graph<'a>(
        &self,
        graph_name: impl Into<NamedOrBlankNodeRef<'a>>,
    ) -> Result<Option<bool>, StorageError> {
        self.context
            .storage()
            .drop_named_graph(&self.context.session_context().state(), graph_name.into())
            .await
    }

    /// Clears the store.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let store = Store::new_in_memory().await;
    /// store.insert(QuadRef::new(ex, ex, ex, ex)).await?;
    /// store.insert(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph)).await?;
    /// assert_eq!(2, store.len().await?);
    ///
    /// store.clear().await?;
    /// assert!(store.is_empty().await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn clear(&self) -> Result<(), StorageError> {
        self.context
            .storage()
            .clear(&self.context.session_context().state())
            .await
    }

    /// Optimizes the database for future workload.
    ///
    /// Useful to call after a batch upload or another similar operation. Usually
    pub async fn optimize(&self) -> Result<(), StorageError> {
        self.context
            .storage()
            .optimize(&self.context.session_context().state())
            .await
    }

    /// Validates that all the store invariants hold in the data storage
    pub async fn validate(&self) -> Result<(), StorageError> {
        self.context
            .storage()
            .validate(&self.context.session_context().state())
            .await
    }
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    use rdf_fusion_model::{
        BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Term,
    };
    use std::collections::HashSet;

    #[test]
    fn test_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<Store>();
    }

    #[tokio::test]
    async fn test_stream_default_graph_quads() -> Result<(), QueryEvaluationError> {
        let store = Store::new_in_memory().await;
        let ex = NamedNodeRef::new("http://example.com")
            .map_err(|e| QueryEvaluationError::InternalError(e.to_string()))?;
        let quad = QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph);

        store.insert(quad).await?;

        let collected_quads = store.stream().await?.try_collect_to_vec().await?;
        assert_eq!(collected_quads, vec![quad.into_owned()]);

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_named_graph_quads() -> Result<(), QueryEvaluationError> {
        let store = Store::new_in_memory().await;
        let ex = NamedNodeRef::new("http://example.com")
            .map_err(|e| QueryEvaluationError::InternalError(e.to_string()))?;
        let graph = GraphName::BlankNode(BlankNode::default());
        let quad = QuadRef::new(ex, ex, ex, graph.as_ref());

        store.insert(quad).await?;

        let collected_quads = store.stream().await?.try_collect_to_vec().await?;
        assert_eq!(collected_quads, vec![quad.into_owned()]);

        Ok(())
    }

    #[tokio::test]
    async fn store() -> Result<(), QueryEvaluationError> {
        let main_s = NamedOrBlankNode::from(BlankNode::default());
        let main_p = NamedNode::new("http://example.com").unwrap();
        let main_o = Term::from(Literal::from(1));
        let main_g = GraphName::from(BlankNode::default());

        let default_quad = Quad::new(
            main_s.clone(),
            main_p.clone(),
            main_o.clone(),
            GraphName::DefaultGraph,
        );
        let named_quad = Quad::new(
            main_s.clone(),
            main_p.clone(),
            main_o.clone(),
            main_g.clone(),
        );
        let mut default_quads = vec![
            Quad::new(
                main_s.clone(),
                main_p.clone(),
                Literal::from(0),
                GraphName::DefaultGraph,
            ),
            default_quad.clone(),
            Quad::new(
                main_s.clone(),
                main_p.clone(),
                Literal::from(200_000_000),
                GraphName::DefaultGraph,
            ),
        ];
        let all_quads = vec![
            named_quad.clone(),
            Quad::new(
                main_s.clone(),
                main_p.clone(),
                Literal::from(200_000_000),
                GraphName::DefaultGraph,
            ),
            default_quad.clone(),
            Quad::new(
                main_s.clone(),
                main_p.clone(),
                Literal::from(0),
                GraphName::DefaultGraph,
            ),
        ];

        let store = Store::new_in_memory().await;
        for t in &default_quads {
            assert!(store.insert(t).await?.unwrap_or(true));
        }
        assert!(!store.insert(&default_quad).await?.unwrap_or(false));

        assert!(store.remove(&default_quad).await?.unwrap_or(true));
        assert!(!store.remove(&default_quad).await?.unwrap_or(false));
        assert!(store.insert(&named_quad).await?.unwrap_or(true));
        assert!(!store.insert(&named_quad).await?.unwrap_or(false));
        assert!(store.insert(&default_quad).await?.unwrap_or(true));
        assert!(!store.insert(&default_quad).await?.unwrap_or(false));
        store.validate().await?;

        assert_eq!(store.len().await?, 4);

        assert_eq!(
            store.stream().await?.try_collect_to_set().await?,
            HashSet::from_iter(all_quads.iter().cloned())
        );
        assert_eq!(
            store
                .quads_for_pattern(Some(main_s.as_ref()), None, None, None)
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from_iter(all_quads.iter().cloned())
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    Some(main_p.as_ref()),
                    None,
                    None
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from_iter(all_quads.iter().cloned())
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    None
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from([named_quad.clone(), default_quad.clone()])
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    Some(GraphNameRef::DefaultGraph)
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from([default_quad.clone()])
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    Some(main_g.as_ref())
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from([named_quad.clone()])
        );

        default_quads.reverse();

        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    Some(main_p.as_ref()),
                    None,
                    Some(GraphNameRef::DefaultGraph)
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from_iter(default_quads.iter().cloned())
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    None,
                    Some(main_o.as_ref()),
                    None
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from([named_quad.clone(), default_quad.clone()])
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    None,
                    Some(main_o.as_ref()),
                    Some(GraphNameRef::DefaultGraph)
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from([default_quad.clone()])
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    None,
                    Some(main_o.as_ref()),
                    Some(main_g.as_ref())
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from([named_quad.clone()])
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    None,
                    None,
                    Some(GraphNameRef::DefaultGraph)
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from_iter(default_quads.iter().cloned())
        );
        assert_eq!(
            store
                .quads_for_pattern(None, Some(main_p.as_ref()), None, None)
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from_iter(all_quads.iter().cloned())
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    None,
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    None
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from([named_quad.clone(), default_quad.clone()])
        );
        assert_eq!(
            store
                .quads_for_pattern(None, None, Some(main_o.as_ref()), None)
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from([named_quad.clone(), default_quad.clone()])
        );
        assert_eq!(
            store
                .quads_for_pattern(None, None, None, Some(GraphNameRef::DefaultGraph))
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from_iter(default_quads.iter().cloned())
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    None,
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    Some(GraphNameRef::DefaultGraph)
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from([default_quad.clone()])
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    None,
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    Some(main_g.as_ref())
                )
                .await?
                .try_collect_to_set()
                .await?,
            HashSet::from([named_quad.clone()])
        );

        Ok(())
    }
}
