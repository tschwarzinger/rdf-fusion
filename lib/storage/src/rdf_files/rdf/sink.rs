use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::DisplayAs;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use oxrdfio::{RdfSerializer, TokioAsyncWriterQuadSerializer};
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{
    GraphNameRef, NamedNode, NamedOrBlankNode, Quad, TermRef, Triple,
};
use rdf_fusion_encoding::plain_term::decoders::{
    DefaultPlainTermDecoder, GraphNameRefPlainTermDecoder,
};
use rdf_fusion_encoding::plain_term::{PLAIN_TERM_ENCODING, PlainTermEncoding};
use rdf_fusion_encoding::{TermDecoder, TermEncoding};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::rdf_files::rdf::RdfFormat;
use datafusion::datasource::file_format::parquet::ParquetSink;

/// A [`DataSink`] that handles both RDF and Parquet serialization.
#[derive(Debug)]
pub struct RdfDataSink {
    inner: RdfDataSinkInner,
    schema: SchemaRef,
}

#[derive(Debug)]
enum RdfDataSinkInner {
    Oxigraph(Box<OxigraphRdfDataSink>),
    Parquet(Box<ParquetSink>),
}

impl RdfDataSink {
    /// Creates a new [`RdfDataSink`] for RDF formats.
    pub fn new_rdf(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        format: RdfFormat,
        schema: SchemaRef,
    ) -> Self {
        let oxigraph_format = format.to_oxigraph().expect("RDF format expected");
        Self {
            inner: RdfDataSinkInner::Oxigraph(Box::new(OxigraphRdfDataSink::new(
                object_store,
                path,
                oxigraph_format,
                Arc::clone(&schema),
            ))),
            schema,
        }
    }

    /// Creates a new [`RdfDataSink`] for Parquet format.
    pub fn new_parquet(sink: ParquetSink, schema: SchemaRef) -> Self {
        Self {
            inner: RdfDataSinkInner::Parquet(Box::new(sink)),
            schema,
        }
    }

    /// Compatibility constructor for existing code.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        format: RdfFormat,
        schema: SchemaRef,
    ) -> Self {
        match format {
            RdfFormat::Parquet => {
                // This is a bit of a hack because we need FileSinkConfig to create ParquetSink properly.
                // In store.dump, we might need to adjust how this is called.
                panic!("Use new_parquet for Parquet format or provide enough config");
            }
            _ => Self::new_rdf(object_store, path, format, schema),
        }
    }
}

impl DisplayAs for RdfDataSink {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match &self.inner {
            RdfDataSinkInner::Oxigraph(s) => s.fmt_as(t, f),
            RdfDataSinkInner::Parquet(s) => s.fmt_as(t, f),
        }
    }
}

#[async_trait]
impl DataSink for RdfDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        self.validate_schema()?;

        match &self.inner {
            RdfDataSinkInner::Oxigraph(s) => s.write_all(data, context).await,
            RdfDataSinkInner::Parquet(s) => s.write_all(data, context).await,
        }
    }
}

impl RdfDataSink {
    fn validate_schema(&self) -> datafusion::common::Result<()> {
        let has_subject = self.schema.field_with_name(COL_SUBJECT).is_ok();
        let has_predicate = self.schema.field_with_name(COL_PREDICATE).is_ok();
        let has_object = self.schema.field_with_name(COL_OBJECT).is_ok();

        if !has_subject || !has_predicate || !has_object {
            return Err(DataFusionError::Execution(format!(
                "Schema must contain at least subject, predicate, and object columns. Found: {:?}",
                self.schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            )));
        }
        Ok(())
    }
}

/// A [`DataSink`] for writing RDF data using Oxigraph's serializers.
#[derive(Debug)]
pub struct OxigraphRdfDataSink {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    format: oxrdfio::RdfFormat,
    schema: SchemaRef,
}

impl OxigraphRdfDataSink {
    /// Creates a new [`OxigraphRdfDataSink`].
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        format: oxrdfio::RdfFormat,
        schema: SchemaRef,
    ) -> Self {
        Self {
            object_store,
            path,
            format,
            schema,
        }
    }
}

impl DisplayAs for OxigraphRdfDataSink {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "OxigraphRdfDataSink(path={})", self.path)
    }
}

#[async_trait]
impl DataSink for OxigraphRdfDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut serializer = self.create_serializer();

        let has_graph = self.schema.field_with_name(COL_GRAPH).is_ok();

        let mut data = data;
        if !self.format.supports_datasets() && has_graph {
            data = Box::pin(StrictTripleStream::new(data)?);
        }

        let mut count = 0;
        let has_graph = data.schema().field_with_name(COL_GRAPH).is_ok();

        while let Some(batch) = data.next().await {
            let batch = batch?;
            self.write_batch(&batch, &mut serializer, has_graph).await?;
            count += batch.num_rows() as u64;
        }

        self.finish_serializer(serializer).await?;

        Ok(count)
    }
}

impl OxigraphRdfDataSink {
    /// Creates a new quad serializer for the given format and object store path.
    fn create_serializer(
        &self,
    ) -> TokioAsyncWriterQuadSerializer<object_store::buffered::BufWriter> {
        let writer = object_store::buffered::BufWriter::new(
            Arc::clone(&self.object_store),
            self.path.clone(),
        );
        RdfSerializer::from_format(self.format).for_tokio_async_writer(writer)
    }

    /// Finishes the serializer and ensures all data is written and the writer is closed.
    async fn finish_serializer<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        serializer: TokioAsyncWriterQuadSerializer<W>,
    ) -> datafusion::common::Result<()> {
        let mut writer = serializer
            .finish()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        use tokio::io::AsyncWriteExt;
        writer
            .shutdown()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }

    /// Writes a single [`RecordBatch`] to the serializer.
    async fn write_batch<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        batch: &RecordBatch,
        serializer: &mut TokioAsyncWriterQuadSerializer<W>,
        has_graph: bool,
    ) -> datafusion::common::Result<()> {
        if has_graph {
            self.write_quads_batch(batch, serializer).await
        } else {
            self.write_triples_batch(batch, serializer).await
        }
    }

    /// Writes a batch of quads.
    async fn write_quads_batch<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        batch: &RecordBatch,
        serializer: &mut TokioAsyncWriterQuadSerializer<W>,
    ) -> datafusion::common::Result<()> {
        let graphs = self.decode_column(batch, COL_GRAPH)?;
        let subjects = self.decode_column(batch, COL_SUBJECT)?;
        let predicates = self.decode_column(batch, COL_PREDICATE)?;
        let objects = self.decode_column(batch, COL_OBJECT)?;

        let graph_terms = GraphNameRefPlainTermDecoder::decode_terms(&graphs);
        let subject_terms = DefaultPlainTermDecoder::decode_terms(&subjects);
        let predicate_terms = DefaultPlainTermDecoder::decode_terms(&predicates);
        let object_terms = DefaultPlainTermDecoder::decode_terms(&objects);

        for (((g, s), p), o) in graph_terms
            .zip(subject_terms)
            .zip(predicate_terms)
            .zip(object_terms)
        {
            let g = g.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let s = s.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let p = p.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let o = o.map_err(|e| DataFusionError::External(Box::new(e)))?;

            self.serialize_quad(serializer, g, s, p, o).await?;
        }
        Ok(())
    }

    /// Serializes a single quad.
    async fn serialize_quad<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        serializer: &mut TokioAsyncWriterQuadSerializer<W>,
        g: GraphNameRef<'_>,
        s: TermRef<'_>,
        p: TermRef<'_>,
        o: TermRef<'_>,
    ) -> datafusion::common::Result<()> {
        let quad =
            Quad::new(Self::extract_subject(s)?, Self::extract_predicate(p)?, o, g);
        serializer
            .serialize_quad(&quad)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    /// Writes a batch of triples.
    async fn write_triples_batch<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        batch: &RecordBatch,
        serializer: &mut TokioAsyncWriterQuadSerializer<W>,
    ) -> datafusion::common::Result<()> {
        let subjects = self.decode_column(batch, COL_SUBJECT)?;
        let predicates = self.decode_column(batch, COL_PREDICATE)?;
        let objects = self.decode_column(batch, COL_OBJECT)?;

        let subject_terms = DefaultPlainTermDecoder::decode_terms(&subjects);
        let predicate_terms = DefaultPlainTermDecoder::decode_terms(&predicates);
        let object_terms = DefaultPlainTermDecoder::decode_terms(&objects);

        for ((s, p), o) in subject_terms.zip(predicate_terms).zip(object_terms) {
            let s = s.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let p = p.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let o = o.map_err(|e| DataFusionError::External(Box::new(e)))?;

            self.serialize_triple(serializer, s, p, o).await?;
        }
        Ok(())
    }

    /// Serializes a single triple.
    async fn serialize_triple<W: tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        serializer: &mut TokioAsyncWriterQuadSerializer<W>,
        s: TermRef<'_>,
        p: TermRef<'_>,
        o: TermRef<'_>,
    ) -> datafusion::common::Result<()> {
        let triple =
            Triple::new(Self::extract_subject(s)?, Self::extract_predicate(p)?, o);
        serializer
            .serialize_triple(triple.as_ref())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    /// Decodes a column from a [`RecordBatch`] into a term array.
    fn decode_column(
        &self,
        batch: &RecordBatch,
        col_name: &str,
    ) -> datafusion::common::Result<<PlainTermEncoding as TermEncoding>::Array> {
        let column = batch.column_by_name(col_name).ok_or_else(|| {
            DataFusionError::Execution(format!("Column {col_name} not found"))
        })?;
        PLAIN_TERM_ENCODING
            .try_new_array(Arc::clone(column))
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Extracts a subject from a [`TermRef`].
    fn extract_subject(s: TermRef<'_>) -> datafusion::common::Result<NamedOrBlankNode> {
        match s {
            TermRef::NamedNode(nn) => Ok(nn.into()),
            TermRef::BlankNode(bn) => Ok(bn.into()),
            TermRef::Literal(_) => Err(DataFusionError::Execution(
                "Subject cannot be a literal".to_string(),
            )),
        }
    }

    /// Extracts a predicate from a [`TermRef`].
    fn extract_predicate(p: TermRef<'_>) -> datafusion::common::Result<NamedNode> {
        match p {
            TermRef::NamedNode(nn) => Ok(nn.into()),
            _ => Err(DataFusionError::Execution(
                "Predicate must be a named node".to_string(),
            )),
        }
    }
}

/// A stream that enforces that all quads belong to the default graph and projects them to triples.
struct StrictTripleStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl StrictTripleStream {
    fn new(inner: SendableRecordBatchStream) -> datafusion::common::Result<Self> {
        let schema = inner.schema();
        let projected_schema = Arc::new(schema.project(&[
            schema.index_of(COL_SUBJECT)?,
            schema.index_of(COL_PREDICATE)?,
            schema.index_of(COL_OBJECT)?,
        ])?);
        Ok(Self {
            inner,
            schema: projected_schema,
        })
    }
}

impl futures::Stream for StrictTripleStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => {
                let graph_col = batch.column_by_name(COL_GRAPH).unwrap();
                let graphs = PLAIN_TERM_ENCODING
                    .try_new_array(Arc::clone(graph_col))
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let graph_terms = GraphNameRefPlainTermDecoder::decode_terms(&graphs);

                for g in graph_terms {
                    let g = g.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    if g != GraphNameRef::DefaultGraph {
                        return std::task::Poll::Ready(Some(Err(DataFusionError::Execution(
                            "Encountered non-default graph while dumping to a triple-only format."
                                .to_string(),
                        ))));
                    }
                }

                let s = Arc::clone(batch.column_by_name(COL_SUBJECT).unwrap());
                let p = Arc::clone(batch.column_by_name(COL_PREDICATE).unwrap());
                let o = Arc::clone(batch.column_by_name(COL_OBJECT).unwrap());

                let projected_batch =
                    RecordBatch::try_new(Arc::clone(&self.schema), vec![s, p, o])
                        .map_err(DataFusionError::from);

                std::task::Poll::Ready(Some(projected_batch))
            }
            other => other,
        }
    }
}

impl datafusion::execution::RecordBatchStream for StrictTripleStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use object_store::memory::InMemory;
    use object_store::{GetResult, ObjectStore, ObjectStoreExt};
    use oxrdfio::RdfParser;
    use rdf_fusion_common::{NamedNode, Quad};
    use rdf_fusion_encoding::QuadStorageEncoding;
    use rdf_fusion_encoding::plain_term::PlainTermQuadsBuilder;

    #[tokio::test]
    async fn test_rdf_data_sink_triples() -> datafusion::common::Result<()> {
        let store = Arc::new(InMemory::new());
        let path = Path::from("test.ttl");
        let schema = Arc::clone(QuadStorageEncoding::PlainTerm.quad_schema().inner());

        // Create a batch with some triples
        let mut builder = PlainTermQuadsBuilder::with_capacity(2);
        let s = NamedNode::new_unchecked("http://example.org/s");
        let p = NamedNode::new_unchecked("http://example.org/p");
        let o1 = NamedNode::new_unchecked("http://example.org/o1");
        let o2 = NamedNode::new_unchecked("http://example.org/o2");

        builder.append_quad(
            Quad::new(s.clone(), p.clone(), o1.clone(), GraphNameRef::DefaultGraph)
                .as_ref(),
        );
        builder.append_quad(
            Quad::new(s.clone(), p.clone(), o2.clone(), GraphNameRef::DefaultGraph)
                .as_ref(),
        );

        let batch = builder.finish().into_record_batch();
        // Remove graph column for triples test
        let triple_schema = Arc::new(schema.project(&[1, 2, 3])?);
        let triple_batch = RecordBatch::try_new(
            Arc::clone(&triple_schema),
            vec![
                Arc::clone(batch.column(1)),
                Arc::clone(batch.column(2)),
                Arc::clone(batch.column(3)),
            ],
        )?;

        let sink = RdfDataSink::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            path.clone(),
            RdfFormat::Turtle,
            Arc::clone(&triple_schema),
        );

        let stream = RecordBatchStreamAdapter::new(
            triple_schema,
            futures::stream::iter(vec![Ok(triple_batch)]),
        );

        let ctx = Arc::new(TaskContext::default());
        sink.write_all(Box::pin(stream), &ctx).await?;

        // Read back and verify
        let get_result: GetResult = store.get(&path).await?;
        let bytes = get_result.bytes().await?;
        let mut parser =
            RdfParser::from_format(oxrdfio::RdfFormat::Turtle).for_reader(&bytes[..]);
        let mut count = 0;
        while let Some(quad_result) = parser.next() {
            quad_result.map_err(|e| DataFusionError::External(Box::new(e)))?;
            count += 1;
        }
        assert_eq!(count, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_rdf_data_sink_quads() -> datafusion::common::Result<()> {
        let store = Arc::new(InMemory::new());
        let path = Path::from("test.nq");
        let schema = Arc::clone(QuadStorageEncoding::PlainTerm.quad_schema().inner());

        let mut builder = PlainTermQuadsBuilder::with_capacity(1);
        let g = NamedNode::new_unchecked("http://example.org/g");
        let s = NamedNode::new_unchecked("http://example.org/s");
        let p = NamedNode::new_unchecked("http://example.org/p");
        let o = NamedNode::new_unchecked("http://example.org/o");

        builder
            .append_quad(Quad::new(s.clone(), p.clone(), o.clone(), g.clone()).as_ref());

        let batch = builder.finish().into_record_batch();

        let sink = RdfDataSink::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            path.clone(),
            RdfFormat::NQuads,
            Arc::clone(&schema),
        );

        let stream =
            RecordBatchStreamAdapter::new(schema, futures::stream::iter(vec![Ok(batch)]));

        let ctx = Arc::new(TaskContext::default());
        sink.write_all(Box::pin(stream), &ctx).await?;

        // Read back and verify
        let get_result: GetResult = store.get(&path).await?;
        let bytes = get_result.bytes().await?;
        let mut parser =
            RdfParser::from_format(oxrdfio::RdfFormat::NQuads).for_reader(&bytes[..]);
        let quad_result = parser
            .next()
            .unwrap()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let quad: rdf_fusion_common::Quad = quad_result;
        assert_eq!(quad.subject.to_string(), "<http://example.org/s>");
        assert_eq!(quad.graph_name.to_string(), "<http://example.org/g>");

        Ok(())
    }
}
