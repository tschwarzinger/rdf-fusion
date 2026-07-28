use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use rdf_fusion_common::StorageError;
use rdf_fusion_common::quads::COL_GRAPH;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::{EncodingScalar, QuadStorageEncoding, TermEncoding};
use rdf_fusion_extensions::storage::{QuadStorageGraphTarget, QuadStorageSnapshot};
use std::sync::Arc;

/// Creates a [`DataFrame`] from the given graph target using the plain term encoding.
pub async fn graph_target_to_plain_term_dataframe(
    session: &SessionContext,
    storage_encoding: &QuadStorageEncoding,
    snapshot: &dyn QuadStorageSnapshot,
    graph: &QuadStorageGraphTarget,
) -> Result<DataFrame, StorageError> {
    match graph {
        QuadStorageGraphTarget::NamedNode(graph_name) => {
            let scalar_value = PLAIN_TERM_ENCODING
                .encode_term(Ok(graph_name.as_ref().into()))
                .expect("Valid term")
                .into_scalar_value();
            let schema = Arc::new(Schema::new(vec![Field::new(
                COL_GRAPH,
                scalar_value.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(
                schema,
                vec![scalar_value.to_array_of_size(1).expect("Valid array")],
            )
            .expect("Valid batch");
            session
                .read_batch(batch)
                .map_err(|e| StorageError::Other(Box::new(e)))
        }
        QuadStorageGraphTarget::BlankNode(graph_name) => {
            let scalar_value = PLAIN_TERM_ENCODING
                .encode_term(Ok(graph_name.as_ref().into()))
                .expect("Valid term")
                .into_scalar_value();
            let schema = Arc::new(Schema::new(vec![Field::new(
                COL_GRAPH,
                scalar_value.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(
                schema,
                vec![scalar_value.to_array_of_size(1).expect("Valid array")],
            )
            .expect("Valid batch");
            session
                .read_batch(batch)
                .map_err(|e| StorageError::Other(Box::new(e)))
        }
        QuadStorageGraphTarget::DefaultGraph => {
            let schema = Arc::new(Schema::new(vec![Field::new(
                COL_GRAPH,
                PLAIN_TERM_ENCODING.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(
                schema,
                vec![PLAIN_TERM_ENCODING.create_null_array(1).into_array_ref()],
            )
            .expect("Valid batch");
            session
                .read_batch(batch)
                .map_err(|e| StorageError::Other(Box::new(e)))
        }
        QuadStorageGraphTarget::NamedGraphs | QuadStorageGraphTarget::AllGraphs => {
            let named_graphs = snapshot.named_graphs(&session.state()).await?;
            let mut stream = datafusion::physical_plan::execute_stream(
                named_graphs,
                session.task_ctx(),
            )
            .map_err(|e| StorageError::Other(Box::new(e)))?;

            let mut plain_term_batches = Vec::new();
            let pt_schema = Arc::new(Schema::new(vec![Field::new(
                COL_GRAPH,
                PLAIN_TERM_ENCODING.data_type().clone(),
                true,
            )]));

            while let Some(record_batch) = stream.next().await {
                let record_batch =
                    record_batch.map_err(|e| StorageError::Other(Box::new(e)))?;
                let column = &record_batch.columns()[0];
                let plain_term_array = match &storage_encoding {
                    QuadStorageEncoding::PlainTerm => Arc::clone(column),
                    QuadStorageEncoding::ObjectId(encoding) => encoding
                        .mapping()
                        .decode_array(column)
                        .await
                        .map_err(|e| StorageError::Other(Box::new(e)))?
                        .into_array_ref(),
                    QuadStorageEncoding::String => {
                        use rdf_fusion_encoding::string::STRING_ENCODING;
                        STRING_ENCODING
                            .try_new_array(Arc::clone(column))
                            .map_err(|e| StorageError::Other(Box::new(e)))?
                            .as_plain_term_array()
                            .map_err(|e| StorageError::Other(Box::new(e)))?
                            .into_array_ref()
                    }
                };

                let pt_batch =
                    RecordBatch::try_new(Arc::clone(&pt_schema), vec![plain_term_array])
                        .expect("Valid batch");
                plain_term_batches.push(pt_batch);
            }

            if matches!(graph, QuadStorageGraphTarget::AllGraphs) {
                let default_graph_batch = RecordBatch::try_new(
                    Arc::clone(&pt_schema),
                    vec![PLAIN_TERM_ENCODING.create_null_array(1).into_array_ref()],
                )
                .expect("Schema should match");
                plain_term_batches.push(default_graph_batch);
            }

            session
                .read_batches(plain_term_batches)
                .map_err(|e| StorageError::Other(Box::new(e)))
        }
    }
}
