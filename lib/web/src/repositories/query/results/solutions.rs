use axum::body::{Body, Bytes};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use futures::StreamExt;
use rdf_fusion::common::{TermRef, ThinResult, Variable};
use rdf_fusion::encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion::encoding::plain_term::{PLAIN_TERM_ENCODING, PlainTermArray};
use rdf_fusion::encoding::{TermDecoder, TermEncoding};
use rdf_fusion::execution::results::{
    QueryResultsFormat, QueryResultsSerializer, QuerySolutionStream,
    WriterSolutionsSerializer,
};
use rdf_fusion::execution::sparql::error::QueryEvaluationError;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    /// Extracts the accumulated bytes and resets the inner buffer
    fn take_bytes(&self) -> Option<Bytes> {
        let mut guard = self.0.lock().unwrap();
        if guard.is_empty() {
            None
        } else {
            Some(Bytes::from(std::mem::take(&mut *guard)))
        }
    }
}

impl std::io::Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Serializes `solutions` according to `format`.
pub fn serialize_solutions(
    solutions: QuerySolutionStream,
    format: QueryResultsFormat,
) -> anyhow::Result<Body> {
    let variables = solutions.variables().to_vec();
    let shared_buffer = SharedBuffer::default();

    let mut serializer = QueryResultsSerializer::from_format(format)
        .serialize_solutions_to_writer(shared_buffer.clone(), variables.clone())?;

    let (tx, rx) = mpsc::channel(4);

    SpawnedTask::spawn(async move {
        // Helper macro to handle errors and send them across the channel
        macro_rules! try_or_send_err {
            ($res:expr) => {
                match $res {
                    Ok(val) => val,
                    Err(e) => {
                        let err =
                            std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
                        let _ = tx.send(Err(err)).await;
                        return;
                    }
                }
            };
        }

        let mut stream = try_or_send_err!(solutions.into_record_batch_stream());

        while let Some(batch_res) = stream.next().await {
            let batch = try_or_send_err!(batch_res);

            try_or_send_err!(handle_record_batch(&mut serializer, &variables, batch));

            if let Some(chunk) = shared_buffer.take_bytes() {
                if tx.send(Ok(chunk)).await.is_err() {
                    return; // Receiver dropped, exit early
                }
            }
        }

        try_or_send_err!(serializer.finish());

        if let Some(chunk) = shared_buffer.take_bytes() {
            let _ = tx.send(Ok(chunk)).await;
        }
    });

    Ok(Body::from_stream(ReceiverStream::new(rx)))
}

fn handle_record_batch<W: std::io::Write>(
    serializer: &mut WriterSolutionsSerializer<W>,
    variables: &[Variable],
    record_batch: RecordBatch,
) -> anyhow::Result<()> {
    let arrays = create_plain_term_arrays(variables, &record_batch)?;
    let mut iterators = get_term_iterators(&arrays);

    // We iterate directly, zipping iterators to avoid creating a new `Vec` allocation for every row.
    for _ in 0..record_batch.num_rows() {
        let solution = variables
            .iter()
            .zip(&mut iterators)
            .filter_map(|(var, it)| {
                it.next()
                    .expect("Length known")
                    .ok()
                    .map(|term| (var.as_ref(), term))
            });

        serializer.serialize(solution)?;
    }

    Ok(())
}

/// Extracts a vector of [PlainTermArray] from the underlying [RecordBatch].
fn create_plain_term_arrays(
    variables: &[Variable],
    record_batch: &RecordBatch,
) -> Result<Vec<PlainTermArray>, QueryEvaluationError> {
    variables
        .iter()
        .map(|v| {
            let arr = record_batch.column_by_name(v.as_str()).ok_or_else(|| {
                QueryEvaluationError::InternalError(format!(
                    "Cannot find variable '{v}' in the result set."
                ))
            })?;

            PLAIN_TERM_ENCODING
                .try_new_array(Arc::clone(arr))
                .map_err(|_| {
                    QueryEvaluationError::InternalError(
                        "Failed to convert column to PlainTermEncoding".to_owned(),
                    )
                })
        })
        .collect()
}

/// Create a new [TermRef] iterator over each of the [PlainTermArray].
fn get_term_iterators(
    arrays: &[PlainTermArray],
) -> Vec<impl Iterator<Item = ThinResult<TermRef<'_>>>> {
    arrays
        .iter()
        .map(DefaultPlainTermDecoder::decode_terms)
        .collect()
}
