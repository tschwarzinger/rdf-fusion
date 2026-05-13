use anyhow::Context;
use datafusion::arrow::array::RecordBatch;
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
use std::sync::Arc;

/// Serializes `solutions` according to `format`.
pub async fn serialize_solutions(
    solutions: QuerySolutionStream,
    format: QueryResultsFormat,
) -> anyhow::Result<Vec<u8>> {
    let variables = solutions.variables().to_vec();
    let mut buffer = Vec::new();

    let mut serializer = QueryResultsSerializer::from_format(format)
        .serialize_solutions_to_writer(&mut buffer, variables.clone())?;

    let mut solutions = solutions.into_record_batch_stream()?;
    while let Some(solutions) = solutions.next().await {
        let solutions = solutions?;
        handle_record_batch(&mut serializer, &variables, solutions)?;
    }

    serializer
        .finish()
        .context("Could not finalize serializer")?;
    Ok(buffer)
}

fn handle_record_batch(
    serializer: &mut WriterSolutionsSerializer<&mut Vec<u8>>,
    variables: &[Variable],
    record_batch: RecordBatch,
) -> anyhow::Result<()> {
    let arrays = create_plain_term_arrays(variables, &record_batch)?;
    let mut iterators = get_term_iterators(&arrays);
    for _ in 0..record_batch.num_rows() {
        serialize_solution(serializer, variables, &mut iterators)?;
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
            record_batch.column_by_name(v.as_str()).ok_or_else(|| {
                QueryEvaluationError::InternalError(format!(
                    "Cannot find variable '{v}' in the result set."
                ))
            })
        })
        .map(|col| {
            col.and_then(|arr| {
                PLAIN_TERM_ENCODING
                    .try_new_array(Arc::clone(arr))
                    .map_err(|_| {
                        QueryEvaluationError::InternalError(
                            "Failed to convert column to PlainTermEncoding".to_owned(),
                        )
                    })
            })
        })
        .collect()
}

/// Create a new [TermRef] iterator over each of the [PlainTermArray].
///
/// The caller must own the arrays, as we otherwise must allocate for a term.
fn get_term_iterators(
    arrays: &[PlainTermArray],
) -> Vec<impl Iterator<Item = ThinResult<TermRef<'_>>>> {
    arrays
        .iter()
        .map(DefaultPlainTermDecoder::decode_terms)
        .collect::<Vec<_>>()
}

/// Serializes a single solution into the `serialiter`.
fn serialize_solution<'terms>(
    serializer: &mut WriterSolutionsSerializer<&mut Vec<u8>>,
    variables: &[Variable],
    iterators: &mut Vec<impl Iterator<Item = ThinResult<TermRef<'terms>>>>,
) -> anyhow::Result<()> {
    let mut terms = Vec::new();
    for iterator in iterators {
        let term_ref = iterator.next().expect("Length known").ok();
        terms.push(term_ref)
    }

    let solution = variables
        .iter()
        .map(|v| v.as_ref())
        .zip(terms)
        .filter_map(|(var, term)| term.map(|term| (var, term)));
    serializer.serialize(solution)?;
    Ok(())
}
