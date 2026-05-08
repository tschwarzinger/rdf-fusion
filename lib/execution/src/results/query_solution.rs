use crate::sparql::error::QueryEvaluationError;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::exec_err;
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::Variable;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::plain_term::{PLAIN_TERM_ENCODING, PlainTermEncoding};
use rdf_fusion_encoding::{TermDecoder, TermEncoding};
pub use sparesults::QuerySolution;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

/// A stream over [`QuerySolution`]s.
pub struct QuerySolutionStream {
    /// The variables used in the query solutions.
    variables: Arc<[Variable]>,
    /// The underlying DataFusion record batch stream that provides the raw data.
    ///
    /// This is wrapped in an Option for termination handling.
    inner: Option<SendableRecordBatchStream>,
    /// The current batch of solutions being processed.
    ///
    /// When a new batch is loaded from `inner`, it's converted to query solutions
    /// and stored here until all solutions in the batch are consumed.
    current: Option<<Vec<QuerySolution> as IntoIterator>::IntoIter>,
}

impl QuerySolutionStream {
    /// Construct a new iterator of solutions from an ordered list of solution variables and an iterator of solution tuples
    /// (each tuple using the same ordering as the variable list such that tuple element 0 is the value for the variable 0...)
    pub fn try_new(
        variables: Arc<[Variable]>,
        inner: SendableRecordBatchStream,
    ) -> DFResult<Self> {
        for field in inner.schema().fields() {
            if &PlainTermEncoding::data_type() != field.data_type() {
                return exec_err!(
                    "Field {field} has unsupported type {} for query solution.",
                    field.data_type()
                );
            }
        }

        Ok(Self {
            variables,
            inner: Some(inner),
            current: None,
        })
    }

    /// The variables used in the solutions.
    #[inline]
    pub fn variables(&self) -> &[Variable] {
        self.variables.as_ref()
    }

    /// Returns the underlying DataFusion [SendableRecordBatchStream].
    ///
    /// It is guaranteed that [SendableRecordBatchStream] has only column with RDF terms in the
    /// [PlainTermEncoding].
    ///
    /// # Errors
    ///
    /// If the stream has been fully consumed.
    pub fn into_record_batch_stream(self) -> DFResult<SendableRecordBatchStream> {
        match self.inner {
            None => exec_err!("Stream has already been consumed."),
            Some(stream) => Ok(stream),
        }
    }

    fn poll_inner(
        &mut self,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Result<QuerySolution, QueryEvaluationError>>> {
        match (&mut self.inner, &mut self.current) {
            // Still entries from the current batch to return
            (_, Some(iter)) => {
                let next = iter.next();
                match next {
                    None => {
                        self.current = None;
                        self.poll_inner(ctx)
                    }
                    Some(solution) => Poll::Ready(Some(Ok(solution))),
                }
            }
            // Load new batch
            (Some(stream), None) => {
                let next_batch = ready!(stream.poll_next_unpin(ctx));
                match next_batch {
                    None => {
                        self.inner = None;
                        self.poll_inner(ctx)
                    }
                    Some(batch) => {
                        let query_solution = to_query_solution(&self.variables, &batch?)?;
                        self.current = Some(query_solution);
                        self.poll_inner(ctx)
                    }
                }
            }
            // Empty
            (None, None) => Poll::Ready(None),
        }
    }
}

impl Stream for QuerySolutionStream {
    type Item = Result<QuerySolution, QueryEvaluationError>;

    #[inline]
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

fn to_query_solution(
    variables: &[Variable],
    batch: &RecordBatch,
) -> Result<<Vec<QuerySolution> as IntoIterator>::IntoIter, QueryEvaluationError> {
    let num_rows = batch.num_rows();

    // Get column terms first - compute all terms for each column
    let mut column_terms = Vec::with_capacity(variables.len());

    for variable in variables {
        let column = batch.column_by_name(variable.as_str()).ok_or_else(|| {
            QueryEvaluationError::InternalError(format!(
                "Variable {variable} was not present in result."
            ))
        })?;

        // Convert the column to a PlainTermEncoding array
        let array = PLAIN_TERM_ENCODING
            .try_new_array(Arc::clone(column))
            .map_err(|e| {
                QueryEvaluationError::InternalError(format!(
                    "Failed to convert column to PlainTermEncoding: {e}"
                ))
            })?;

        // Decode all terms for this column at once
        let terms = DefaultPlainTermDecoder::decode_terms(&array)
            .map(|t| match t {
                Ok(t) => Ok(Some(t.into_owned())),
                Err(_) => Ok(None),
            })
            .collect::<DFResult<Vec<_>>>()
            .map_err(|e| {
                QueryEvaluationError::InternalError(format!(
                    "Failed to decode terms: {e}"
                ))
            })?;

        column_terms.push(terms.into_iter());
    }

    // Now build the solutions row by row
    let mut result = Vec::with_capacity(num_rows);
    let variables_arc: Arc<[Variable]> = variables.into();
    for _ in 0..num_rows {
        let mut row_terms = Vec::with_capacity(column_terms.len());

        // Get the term at index i from each column
        #[allow(clippy::expect_used)]
        for column in &mut column_terms {
            let term = column.next().expect("Length is guaranteed");
            row_terms.push(term);
        }

        result.push((Arc::clone(&variables_arc), row_terms).into());
    }

    Ok(result.into_iter())
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    use crate::results::{QueryResults, query_result_for_iterator};
    use rdf_fusion_common::{BlankNode, Literal, NamedNode};
    use sparesults::QueryResultsFormat;
    use std::error::Error;
    use std::io::Cursor;

    #[test]
    fn test_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<QuerySolution>();
    }

    #[tokio::test]
    async fn test_serialization_roundtrip() -> Result<(), Box<dyn Error>> {
        use std::str;

        for format in [
            QueryResultsFormat::Json,
            QueryResultsFormat::Xml,
            QueryResultsFormat::Tsv,
        ] {
            let variables: Arc<[Variable]> = Arc::new([
                Variable::new_unchecked("foo"),
                Variable::new_unchecked("bar"),
            ]);

            let terms = vec![
                vec![None, None],
                vec![
                    Some(NamedNode::new_unchecked("http://example.com").into()),
                    None,
                ],
                vec![
                    None,
                    Some(NamedNode::new_unchecked("http://example.com").into()),
                ],
                vec![
                    Some(BlankNode::new_unchecked("foo").into()),
                    Some(BlankNode::new_unchecked("bar").into()),
                ],
                vec![Some(Literal::new_simple_literal("foo").into()), None],
                vec![
                    Some(
                        Literal::new_language_tagged_literal_unchecked("foo", "fr")
                            .into(),
                    ),
                    None,
                ],
                vec![
                    Some(Literal::from(1).into()),
                    Some(Literal::from(true).into()),
                ],
                vec![
                    Some(Literal::from(1.33).into()),
                    Some(Literal::from(false).into()),
                ],
                //F TODO #3: Quoted Triples
                // vec![
                //     Some(
                //         Triple::new(
                //             NamedNode::new_unchecked("http://example.com/s"),
                //             NamedNode::new_unchecked("http://example.com/p"),
                //             Triple::new(
                //                 NamedNode::new_unchecked("http://example.com/os"),
                //                 NamedNode::new_unchecked("http://example.com/op"),
                //                 NamedNode::new_unchecked("http://example.com/oo"),
                //             ),
                //         )
                //         .into(),
                //     ),
                //     None,
                // ],
            ]
            .into_iter()
            .map(|ts| Ok(QuerySolution::from((Arc::clone(&variables), ts))));
            let results = vec![
                QueryResults::Boolean(true),
                QueryResults::Boolean(false),
                query_result_for_iterator(Arc::clone(&variables), terms)?,
            ];

            for ex in results {
                let mut buffer = Vec::new();
                ex.write(&mut buffer, format).await?;
                let ex2 = QueryResults::read(Cursor::new(buffer.clone()), format).await?;
                let mut buffer2 = Vec::new();
                ex2.write(&mut buffer2, format).await?;
                assert_eq!(
                    str::from_utf8(&buffer).unwrap(),
                    str::from_utf8(&buffer2).unwrap()
                );
            }
        }

        Ok(())
    }
}
