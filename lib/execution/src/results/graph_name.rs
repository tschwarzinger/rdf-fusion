use crate::results::QuerySolutionStream;
use crate::sparql::error::QueryEvaluationError;
use datafusion::common::exec_err;
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::quads::COL_GRAPH;
use rdf_fusion_common::{NamedOrBlankNode, Term, Variable};
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

/// An iterator yielding graph names.
pub struct GraphNameStream {
    stream: QuerySolutionStream,
}

impl GraphNameStream {
    /// Creates a new [GraphNameStream] based on a [SendableRecordBatchStream].
    ///
    /// # Errors
    ///
    /// Returns an error if the schema has not exactly one field with the data type of encoded
    /// terms.
    pub fn try_new(stream: SendableRecordBatchStream) -> DFResult<Self> {
        if stream.schema().fields.len() != 1 {
            return exec_err!("Unexpected number of columns in the result");
        }

        if stream.schema().field(0).data_type() != PLAIN_TERM_ENCODING.data_type() {
            return exec_err!("Unexpected data type in the result");
        }

        let solutions_stream = QuerySolutionStream::try_new(
            Arc::new([Variable::new_unchecked(COL_GRAPH)]),
            stream,
        )?;
        Ok(Self {
            stream: solutions_stream,
        })
    }

    pub async fn try_collect_to_vec(
        mut self,
    ) -> Result<Vec<NamedOrBlankNode>, QueryEvaluationError> {
        let mut result = Vec::new();
        while let Some(element) = self.next().await {
            result.push(element?);
        }
        Ok(result)
    }
}

impl Stream for GraphNameStream {
    type Item = Result<NamedOrBlankNode, QueryEvaluationError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Some(inner) = ready!(self.stream.poll_next_unpin(cx)) else {
            return Poll::Ready(None);
        };

        let graph_name = inner
            .and_then(|s| {
                s.get(COL_GRAPH)
                    .cloned()
                    .ok_or(QueryEvaluationError::InternalError(
                        "Missing graph name".to_owned(),
                    ))
            })
            .and_then(|g| match g {
                Term::NamedNode(nnode) => Ok(NamedOrBlankNode::from(nnode)),
                Term::BlankNode(bnode) => Ok(NamedOrBlankNode::from(bnode)),
                Term::Literal(_) => Err(QueryEvaluationError::InternalError(
                    "Graph name was a literal.".to_owned(),
                )),
            });
        Poll::Ready(Some(graph_name))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
