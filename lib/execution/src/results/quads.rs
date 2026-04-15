use crate::results::QuerySolutionStream;
use crate::sparql::error::QueryEvaluationError;
use futures::{Stream, StreamExt, ready};
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term, Variable};
use sparesults::QuerySolution;
use std::collections::HashSet;
use std::pin::Pin;
use std::task::{Context, Poll};

/// An iterator returning quads
pub struct QuadStream {
    inner: QuerySolutionStream,
}

impl QuadStream {
    pub async fn try_collect_to_vec(mut self) -> Result<Vec<Quad>, QueryEvaluationError> {
        let mut result = Vec::new();
        while let Some(element) = self.next().await {
            result.push(element?);
        }
        Ok(result)
    }

    pub async fn try_collect_to_set(
        mut self,
    ) -> Result<HashSet<Quad>, QueryEvaluationError> {
        let mut result = HashSet::new();
        while let Some(element) = self.next().await {
            result.insert(element?);
        }
        Ok(result)
    }

    pub fn try_new(inner: QuerySolutionStream) -> Result<Self, String> {
        let variables = inner
            .variables()
            .iter()
            .map(Variable::as_str)
            .collect::<Vec<&str>>();
        if !matches!(
            variables.as_slice(),
            &[COL_GRAPH, COL_SUBJECT, COL_PREDICATE, COL_OBJECT]
        ) {
            return Err(String::from("Unexpected schema of solution stream"));
        }
        Ok(Self { inner })
    }
}

impl Stream for QuadStream {
    type Item = Result<Quad, QueryEvaluationError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let inner_poll = ready!(self.inner.poll_next_unpin(cx));
        match inner_poll {
            None => Poll::Ready(None),
            Some(inner_result) => {
                Poll::Ready(Some(inner_result.and_then(|solution| to_quad(&solution))))
            }
        }
    }
}

#[allow(
    clippy::expect_used,
    reason = "Schema already checked in QuadStream::try_new"
)]
fn to_quad(solution: &QuerySolution) -> Result<Quad, QueryEvaluationError> {
    let graph_name = to_graph_name(solution.get(COL_GRAPH))?;
    let subject = to_subject(
        solution
            .get(COL_SUBJECT)
            .expect("Subject not found")
            .clone(),
    )?;
    let predicate = to_predicate(
        solution
            .get(COL_PREDICATE)
            .expect("Predicate not found")
            .clone(),
    )?;
    let object = solution.get(COL_OBJECT).expect("Object not found").clone();
    Ok(Quad::new(subject, predicate, object, graph_name))
}

fn to_graph_name(term: Option<&Term>) -> Result<GraphName, QueryEvaluationError> {
    match term {
        None => Ok(GraphName::DefaultGraph),
        Some(Term::NamedNode(n)) => Ok(GraphName::from(n.clone())),
        Some(Term::BlankNode(n)) => Ok(GraphName::from(n.clone())),
        _ => QueryEvaluationError::internal(
            "Graph name has invalid value in quads.".into(),
        ),
    }
}

fn to_subject(term: Term) -> Result<NamedOrBlankNode, QueryEvaluationError> {
    match term {
        Term::NamedNode(n) => Ok(NamedOrBlankNode::from(n)),
        Term::BlankNode(n) => Ok(NamedOrBlankNode::from(n)),
        Term::Literal(_) => {
            QueryEvaluationError::internal("Subject has invalid value in quads.".into())
        }
    }
}

fn to_predicate(term: Term) -> Result<NamedNode, QueryEvaluationError> {
    match term {
        Term::NamedNode(n) => Ok(n),
        _ => QueryEvaluationError::internal(
            "Predicate has invalid value in quads.".to_owned(),
        ),
    }
}
