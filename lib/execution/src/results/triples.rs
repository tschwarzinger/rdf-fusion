use crate::results::QuerySolutionStream;
use crate::sparql::error::QueryEvaluationError;
use futures::{Stream, StreamExt};
use rdf_fusion_common::{BlankNode, Graph, Term, TermPattern, Triple, TriplePattern};
use sparesults::QuerySolution;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

/// A stream over the triples that compose a graph solution.
pub struct QueryTripleStream {
    template: Vec<TriplePattern>,
    inner: QuerySolutionStream,

    buffered_results: Vec<Result<Triple, QueryEvaluationError>>,
    already_emitted_results: HashSet<Triple>,
    bnodes: HashMap<BlankNode, BlankNode>,
}

impl QueryTripleStream {
    pub fn new(template: Vec<TriplePattern>, inner: QuerySolutionStream) -> Self {
        Self {
            template,
            inner,
            buffered_results: vec![],
            already_emitted_results: HashSet::new(),
            bnodes: HashMap::new(),
        }
    }

    pub async fn collect_as_graph(&mut self) -> Result<Graph, QueryEvaluationError> {
        let mut graph = Graph::new();
        while let Some(triple) = self.next().await {
            let triple = triple?;
            graph.insert(triple.as_ref());
        }
        Ok(graph)
    }

    fn poll_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Triple, QueryEvaluationError>>> {
        // Keep polling until we have results.
        let result = loop {
            // If we already have a result buffered, use that
            if let Some(result) = self.buffered_results.pop() {
                break result;
            }

            // If we do not have buffered results, create them
            let solution = match ready!(self.inner.poll_next_unpin(cx)) {
                None => return Poll::Ready(None),
                Some(Ok(solution)) => solution,
                Some(Err(error)) => return Poll::Ready(Some(Err(error))),
            };

            for template in &self.template {
                let subject = get_triple_template_value(
                    &template.subject,
                    &solution,
                    &mut self.bnodes,
                )
                .and_then(|t| t.try_into().ok());
                let predicate = get_triple_template_value(
                    &TermPattern::from(template.predicate.clone()),
                    &solution,
                    &mut self.bnodes,
                )
                .and_then(|t| t.try_into().ok());
                let object = get_triple_template_value(
                    &template.object,
                    &solution,
                    &mut self.bnodes,
                );

                if let (Some(subject), Some(predicate), Some(object)) =
                    (subject, predicate, object)
                {
                    let triple = Triple {
                        subject,
                        predicate,
                        object,
                    };
                    // We allocate new blank nodes for each solution,
                    // triples with blank nodes are likely to be new.
                    let new_triple = triple.subject.is_blank_node()
                        || triple.object.is_blank_node()
                        || self.already_emitted_results.insert(triple.clone());
                    if new_triple {
                        self.buffered_results.push(Ok(triple));
                    }
                }
            }
            self.bnodes.clear(); // We do not reuse blank nodes
        };

        Poll::Ready(Some(result))
    }
}

fn get_triple_template_value(
    selector: &TermPattern,
    tuple: &QuerySolution,
    bnodes: &mut HashMap<BlankNode, BlankNode>,
) -> Option<Term> {
    match selector {
        TermPattern::NamedNode(nn) => Some(Term::NamedNode(nn.clone())),
        TermPattern::BlankNode(bnode) => {
            if !bnodes.contains_key(bnode) {
                bnodes.insert(bnode.clone(), BlankNode::default());
            }
            Some(Term::BlankNode(bnodes[bnode].clone()))
        }
        TermPattern::Literal(term) => Some(Term::Literal(term.clone())),
        TermPattern::Variable(v) => tuple.get(v).cloned(),
    }
}

impl Stream for QueryTripleStream {
    type Item = Result<Triple, QueryEvaluationError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (min, max) = self.inner.size_hint();
        (
            min.saturating_mul(self.template.len()),
            max.map(|v| v.saturating_mul(self.template.len())),
        )
    }
}
