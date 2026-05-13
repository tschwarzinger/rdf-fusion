use crate::vocab::rs;
use crate::w3c::files::{W3CTestRuntime, guess_rdf_format};
use crate::w3c::report::{dataset_diff, format_diff};
use anyhow::{Result, bail};
use futures::StreamExt;
use rdf_fusion::common::dataset::CanonicalizationAlgorithm;
use rdf_fusion::common::vocab::*;
use rdf_fusion::common::*;
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::storage::rdf_files::{RdfFileSourceConfig, RdfParserOptions};
use rdf_fusion::store::Store;
use sparesults::QueryResultsFormat;
use std::collections::HashMap;
use std::fmt::Write;
use std::str::FromStr;

pub struct W3CTestUtils {
    runtime: W3CTestRuntime,
}

impl W3CTestUtils {
    pub fn new(runtime: W3CTestRuntime) -> Self {
        Self { runtime }
    }

    pub async fn load_sparql_query_result(
        &self,
        url: &str,
    ) -> Result<StaticQueryResults> {
        if let Some(format) = url
            .rsplit_once('.')
            .and_then(|(_, extension)| QueryResultsFormat::from_extension(extension))
        {
            StaticQueryResults::from_query_results(
                QueryResults::read(self.runtime.read_file(url).await?, format).await?,
                false,
            )
            .await
        } else {
            StaticQueryResults::from_graph(
                &self
                    .runtime
                    .load_graph(url, guess_rdf_format(url)?, false)
                    .await?,
            )
            .await
        }
    }

    pub async fn load_to_store(
        &self,
        url: &str,
        store: &Store,
        to_graph_name: impl Into<GraphName>,
    ) -> Result<()> {
        self.load_to_store_from_source(
            &RdfFileSourceConfig {
                url: url.to_string(),
                format: guess_rdf_format(url)?,
            },
            store,
            to_graph_name,
        )
        .await
    }

    pub async fn load_to_store_from_source(
        &self,
        source: &RdfFileSourceConfig,
        store: &Store,
        to_graph_name: impl Into<GraphName>,
    ) -> Result<()> {
        let to_graph_name = to_graph_name.into();
        let reader = self.runtime.read_file(&source.url).await?;
        store
            .load_from_reader(
                reader,
                RdfParserOptions {
                    format: source.format,
                    base_iri: Some(source.url.parse()?),
                    rename_blank_nodes: false,
                    default_graph: Some(to_graph_name),
                    without_named_graphs: false,
                },
            )
            .await?;
        Ok(())
    }
}

async fn to_graph(result: QueryResults, with_order: bool) -> Result<Graph> {
    Ok(match result {
        QueryResults::Graph(mut graph) => graph.collect_as_graph().await?,
        QueryResults::Boolean(value) => {
            let mut graph = Graph::new();
            let result_set = BlankNode::default();
            graph.insert(TripleRef::new(&result_set, rdf::TYPE, rs::RESULT_SET));
            graph.insert(TripleRef::new(
                &result_set,
                rs::BOOLEAN,
                &Literal::from(value),
            ));
            graph
        }
        QueryResults::Solutions(mut solutions) => {
            let mut graph = Graph::new();
            let result_set = BlankNode::default();
            graph.insert(TripleRef::new(&result_set, rdf::TYPE, rs::RESULT_SET));
            for variable in solutions.variables() {
                graph.insert(TripleRef::new(
                    &result_set,
                    rs::RESULT_VARIABLE,
                    LiteralRef::new_simple_literal(variable.as_str()),
                ));
            }
            let mut i = 0;
            while let Some(solution) = solutions.next().await {
                let solution = solution?;
                let solution_id = BlankNode::default();
                graph.insert(TripleRef::new(&result_set, rs::SOLUTION, &solution_id));
                for (variable, value) in solution.iter() {
                    let binding = BlankNode::default();
                    graph.insert(TripleRef::new(&solution_id, rs::BINDING, &binding));
                    graph.insert(TripleRef::new(&binding, rs::VALUE, value));
                    graph.insert(TripleRef::new(
                        &binding,
                        rs::VARIABLE,
                        LiteralRef::new_simple_literal(variable.as_str()),
                    ));
                }
                if with_order {
                    graph.insert(TripleRef::new(
                        &solution_id,
                        rs::INDEX,
                        &Literal::from(i128::from(i + 1)),
                    ));
                }
                i += 1;
            }
            graph
        }
    })
}

pub async fn are_query_results_isomorphic(
    expected: &StaticQueryResults,
    actual: QueryResults,
) -> bool {
    let with_order = if let StaticQueryResults::Solutions { ordered, .. } = &expected {
        *ordered
    } else {
        false
    };
    let actual = match StaticQueryResults::from_query_results(actual, with_order).await {
        Ok(actual) => actual,
        Err(_) => return false,
    };

    match (expected, actual) {
        (
            StaticQueryResults::Solutions {
                variables: expected_variables,
                solutions: expected_solutions,
                ordered,
            },
            StaticQueryResults::Solutions {
                variables: actual_variables,
                solutions: actual_solutions,
                ..
            },
        ) => {
            expected_variables == &actual_variables
                && expected_solutions.len() == actual_solutions.len()
                && if *ordered {
                    expected_solutions.iter().zip(actual_solutions).all(
                        |(expected_solution, actual_solution)| {
                            compare_solutions(expected_solution, &actual_solution)
                        },
                    )
                } else {
                    expected_solutions.iter().all(|expected_solution| {
                        actual_solutions.iter().any(|actual_solution| {
                            compare_solutions(expected_solution, actual_solution)
                        })
                    })
                }
        }
        (StaticQueryResults::Boolean(expected), StaticQueryResults::Boolean(actual)) => {
            *expected == actual
        }
        (StaticQueryResults::Graph(expected), StaticQueryResults::Graph(actual)) => {
            *expected == actual
        }
        _ => false,
    }
}

fn compare_solutions(expected: &[(Variable, Term)], actual: &[(Variable, Term)]) -> bool {
    let mut bnode_map = HashMap::new();
    expected.len() == actual.len()
        && expected.iter().zip(actual).all(
            move |(
                (expected_variable, expected_value),
                (actual_variable, actual_value),
            )| {
                expected_variable == actual_variable
                    && compare_terms(
                        expected_value.as_ref(),
                        actual_value.as_ref(),
                        &mut bnode_map,
                    )
            },
        )
}

fn compare_terms<'a>(
    expected: TermRef<'a>,
    actual: TermRef<'a>,
    bnode_map: &mut HashMap<BlankNodeRef<'a>, BlankNodeRef<'a>>,
) -> bool {
    match (expected, actual) {
        (TermRef::BlankNode(expected), TermRef::BlankNode(actual)) => {
            expected == *bnode_map.entry(actual).or_insert(expected)
        }
        (expected, actual) => {
            if expected == actual {
                return true;
            }

            let value_lhs = TypedValueRef::try_from(expected);
            let value_rhs = TypedValueRef::try_from(actual);
            if let (Ok(value_lhs), Ok(value_rhs)) = (value_lhs, value_rhs) {
                value_lhs == value_rhs
            } else {
                // If these are ill-formed literals, they must be the same term to match
                // TODO: Check if this is standard conform
                false
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub enum StaticQueryResults {
    Graph(Graph),
    Solutions {
        variables: Vec<Variable>,
        solutions: Vec<Vec<(Variable, Term)>>,
        ordered: bool,
    },
    Boolean(bool),
}

impl StaticQueryResults {
    pub async fn from_query_results(
        results: QueryResults,
        with_order: bool,
    ) -> Result<Self> {
        Self::from_graph(&to_graph(results, with_order).await?).await
    }

    pub async fn from_graph(graph: &Graph) -> Result<Self> {
        if let Some(result_set) =
            graph.subject_for_predicate_object(rdf::TYPE, rs::RESULT_SET)
        {
            if let Some(bool) =
                graph.object_for_subject_predicate(result_set, rs::BOOLEAN)
            {
                // Boolean query
                Ok(Self::Boolean(bool == Literal::from(true).as_ref().into()))
            } else {
                // Regular query
                let mut variables: Vec<Variable> = graph
                    .objects_for_subject_predicate(result_set, rs::RESULT_VARIABLE)
                    .map(|object| {
                        let TermRef::Literal(l) = object else {
                            bail!("Invalid rs:resultVariable: {object}")
                        };
                        Ok(Variable::new_unchecked(l.value()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                variables.sort();

                let mut solutions = graph
                    .objects_for_subject_predicate(result_set, rs::SOLUTION)
                    .map(|object| {
                        let TermRef::BlankNode(solution) = object else {
                            bail!("Invalid rs:solution: {object}")
                        };
                        let mut bindings = graph
                            .objects_for_subject_predicate(solution, rs::BINDING)
                            .map(|object| {
                                let TermRef::BlankNode(binding) = object else {
                                    bail!("Invalid rs:binding: {object}")
                                };
                                let (Some(TermRef::Literal(variable)), Some(value)) = (
                                    graph.object_for_subject_predicate(
                                        binding,
                                        rs::VARIABLE,
                                    ),
                                    graph
                                        .object_for_subject_predicate(binding, rs::VALUE),
                                ) else {
                                    bail!("Invalid rs:binding: {binding}")
                                };
                                Ok((
                                    Variable::new_unchecked(variable.value()),
                                    value.into_owned(),
                                ))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        bindings.sort_by(|(a, _), (b, _)| a.cmp(b));
                        let index = graph
                            .object_for_subject_predicate(solution, rs::INDEX)
                            .map(|object| {
                                let TermRef::Literal(l) = object else {
                                    bail!("Invalid rs:index: {object}")
                                };
                                Ok(u64::from_str(l.value())?)
                            })
                            .transpose()?;
                        Ok((bindings, index))
                    })
                    .collect::<Result<Vec<_>>>()?;
                solutions.sort_by_key(|(_, index)| *index);

                let ordered = solutions.iter().all(|(_, index)| index.is_some());

                Ok(Self::Solutions {
                    variables,
                    solutions: solutions
                        .into_iter()
                        .map(|(solution, _)| solution)
                        .collect(),
                    ordered,
                })
            }
        } else {
            let mut graph = graph.clone();
            graph.canonicalize(CanonicalizationAlgorithm::Unstable);
            Ok(Self::Graph(graph))
        }
    }
}

pub async fn results_diff(expected: StaticQueryResults, actual: QueryResults) -> String {
    let with_order = if let StaticQueryResults::Solutions { ordered, .. } = &expected {
        *ordered
    } else {
        false
    };
    let actual = match StaticQueryResults::from_query_results(actual, with_order).await {
        Ok(actual) => actual,
        Err(e) => return format!("Failure to parse actual results: {e}"),
    };

    match expected {
        StaticQueryResults::Solutions {
            variables: mut expected_variables,
            solutions: expected_solutions,
            ordered,
        } => match actual {
            StaticQueryResults::Solutions {
                variables: mut actual_variables,
                solutions: actual_solutions,
                ..
            } => {
                let mut out = String::new();
                expected_variables.sort_unstable();
                actual_variables.sort_unstable();
                if expected_variables != actual_variables {
                    write!(
                        &mut out,
                        "Variables diff:\n{}",
                        format_diff(
                            &expected_variables
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("\n"),
                            &actual_variables
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("\n"),
                            "variables",
                        )
                    )
                    .unwrap();
                }
                write!(
                    &mut out,
                    "Solutions diff:\n{}",
                    format_diff(
                        &solutions_to_string(expected_solutions, ordered),
                        &solutions_to_string(actual_solutions, ordered),
                        "solutions",
                    )
                )
                .unwrap();
                out
            }
            StaticQueryResults::Boolean(actual) => {
                format!("Expecting solutions but found the boolean {actual}")
            }
            StaticQueryResults::Graph(actual) => {
                format!("Expecting solutions but found the graph:\n{actual}")
            }
        },
        StaticQueryResults::Graph(expected) => match actual {
            StaticQueryResults::Solutions { .. } => {
                "Expecting a graph but found solutions".into()
            }
            StaticQueryResults::Boolean(actual) => {
                format!("Expecting a graph but found the boolean {actual}")
            }
            StaticQueryResults::Graph(actual) => {
                let expected = expected
                    .into_iter()
                    .map(|t| t.in_graph(GraphNameRef::DefaultGraph))
                    .collect();
                let actual = actual
                    .into_iter()
                    .map(|t| t.in_graph(GraphNameRef::DefaultGraph))
                    .collect();
                dataset_diff(&expected, &actual)
            }
        },
        StaticQueryResults::Boolean(expected) => match actual {
            StaticQueryResults::Solutions { .. } => {
                "Expecting a boolean but found solutions".into()
            }
            StaticQueryResults::Boolean(actual) => {
                format!("Expecting {expected} but found {actual}")
            }
            StaticQueryResults::Graph(actual) => {
                format!("Expecting solutions but found the graph:\n{actual}")
            }
        },
    }
}

fn solutions_to_string(solutions: Vec<Vec<(Variable, Term)>>, ordered: bool) -> String {
    let mut lines = solutions
        .into_iter()
        .map(|mut s| {
            let mut out = String::new();
            out.write_str("{").unwrap();
            s.sort_unstable_by(|(v1, _), (v2, _)| v1.cmp(v2));
            for (variable, value) in s {
                write!(&mut out, "{variable} = {value} ").unwrap();
            }
            out.write_str("}").unwrap();
            out
        })
        .collect::<Vec<_>>();
    if !ordered {
        lines.sort_unstable();
    }
    lines.join("\n")
}
