use futures::StreamExt;
use rdf_fusion::execution::results::{
    QueryResults, QueryResultsFormat, QueryResultsSerializer,
};
use rdf_fusion::store::Store;
use serde_json::Value;

mod bsbm;
mod windfarm;

async fn run_select_query(store: &Store, query: &str) -> String {
    let result = store.query(query).await.unwrap();
    let QueryResults::Solutions(mut solutions) = result else {
        panic!("Unexpected result format!")
    };

    let mut buffer = Vec::new();
    let mut serializer = QueryResultsSerializer::from_format(QueryResultsFormat::Json)
        .serialize_solutions_to_writer(&mut buffer, solutions.variables().to_vec())
        .unwrap();
    while let Some(solution) = solutions.next().await {
        let solution = solution.unwrap();
        serializer.serialize(solution.iter()).unwrap();
    }
    serializer.finish().unwrap();

    let raw_json = String::from_utf8(buffer).unwrap();
    let v: Value = serde_json::from_str(&raw_json).unwrap();
    serde_json::to_string_pretty(&v).unwrap()
}

async fn run_graph_result_query(store: &Store, query: &str) -> String {
    let result = store.query(query).await.unwrap();
    let QueryResults::Graph(mut solutions) = result else {
        panic!("Unexpected result format!")
    };

    let mut buffer = Vec::new();
    let mut serializer = oxrdfio::RdfSerializer::from_format(oxrdfio::RdfFormat::Turtle)
        .for_writer(&mut buffer);
    while let Some(solution) = solutions.next().await {
        let solution = solution.unwrap();
        serializer.serialize_triple(solution.as_ref()).unwrap();
    }
    serializer.finish().unwrap();

    String::from_utf8(buffer).unwrap()
}
