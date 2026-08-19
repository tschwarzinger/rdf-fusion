#![cfg(test)]
#![allow(clippy::panic_in_result_fn)]

use crate::store::create_store_for_result;
use deltalake::arrow::util::pretty::pretty_format_batches;
use futures::StreamExt;
use insta::assert_snapshot;
use rdf_fusion::common::vocab::{rdf, xsd};
use rdf_fusion::common::{GraphNameRef, LiteralRef, NamedNodeRef, QuadRef};
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::store::{RdfDumpOptions, Store};
use rdf_fusion_common::{
    BlankNode, GraphName, NamedNode, Quad, RdfDumpFormat, RdfFormat,
};
use rdf_fusion_encoding::{EncodingName, QuadStorageEncodingName};
use rdf_fusion_execution::sparql::QueryOptions;
use rdf_fusion_storage::rdf_files::RdfFileScanOptions;
use std::error::Error;
use tokio::fs::File;

const NUMBER_OF_TRIPLES: usize = 8;

fn quads(graph_name: impl Into<GraphNameRef<'static>>) -> Vec<QuadRef<'static>> {
    let graph_name = graph_name.into();
    let paris = NamedNodeRef::new_unchecked("http://www.wikidata.org/entity/Q90");
    let france = NamedNodeRef::new_unchecked("http://www.wikidata.org/entity/Q142");
    let city = NamedNodeRef::new_unchecked("http://schema.org/City");
    let name = NamedNodeRef::new_unchecked("http://schema.org/name");
    let country = NamedNodeRef::new_unchecked("http://schema.org/country");
    let population = NamedNodeRef::new_unchecked("http://schema.org/population");
    let start_date = NamedNodeRef::new_unchecked("http://schema.org/startDate");
    let url = NamedNodeRef::new_unchecked("http://schema.org/url");
    let postal_code = NamedNodeRef::new_unchecked("http://schema.org/postalCode");
    vec![
        QuadRef::new(paris, rdf::TYPE, city, graph_name),
        QuadRef::new(
            paris,
            name,
            LiteralRef::new_language_tagged_literal_unchecked("Paris", "fr"),
            graph_name,
        ),
        QuadRef::new(
            paris,
            name,
            LiteralRef::new_language_tagged_literal_unchecked(
                "la ville lumi\u{E8}re",
                "fr",
            ),
            graph_name,
        ),
        QuadRef::new(paris, country, france, graph_name),
        QuadRef::new(
            paris,
            population,
            LiteralRef::new_typed_literal("2000000", xsd::INTEGER),
            graph_name,
        ),
        QuadRef::new(
            paris,
            start_date,
            LiteralRef::new_typed_literal("-300", xsd::G_YEAR),
            graph_name,
        ),
        QuadRef::new(
            paris,
            url,
            LiteralRef::new_typed_literal("https://www.paris.fr/", xsd::ANY_URI),
            graph_name,
        ),
        QuadRef::new(
            paris,
            postal_code,
            LiteralRef::new_simple_literal("75001"),
            graph_name,
        ),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_load_graph() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;
    store
        .load_from_reader(
            File::open("../../examples/data/paris.ttl").await.unwrap(),
            RdfFileScanOptions::with_format(RdfFormat::Turtle),
        )
        .await?;
    for q in quads(GraphNameRef::DefaultGraph) {
        assert!(store.contains(q).await?);
    }
    store.validate().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_load_dataset() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;
    store
        .load_from_reader(
            File::open("../../examples/data/paris-graph.ttl")
                .await
                .unwrap(),
            RdfFileScanOptions::with_format(RdfFormat::TriG),
        )
        .await?;
    for q in quads(NamedNodeRef::new_unchecked(
        "http://www.wikidata.org/wiki/Special:EntityData/Q90",
    )) {
        assert!(store.contains(q).await?);
    }
    store.validate().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_load_graph_generates_new_blank_nodes() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;
    for _ in 0..2 {
        store
            .load_from_reader(
                "_:a <http://example.com/p> <http://example.com/p> .".as_bytes(),
                RdfFileScanOptions::with_format(RdfFormat::NTriples),
            )
            .await?;
    }
    assert_eq!(store.len().await?, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_dump_graph_and_then_query_dump() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;
    for q in quads(GraphNameRef::DefaultGraph) {
        store.insert(q).await?;
    }

    store
        .dump(
            "memory:///test.parquet".to_owned(),
            RdfDumpFormat::Parquet,
            RdfDumpOptions::default(),
        )
        .await?;

    let store = create_store_for_result(
        store.context().session_context().runtime_env(),
        "memory:///test.parquet",
        QuadStorageEncodingName::PlainTerm,
    )
    .await;
    assert_eq!(store.len().await?, NUMBER_OF_TRIPLES);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_dump_named_graph() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;
    for q in quads(NamedNodeRef::new_unchecked("http://example.com/g1")) {
        store.insert(q).await?;
    }

    store
        .dump(
            "memory:///test_named_dump/data.parquet".to_owned(),
            RdfDumpFormat::Parquet,
            RdfDumpOptions::default().with_graph(Some(
                NamedNode::new_unchecked("http://example.com/g1".to_string()).into(),
            )),
        )
        .await?;

    let store = create_store_for_result(
        store.context().session_context().runtime_env(),
        "memory:///test_named_dump/data.parquet",
        QuadStorageEncodingName::PlainTerm,
    )
    .await;
    assert_eq!(store.len().await?, NUMBER_OF_TRIPLES);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_dump_graph_with_no_quad_in_graph() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;
    for q in quads(NamedNodeRef::new_unchecked("http://example.com/g1")) {
        store.insert(q).await?;
        store.insert(q).await?;
    }

    store
        .dump(
            "memory:///test_empty_dump/data.parquet".to_owned(),
            RdfDumpFormat::Parquet,
            RdfDumpOptions::default().with_graph(Some(GraphName::DefaultGraph)),
        )
        .await?;

    let store = create_store_for_result(
        store.context().session_context().runtime_env(),
        "memory:///test_empty_dump/data.parquet",
        QuadStorageEncodingName::PlainTerm,
    )
    .await;
    assert_eq!(store.len().await?, 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_empty_store() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;
    let QueryResults::Solutions(result) =
        store.query("SELECT ?s WHERE { ?s ?p ?o }").await?
    else {
        panic!("Wrong query result failed");
    };

    let stream = result.into_record_batch_stream()?;
    let collected = stream.collect::<Vec<_>>().await;
    assert_eq!(collected.len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_construct_with_duplicate_triples() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;

    let quads = (0..5000).map(|_| {
        Quad::new(
            BlankNode::default(),
            NamedNode::new("http://example.com/iri#x").expect("IRI should be valid"),
            NamedNode::new("http://example.com/iri#y").expect("IRI should be valid"),
            GraphName::DefaultGraph,
        )
    });

    store.extend(quads).await?;
    let QueryResults::Graph(mut result) = store
        .query(
            r#"
                PREFIX : <http://example.com/iri#>
                CONSTRUCT {
                    :a :b :c
                }
                WHERE {
                    ?x ?y ?z
                }"#,
        )
        .await?
    else {
        panic!("Wrong query result failed");
    };

    let mut res = Vec::new();
    while let Some(quad) = result.next().await {
        res.push(quad);
    }
    assert_eq!(res.len(), 1);

    Ok(())
}

#[tokio::test]
async fn query_with_string_encoding_option() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;
    let ex = NamedNode::new_unchecked("http://example.com");
    let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
    store.insert(&quad).await?;

    let options = QueryOptions {
        output_encoding_name: Some(EncodingName::String),
        ..Default::default()
    };
    let (results, _) = store
        .explain_query_opt("SELECT ?s WHERE { ?s ?p ?o }", options)
        .await?;

    let QueryResults::Solutions(solutions) = results else {
        panic!("Expected QueryResults::Solutions");
    };

    let mut stream = solutions.into_record_batch_stream()?;
    let batch = stream.next().await.unwrap()?;
    assert_snapshot!(
        pretty_format_batches(&[batch])?,
        @"
    +----------------------+
    | s                    |
    +----------------------+
    | <http://example.com> |
    +----------------------+
    "
    );
    Ok(())
}
