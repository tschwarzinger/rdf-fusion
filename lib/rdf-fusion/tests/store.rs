#![cfg(test)]
#![allow(clippy::panic_in_result_fn)]

use futures::StreamExt;
use rdf_fusion::execution::ingest::RdfParserOptions;
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::model::vocab::{rdf, xsd};
use rdf_fusion::model::{GraphNameRef, LiteralRef, NamedNodeRef, QuadRef};
use rdf_fusion::store::Store;
use std::error::Error;

#[allow(clippy::non_ascii_literal)]
const DATA: &str = r#"
@prefix schema: <http://schema.org/> .
@prefix wd: <http://www.wikidata.org/entity/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

wd:Q90 a schema:City ;
    schema:name "Paris"@fr , "la ville lumière"@fr ;
    schema:country wd:Q142 ;
    schema:population 2000000 ;
    schema:startDate "-300"^^xsd:gYear ;
    schema:url "https://www.paris.fr/"^^xsd:anyURI ;
    schema:postalCode "75001" .
"#;

#[allow(clippy::non_ascii_literal)]
const GRAPH_DATA: &str = r#"
@prefix schema: <http://schema.org/> .
@prefix wd: <http://www.wikidata.org/entity/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

GRAPH <http://www.wikidata.org/wiki/Special:EntityData/Q90> {
    wd:Q90 a schema:City ;
        schema:name "Paris"@fr , "la ville lumière"@fr ;
        schema:country wd:Q142 ;
        schema:population 2000000 ;
        schema:startDate "-300"^^xsd:gYear ;
        schema:url "https://www.paris.fr/"^^xsd:anyURI ;
        schema:postalCode "75001" .
}
"#;
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
            DATA.as_bytes(),
            RdfParserOptions::with_format(RdfFormat::Turtle),
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
            GRAPH_DATA.as_bytes(),
            RdfParserOptions::with_format(RdfFormat::TriG),
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
                RdfParserOptions::with_format(RdfFormat::NTriples),
            )
            .await?;
    }
    assert_eq!(store.len().await?, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_dump_graph() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;
    for q in quads(GraphNameRef::DefaultGraph) {
        store.insert(q).await?;
    }

    let mut buffer = Vec::new();
    store
        .dump_graph_to_writer(
            GraphNameRef::DefaultGraph,
            RdfFormat::NTriples,
            &mut buffer,
        )
        .await?;
    assert_eq!(
        buffer.into_iter().filter(|c| *c == b'\n').count(),
        NUMBER_OF_TRIPLES
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_dump_dataset() -> Result<(), Box<dyn Error>> {
    let store = Store::new_in_memory().await;
    for q in quads(GraphNameRef::DefaultGraph) {
        store.insert(q).await?;
    }

    let buffer = store.dump_to_writer(RdfFormat::NQuads, Vec::new()).await?;
    assert_eq!(
        buffer.into_iter().filter(|c| *c == b'\n').count(),
        NUMBER_OF_TRIPLES
    );
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
