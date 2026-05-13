use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use oxrdfio::{RdfFormat, RdfSerializer};
use rdf_fusion::common::vocab::rdf;
use rdf_fusion::common::{BlankNode, NamedNodeRef, TripleRef};
use rdf_fusion::execution::results::QueryResultsFormat;

mod sd {
    use rdf_fusion::common::NamedNodeRef;

    pub const SERVICE: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#Service",
    );

    pub const DEFAULT_ENTAILMENT_REGIME: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#defaultEntailmentRegime",
    );
    pub const ENDPOINT: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#endpoint",
    );
    pub const FEATURE: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#feature",
    );
    pub const RESULT_FORMAT: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#resultFormat",
    );
    pub const SUPPORTED_LANGUAGE: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#supportedLanguage",
    );

    pub const EMPTY_GRAPHS: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#EmptyGraphs",
    );
    pub const SPARQL_10_QUERY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#SPARQL10Query",
    );
    pub const SPARQL_11_QUERY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#SPARQL11Query",
    );
    pub const SPARQL_11_UPDATE: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#SPARQL11Update",
    );
    pub const UNION_DEFAULT_GRAPH: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "http://www.w3.org/ns/sparql-service-description#UnionDefaultGraph",
    );
}

#[derive(Eq, PartialEq, Clone, Copy)]
pub enum EndpointKind {
    Query,
    Update,
}

pub struct ServiceDescription {
    format: RdfFormat,
    description: String,
}

impl IntoResponse for ServiceDescription {
    fn into_response(self) -> Response {
        (
            StatusCode::OK,
            [(CONTENT_TYPE, self.format.media_type().to_owned())],
            self.description,
        )
            .into_response()
    }
}

pub fn generate_service_description(
    format: RdfFormat,
    kind: EndpointKind,
    union_default_graph: bool,
) -> ServiceDescription {
    let mut graph = Vec::new();
    let root = BlankNode::default();
    graph.push(TripleRef::new(&root, rdf::TYPE, sd::SERVICE));
    if matches!(
        format,
        RdfFormat::Turtle | RdfFormat::TriG | RdfFormat::N3 | RdfFormat::RdfXml
    ) {
        // Hack: we use the default base IRI ie. the IRI from which the file is served
        graph.push(TripleRef::new(
            &root,
            sd::ENDPOINT,
            NamedNodeRef::new_unchecked(""),
        ));
    }
    for language in match kind {
        EndpointKind::Query => [sd::SPARQL_10_QUERY, sd::SPARQL_11_QUERY].as_slice(),
        EndpointKind::Update => [sd::SPARQL_11_UPDATE].as_slice(),
    } {
        graph.push(TripleRef::new(&root, sd::SUPPORTED_LANGUAGE, *language));
    }
    if kind == EndpointKind::Query {
        for format in [
            QueryResultsFormat::Json,
            QueryResultsFormat::Xml,
            QueryResultsFormat::Csv,
            QueryResultsFormat::Tsv,
        ] {
            graph.push(TripleRef::new(
                &root,
                sd::RESULT_FORMAT,
                NamedNodeRef::new_unchecked(format.iri()),
            ));
        }
        for format in [
            RdfFormat::NTriples,
            RdfFormat::NQuads,
            RdfFormat::Turtle,
            RdfFormat::TriG,
            RdfFormat::N3,
            RdfFormat::RdfXml,
        ] {
            graph.push(TripleRef::new(
                &root,
                sd::RESULT_FORMAT,
                NamedNodeRef::new_unchecked(format.iri()),
            ));
        }
    }
    if kind == EndpointKind::Update {
        graph.push(TripleRef::new(&root, sd::FEATURE, sd::EMPTY_GRAPHS));
    }
    if union_default_graph {
        graph.push(TripleRef::new(&root, sd::FEATURE, sd::UNION_DEFAULT_GRAPH));
    }
    graph.push(TripleRef::new(
        &root,
        sd::DEFAULT_ENTAILMENT_REGIME,
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/entailment/Simple"),
    ));

    let mut serializer = RdfSerializer::from_format(format)
        .with_prefix("sd", "http://www.w3.org/ns/sparql-service-description#")
        .unwrap()
        .for_writer(Vec::new());
    for t in graph {
        serializer.serialize_triple(t).unwrap();
    }

    let description = String::from_utf8(serializer.finish().unwrap()).unwrap();
    ServiceDescription {
        format,
        description,
    }
}
