use crate::AppState;
use crate::error::RdfFusionServerError;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use headers::HeaderMapExt;
use headers_accept::Accept;
use mediatype::names::{
    APPLICATION, CSV, JSON, N_QUADS, N_TRIPLES, N3, PLAIN, TEXT, TRIG, TURTLE, XML,
};
use mediatype::{MediaType, Name};
use oxrdfio::RdfFormat;
use rdf_fusion::execution::results::QueryResultsFormat;

/// Handles the content-negotiation for requests that return RDF data.
impl FromRequestParts<AppState> for RdfFormat {
    type Rejection = RdfFusionServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        static MEDIA_TYPES: [MediaType<'_>; 8] = [
            MediaType::new(TEXT, PLAIN),
            MediaType::new(APPLICATION, N_QUADS),
            MediaType::new(APPLICATION, N_TRIPLES),
            MediaType::new(APPLICATION, TRIG),
            MediaType::new(APPLICATION, TURTLE),
            MediaType::new(APPLICATION, N3),
            MediaType::new(APPLICATION, XML),
            MediaType::from_parts(
                APPLICATION,
                Name::new_unchecked("rdf"),
                Some(Name::new_unchecked("xml")),
                &[],
            ),
        ];
        static DEFAULT_MEDIA_TYPE: MediaType<'_> = MediaType::new(APPLICATION, N_QUADS);

        let accept = parts.headers.typed_get::<Accept>();
        let media_type = content_negotiation(
            accept,
            &MEDIA_TYPES,
            &DEFAULT_MEDIA_TYPE,
            "application/turtle",
        )?;

        RdfFormat::from_media_type(media_type.to_string().as_str()).ok_or(
            RdfFusionServerError::BadRequest(format!(
                "Could not convert negotiated media type '{media_type}' to internal representation."
            )),
        )
    }
}

/// Handles the content-negotiation for requests that return query results.
impl FromRequestParts<AppState> for QueryResultsFormat {
    type Rejection = RdfFusionServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        static MEDIA_TYPES: [MediaType<'_>; 8] = [
            MediaType::new(TEXT, PLAIN),
            MediaType::new(TEXT, CSV),
            MediaType::new(TEXT, Name::new_unchecked("tsv")),
            MediaType::new(APPLICATION, JSON),
            MediaType::from_parts(
                APPLICATION,
                Name::new_unchecked("sparql-results"),
                Some(Name::new_unchecked("json")),
                &[],
            ),
            MediaType::from_parts(
                APPLICATION,
                Name::new_unchecked("sparql-results"),
                Some(Name::new_unchecked("xml")),
                &[],
            ),
            MediaType::new(APPLICATION, Name::new_unchecked("tab-separated-values")),
            MediaType::new(APPLICATION, XML),
        ];
        static DEFAULT_MEDIA_TYPE: MediaType<'_> = MediaType::new(APPLICATION, JSON);

        let accept = parts.headers.typed_get::<Accept>();
        let media_type = content_negotiation(
            accept,
            &MEDIA_TYPES,
            &DEFAULT_MEDIA_TYPE,
            "application/sparql-results+json or text/tsv",
        )?;

        QueryResultsFormat::from_media_type(media_type.to_string().as_str()).ok_or(
            RdfFusionServerError::BadRequest(format!(
                "Could not convert negotiated media type '{media_type}' to internal representation."
            )),
        )
    }
}

fn content_negotiation<'media>(
    accept: Option<Accept>,
    available: &'media [MediaType<'media>],
    default: &'media MediaType<'media>,
    example: &str,
) -> Result<MediaType<'media>, RdfFusionServerError> {
    let Some(accept) = accept else {
        return Ok(default.clone());
    };

    match accept.negotiate(available) {
        None => Err(RdfFusionServerError::ContentNegotiation(format!(
            "The accept header does not provide any accepted format like {example}."
        ))),
        Some(result) => Ok(result.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};
    use headers::HeaderMapExt;
    use headers_accept::Accept;

    static MEDIA_TYPES: [MediaType<'_>; 1] = [MediaType::new(APPLICATION, JSON)];
    static DEFAULT_MEDIA_TYPE: MediaType<'_> = MediaType::new(APPLICATION, JSON);

    #[test]
    fn test_content_negotiation_no_accept_returns_default() {
        let result = content_negotiation(
            None,
            &MEDIA_TYPES,
            &DEFAULT_MEDIA_TYPE,
            "application/json",
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MediaType::new(APPLICATION, JSON));
    }

    #[test]
    fn test_content_negotiation_with_match() {
        static MEDIA_TYPES: [MediaType<'_>; 1] = [MediaType::new(APPLICATION, JSON)];
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("application/json"));
        let accept = headers.typed_get::<Accept>();

        let result = content_negotiation(
            accept,
            &MEDIA_TYPES,
            &DEFAULT_MEDIA_TYPE,
            "application/json",
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MediaType::new(APPLICATION, JSON));
    }

    #[test]
    fn test_content_negotiation_with_no_match() {
        static MEDIA_TYPES: [MediaType<'_>; 1] = [MediaType::new(APPLICATION, JSON)];
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("application/xml"));
        let accept = headers.typed_get::<Accept>();

        let result = content_negotiation(
            accept,
            &MEDIA_TYPES,
            &DEFAULT_MEDIA_TYPE,
            "application/json",
        );

        assert!(result.is_err());
    }
}
