use std::fmt::{Display, Formatter};
use std::str::FromStr;
use thiserror::Error;

/// A supported RDF or data format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RdfFormat {
    /// [N-Triples](https://www.w3.org/TR/n-triples/)
    NTriples,
    /// [N-Quads](https://www.w3.org/TR/n-quads/)
    NQuads,
    /// [Turtle](https://www.w3.org/TR/turtle/)
    Turtle,
    /// [TriG](https://www.w3.org/TR/trig/)
    TriG,
    /// [RDF/XML](https://www.w3.org/TR/rdf-syntax-grammar/)
    RdfXml,
    /// [Notation3](https://www.w3.org/TR/n3/)
    N3,
    /// [Apache Parquet](https://parquet.apache.org/)
    Parquet,
}

impl RdfFormat {
    /// Returns the format from its extension.
    pub fn from_extension(extension: &str) -> Option<Self> {
        match extension.to_lowercase().as_str() {
            "nt" => Some(Self::NTriples),
            "nq" => Some(Self::NQuads),
            "ttl" => Some(Self::Turtle),
            "trig" => Some(Self::TriG),
            "rdf" | "xml" => Some(Self::RdfXml),
            "n3" => Some(Self::N3),
            "parquet" => Some(Self::Parquet),
            _ => None,
        }
    }

    /// Returns the format from its media type.
    pub fn from_media_type(media_type: &str) -> Option<Self> {
        match media_type.split(';').next()?.trim().to_lowercase().as_str() {
            "application/n-triples" | "text/plain" => Some(Self::NTriples),
            "application/n-quads" => Some(Self::NQuads),
            "text/turtle" => Some(Self::Turtle),
            "application/trig" => Some(Self::TriG),
            "application/rdf+xml" | "xml" => Some(Self::RdfXml),
            "text/n3" | "application/n3" => Some(Self::N3),
            "application/parquet" => Some(Self::Parquet),
            _ => None,
        }
    }

    /// Returns the default file extension for this format.
    pub fn file_extension(&self) -> &'static str {
        match self {
            Self::NTriples => "nt",
            Self::NQuads => "nq",
            Self::Turtle => "ttl",
            Self::TriG => "trig",
            Self::RdfXml => "rdf",
            Self::N3 => "n3",
            Self::Parquet => "parquet",
        }
    }

    /// Returns the default media type for this format.
    pub fn media_type(&self) -> &'static str {
        match self {
            Self::NTriples => "application/n-triples",
            Self::NQuads => "application/n-quads",
            Self::Turtle => "text/turtle",
            Self::TriG => "application/trig",
            Self::RdfXml => "application/rdf+xml",
            Self::N3 => "text/n3",
            Self::Parquet => "application/parquet",
        }
    }

    /// Returns if this format supports [RDF datasets](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-dataset) and not only [RDF graphs](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-graph).
    pub fn supports_datasets(&self) -> bool {
        match self {
            Self::NQuads | Self::TriG | Self::Parquet => true,
            Self::NTriples | Self::Turtle | Self::RdfXml | Self::N3 => false,
        }
    }

    /// Returns if this format supports [named graphs](https://www.w3.org/TR/rdf11-concepts/#dfn-named-graph).
    pub fn supports_names(&self) -> bool {
        self.supports_datasets()
    }

    /// Maps this format to an [oxrdfio::RdfFormat] if possible.
    pub fn to_oxigraph(&self) -> Option<oxrdfio::RdfFormat> {
        match self {
            Self::NTriples => Some(oxrdfio::RdfFormat::NTriples),
            Self::NQuads => Some(oxrdfio::RdfFormat::NQuads),
            Self::Turtle => Some(oxrdfio::RdfFormat::Turtle),
            Self::TriG => Some(oxrdfio::RdfFormat::TriG),
            Self::RdfXml => Some(oxrdfio::RdfFormat::RdfXml),
            Self::N3 => Some(oxrdfio::RdfFormat::N3),
            Self::Parquet => None,
        }
    }
}

impl FromStr for RdfFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_extension(s)
            .or_else(|| Self::from_media_type(s))
            .ok_or_else(|| format!("Unknown RDF format: {s}"))
    }
}

impl Display for RdfFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.file_extension())
    }
}

#[derive(Debug, Error)]
#[error("Unsupported RDF format: {0}")]
pub struct UnsupportedRdfFormatError(oxrdfio::RdfFormat);

impl TryFrom<oxrdfio::RdfFormat> for RdfFormat {
    type Error = UnsupportedRdfFormatError;

    fn try_from(value: oxrdfio::RdfFormat) -> Result<Self, Self::Error> {
        Ok(match value {
            oxrdfio::RdfFormat::NTriples => Self::NTriples,
            oxrdfio::RdfFormat::NQuads => Self::NQuads,
            oxrdfio::RdfFormat::Turtle => Self::Turtle,
            oxrdfio::RdfFormat::TriG => Self::TriG,
            oxrdfio::RdfFormat::RdfXml => Self::RdfXml,
            oxrdfio::RdfFormat::N3 => Self::N3,
            format => return Err(UnsupportedRdfFormatError(format)),
        })
    }
}

impl TryFrom<RdfFormat> for oxrdfio::RdfFormat {
    type Error = String;

    fn try_from(value: RdfFormat) -> Result<Self, Self::Error> {
        value.to_oxigraph().ok_or_else(|| {
            "Parquet is not an RDF format supported by Oxigraph".to_string()
        })
    }
}
