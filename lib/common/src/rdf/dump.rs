use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// A format for dumping RDF data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RdfDumpFormat {
    /// An RDF format supported by oxrdfio.
    Rdf(oxrdfio::RdfFormat),
    /// Apache Parquet.
    Parquet,
}

impl FromStr for RdfDumpFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.to_lowercase() == "parquet" {
            return Ok(Self::Parquet);
        }

        oxrdfio::RdfFormat::from_extension(s)
            .or_else(|| oxrdfio::RdfFormat::from_media_type(s))
            .map(Self::Rdf)
            .ok_or_else(|| format!("Unknown format: {s}"))
    }
}

impl From<oxrdfio::RdfFormat> for RdfDumpFormat {
    fn from(format: oxrdfio::RdfFormat) -> Self {
        Self::Rdf(format)
    }
}

impl Display for RdfDumpFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rdf(format) => write!(f, "{format}"),
            Self::Parquet => write!(f, "Parquet"),
        }
    }
}

impl RdfDumpFormat {
    /// A list of all supported dump formats.
    pub const LIST_ALL: &[Self] = &[
        Self::Rdf(oxrdfio::RdfFormat::NTriples),
        Self::Rdf(oxrdfio::RdfFormat::NQuads),
        Self::Rdf(oxrdfio::RdfFormat::Turtle),
        Self::Rdf(oxrdfio::RdfFormat::TriG),
        Self::Rdf(oxrdfio::RdfFormat::RdfXml),
        Self::Rdf(oxrdfio::RdfFormat::N3),
        Self::Parquet,
    ];

    /// Returns the file extension for this format.
    pub fn file_extension(&self) -> &'static str {
        match self {
            Self::Rdf(format) => format.file_extension(),
            Self::Parquet => "parquet",
        }
    }

    /// Returns whether this format supports datasets.
    pub fn supports_datasets(&self) -> bool {
        match self {
            Self::Rdf(format) => format.supports_datasets(),
            Self::Parquet => true,
        }
    }
}
