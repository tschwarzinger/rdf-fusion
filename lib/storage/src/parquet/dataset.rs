//! The description of a Parquet dataset (`dataset.ttl`).
//!
//! A Parquet dataset stores the same quads in one or more quad tables, each sorted in a different
//! order (e.g., GPOS and GSPO). Each quad table is a directory of Parquet files:
//!
//! ```text
//! <dataset>/dataset.ttl
//! <dataset>/gpos/<write id>_0.parquet
//! <dataset>/gpos/<write id>_1.parquet
//! <dataset>/gspo/<write id>_0.parquet
//! ```
//!
//! The `dataset.ttl` file describes the dataset in RDF (Turtle). Besides the quad tables, it lists
//! all files with their size and statistics (number of rows and, per column, the null count and
//! the minimum and maximum value). Hence, a reader does not need to list the directory (which
//! static file hosts usually do not support) and can skip files that cannot match a pattern
//! without opening them. All IRIs in the file are relative to the file itself, such that the
//! dataset can be moved.
//!
//! ```turtle
//! @prefix rfp: <https://rdf-fusion.org/spec/dataset#> .
//!
//! <> a rfp:Dataset ;
//!     rfp:formatVersion 1 ;
//!     rfp:encoding "String" ;
//!     rfp:quadTable <gpos/> .
//!
//! <gpos/> a rfp:QuadTable ;
//!     rfp:sortOrder "Native(GPOS)" ;
//!     rfp:file <gpos/part-00000.parquet> .
//!
//! <gpos/part-00000.parquet> a rfp:File ;
//!     rfp:sizeInBytes 1234 ;
//!     rfp:writtenAt "2026-10-25T12:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
//!     rfp:numRows 10 ;
//!     rfp:columnStatistics [
//!         rfp:column "predicate" ;
//!         rfp:nullCount 0 ;
//!         rfp:minValue "<http://example.org/a>" ;
//!         rfp:minValueExact true ;
//!         rfp:maxValue "<http://example.org/b>" ;
//!         rfp:maxValueExact true
//!     ] .
//! ```
//!
//! The size of a file and the time it was written identify the version of the file in caches
//! (e.g., of the footer). Neither is compared with the file in the object store, such that the
//! dataset can be copied (which usually changes the modification time and the ETag of the files).
//! However, a file that is rewritten at the same path receives a new `rfp:writtenAt`, such that
//! stale cache entries are not used.
//!
//! Minimum and maximum values are bounds of the values in the file. They are not exact if the
//! Parquet writer truncated them (see `statistics_truncate_length`).

use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::error::DataFusionError;
use datafusion::parquet::file::metadata::ParquetMetaData;
use object_store::ObjectMeta;
use object_store::path::Path;
use oxrdfio::{RdfFormat, RdfParser};
use rdf_fusion_common::vocab::xsd;
use rdf_fusion_common::{
    Graph, Literal, LiteralRef, NamedNode, NamedNodeRef, NamedOrBlankNodeRef,
    RdfSortOrder, TermRef, Triple,
};
use rdf_fusion_encoding::QuadStorageEncodingName;
use std::fmt::Write;
use std::sync::Arc;
use thiserror::Error;

/// The name of the file that describes a Parquet dataset.
pub const DATASET_DESCRIPTION_FILE: &str = "dataset.ttl";

/// The namespace of the vocabulary used in `dataset.ttl`.
pub const RDF_FUSION_PARQUET_NS: &str = "https://rdf-fusion.org/spec/dataset#";

/// The version of the dataset format that is written.
const FORMAT_VERSION: u64 = 1;

/// The description of a Parquet dataset. See the [module documentation](self) for details.
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetDatasetDescription {
    /// The encoding of the quads.
    pub encoding: QuadStorageEncodingName,
    /// The quad tables, each storing all quads of the dataset.
    pub quad_tables: Vec<ParquetQuadTableDescription>,
}

/// A quad table of a [`ParquetDatasetDescription`].
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetQuadTableDescription {
    /// The directory of the quad table, relative to the dataset (e.g., `gpos/`).
    pub directory: String,
    /// The order of the quads in the files. The files contain non-overlapping ranges of this
    /// order.
    pub sort_order: Option<RdfSortOrder>,
    /// The files of the quad table, ordered by their path.
    pub files: Vec<ParquetFileDescription>,
}

/// A file of a [`ParquetQuadTableDescription`].
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetFileDescription {
    /// The path of the file, relative to the dataset (e.g., `gpos/part-00000.parquet`).
    pub path: String,
    /// The size of the file in bytes.
    pub size: u64,
    /// The time the file was written, if known.
    pub written_at: Option<DateTime<Utc>>,
    /// The statistics of the file. The minimum and maximum values are bounds of the values in
    /// the file, even if they are not exact.
    pub statistics: Statistics,
}

impl ParquetFileDescription {
    /// Describes the file `object_meta`, which is part of the dataset at `directory`, using the
    /// statistics from its `metadata`.
    pub fn try_new(
        directory: &Path,
        object_meta: &ObjectMeta,
        metadata: &ParquetMetaData,
        schema: &Arc<Schema>,
    ) -> Result<Self, DataFusionError> {
        let location = &object_meta.location;
        let path = location
            .prefix_match(directory)
            .map(|parts| {
                parts
                    .map(|part| part.as_ref().to_owned())
                    .collect::<Vec<_>>()
            })
            .filter(|parts| !parts.is_empty())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{location} is not part of the dataset at {directory}"
                ))
            })?
            .join("/");
        let statistics =
            DFParquetMetadata::statistics_from_parquet_metadata(metadata, schema)?;
        Ok(Self {
            path,
            size: object_meta.size,
            written_at: Some(object_meta.last_modified),
            statistics,
        })
    }
}

/// An error while reading a `dataset.ttl` file.
#[derive(Debug, Error)]
#[error("Invalid Parquet dataset description: {0}")]
pub struct ParquetDatasetDescriptionError(String);

impl ParquetQuadTableDescription {
    /// Returns the directory name of a quad table with the given sort order (e.g., `gpos/` for
    /// `Native(GPOS)`).
    pub fn directory_for(sort_order: Option<&RdfSortOrder>) -> String {
        match sort_order {
            None => "quads/".to_owned(),
            Some(RdfSortOrder::NativeOrder(components)) => {
                format!("{}/", components_name(components))
            }
            Some(RdfSortOrder::SparqlOrder(components)) => {
                format!("sparql-{}/", components_name(components))
            }
        }
    }
}

fn components_name(components: &[rdf_fusion_common::QuadComponent]) -> String {
    components
        .iter()
        .map(|c| c.to_string().to_lowercase())
        .collect()
}

impl ParquetDatasetDescription {
    /// Serializes the description as Turtle.
    pub fn to_turtle(&self, schema: &Schema) -> String {
        let mut out = String::new();
        // Writing to a String cannot fail.
        let _ = self.write_turtle(&mut out, schema);
        out
    }

    fn write_turtle(&self, out: &mut String, schema: &Schema) -> std::fmt::Result {
        writeln!(out, "@prefix rfp: <{RDF_FUSION_PARQUET_NS}> .")?;
        writeln!(out)?;
        writeln!(out, "<> a rfp:Dataset ;")?;
        writeln!(out, "    rfp:formatVersion {FORMAT_VERSION} ;")?;
        write!(
            out,
            "    rfp:encoding {}",
            literal(&self.encoding.to_string())
        )?;
        for table in &self.quad_tables {
            write!(out, " ;\n    rfp:quadTable <{}>", table.directory)?;
        }
        writeln!(out, " .")?;

        for table in &self.quad_tables {
            writeln!(out)?;
            write!(out, "<{}> a rfp:QuadTable", table.directory)?;
            if let Some(sort_order) = &table.sort_order {
                write!(
                    out,
                    " ;\n    rfp:sortOrder {}",
                    literal(&sort_order.to_string())
                )?;
            }
            for file in &table.files {
                write!(out, " ;\n    rfp:file <{}>", file.path)?;
            }
            writeln!(out, " .")?;
        }

        for file in self.quad_tables.iter().flat_map(|t| &t.files) {
            writeln!(out)?;
            write_file(out, file, schema)?;
        }
        Ok(())
    }

    /// Parses a description from Turtle.
    ///
    /// `base_iri` is the IRI of the description file itself (e.g.,
    /// `s3://bucket/dataset/dataset.ttl`). The relative IRIs of the quad tables and files are
    /// resolved against it and must point into the directory of the dataset.
    pub fn from_turtle(
        turtle: &[u8],
        base_iri: &str,
        schema: &Schema,
    ) -> Result<Self, ParquetDatasetDescriptionError> {
        let parser = RdfParser::from_format(RdfFormat::Turtle)
            .with_base_iri(base_iri)
            .map_err(|e| ParquetDatasetDescriptionError(e.to_string()))?;
        let mut graph = Graph::new();
        for quad in parser.for_slice(turtle) {
            let quad = quad.map_err(|e| ParquetDatasetDescriptionError(e.to_string()))?;
            graph.insert(&Triple::from(quad));
        }
        let reader = DescriptionReader {
            graph: &graph,
            directory: base_iri
                .rsplit_once('/')
                .map_or(base_iri, |(directory, _)| directory),
        };

        let dataset = NamedNodeRef::new_unchecked(base_iri).into();
        let version = reader.integer(dataset, "formatVersion")?;
        if version != Some(FORMAT_VERSION) {
            return Err(ParquetDatasetDescriptionError(format!(
                "Unsupported format version {version:?}"
            )));
        }
        let encoding = reader
            .required_string(dataset, "encoding")?
            .parse()
            .map_err(|e| ParquetDatasetDescriptionError(format!("{e}")))?;

        let mut quad_tables = Vec::new();
        for table in reader.objects(dataset, "quadTable") {
            let sort_order = reader
                .string(table, "sortOrder")
                .map(|order| order.parse::<RdfSortOrder>())
                .transpose()
                .map_err(|e| ParquetDatasetDescriptionError(format!("{e}")))?;
            let mut files = reader
                .objects(table, "file")
                .into_iter()
                .map(|file| reader.file(file, schema))
                .collect::<Result<Vec<_>, _>>()?;
            // Turtle does not preserve the order of the objects.
            files.sort_by(|a, b| a.path.cmp(&b.path));
            quad_tables.push(ParquetQuadTableDescription {
                directory: reader.relative_path(table)?,
                sort_order,
                files,
            });
        }
        quad_tables.sort_by(|a, b| a.directory.cmp(&b.directory));

        Ok(Self {
            encoding,
            quad_tables,
        })
    }
}

fn write_file(
    out: &mut String,
    file: &ParquetFileDescription,
    schema: &Schema,
) -> std::fmt::Result {
    write!(
        out,
        "<{}> a rfp:File ;\n    rfp:sizeInBytes {}",
        file.path, file.size
    )?;
    if let Some(written_at) = file.written_at {
        // All digits of the fraction, such that the time is restored exactly.
        write!(
            out,
            " ;\n    rfp:writtenAt \"{}\"^^{}",
            written_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            xsd::DATE_TIME
        )?;
    }
    if let Precision::Exact(num_rows) = file.statistics.num_rows {
        write!(out, " ;\n    rfp:numRows {num_rows}")?;
    }
    if let Precision::Exact(total_byte_size) = file.statistics.total_byte_size {
        write!(out, " ;\n    rfp:totalByteSize {total_byte_size}")?;
    }
    for (field, stats) in schema
        .fields()
        .iter()
        .zip(&file.statistics.column_statistics)
    {
        let mut properties = Vec::new();
        if let Precision::Exact(null_count) = stats.null_count {
            properties.push(format!("rfp:nullCount {null_count}"));
        }
        for (name, value) in [
            ("minValue", &stats.min_value),
            ("maxValue", &stats.max_value),
        ] {
            if let Some((value, exact)) = string_bound(value) {
                properties.push(format!("rfp:{name} {}", literal(value)));
                properties.push(format!("rfp:{name}Exact {exact}"));
            }
        }
        if properties.is_empty() {
            continue;
        }
        write!(
            out,
            " ;\n    rfp:columnStatistics [\n        rfp:column {}",
            literal(field.name())
        )?;
        for property in properties {
            write!(out, " ;\n        {property}")?;
        }
        write!(out, "\n    ]")?;
    }
    writeln!(out, " .")
}

/// Returns the string value of a bound and whether it is exact. Only string columns (e.g., the
/// term columns of the string encoding) are supported.
fn string_bound(value: &Precision<ScalarValue>) -> Option<(&str, bool)> {
    let (value, exact) = match value {
        Precision::Exact(value) => (value, true),
        Precision::Inexact(value) => (value, false),
        Precision::Absent => return None,
    };
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some((value.as_str(), exact)),
        _ => None,
    }
}

fn literal(value: &str) -> String {
    Literal::new_simple_literal(value).to_string()
}

/// Returns the IRI of a property of the dataset vocabulary.
fn property(name: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{RDF_FUSION_PARQUET_NS}{name}"))
}

/// Reads the description from the parsed `dataset.ttl`.
struct DescriptionReader<'a> {
    graph: &'a Graph,
    /// The IRI of the directory of the dataset, without a trailing `/`.
    directory: &'a str,
}

impl<'a> DescriptionReader<'a> {
    fn objects(
        &self,
        subject: NamedOrBlankNodeRef<'_>,
        name: &str,
    ) -> Vec<NamedOrBlankNodeRef<'a>> {
        self.graph
            .objects_for_subject_predicate(subject, &property(name))
            .filter_map(|term| match term {
                TermRef::NamedNode(node) => Some(node.into()),
                TermRef::BlankNode(node) => Some(node.into()),
                _ => None,
            })
            .collect()
    }

    fn literal(
        &self,
        subject: NamedOrBlankNodeRef<'_>,
        name: &str,
    ) -> Option<LiteralRef<'a>> {
        match self
            .graph
            .object_for_subject_predicate(subject, &property(name))?
        {
            TermRef::Literal(literal) => Some(literal),
            _ => None,
        }
    }

    fn string(&self, subject: NamedOrBlankNodeRef<'_>, name: &str) -> Option<String> {
        self.literal(subject, name)
            .map(|literal| literal.value().to_owned())
    }

    fn required_string(
        &self,
        subject: NamedOrBlankNodeRef<'_>,
        name: &str,
    ) -> Result<String, ParquetDatasetDescriptionError> {
        self.string(subject, name).ok_or_else(|| {
            ParquetDatasetDescriptionError(format!("Missing rfp:{name} of {subject}"))
        })
    }

    fn integer(
        &self,
        subject: NamedOrBlankNodeRef<'_>,
        name: &str,
    ) -> Result<Option<u64>, ParquetDatasetDescriptionError> {
        self.literal(subject, name)
            .map(|literal| {
                literal.value().parse::<u64>().map_err(|e| {
                    ParquetDatasetDescriptionError(format!("Invalid rfp:{name}: {e}"))
                })
            })
            .transpose()
    }

    fn boolean(&self, subject: NamedOrBlankNodeRef<'_>, name: &str) -> bool {
        self.literal(subject, name)
            .is_some_and(|literal| literal.value() == "true")
    }

    fn file(
        &self,
        file: NamedOrBlankNodeRef<'_>,
        schema: &Schema,
    ) -> Result<ParquetFileDescription, ParquetDatasetDescriptionError> {
        let size = self.integer(file, "sizeInBytes")?.ok_or_else(|| {
            ParquetDatasetDescriptionError(format!("Missing rfp:sizeInBytes of {file}"))
        })?;
        let written_at = self
            .literal(file, "writtenAt")
            .map(|literal| {
                DateTime::parse_from_rfc3339(literal.value())
                    .map(|time| time.with_timezone(&Utc))
                    .map_err(|e| {
                        ParquetDatasetDescriptionError(format!(
                            "Invalid rfp:writtenAt: {e}"
                        ))
                    })
            })
            .transpose()?;
        let num_rows = self.integer(file, "numRows")?;
        let total_byte_size = self.integer(file, "totalByteSize")?;

        let mut column_statistics =
            vec![ColumnStatistics::new_unknown(); schema.fields().len()];
        for stats in self.objects(file, "columnStatistics") {
            let column = self.required_string(stats, "column")?;
            let Ok(index) = schema.index_of(&column) else {
                continue;
            };
            let null_count = self.integer(stats, "nullCount")?;
            let all_null = null_count.is_some() && null_count == num_rows;
            let bound = |name: &str| match self.string(stats, name) {
                Some(value) => {
                    let value = ScalarValue::Utf8(Some(value));
                    if self.boolean(stats, &format!("{name}Exact")) {
                        Precision::Exact(value)
                    } else {
                        Precision::Inexact(value)
                    }
                }
                // The bounds of a column that only contains nulls are null, as in the statistics
                // from the footers. The description cannot express them, but DataFusion requires
                // them, for example, to verify that the files are ordered.
                None if all_null => Precision::Exact(ScalarValue::Utf8(None)),
                None => Precision::Absent,
            };
            column_statistics[index] = ColumnStatistics {
                null_count: null_count
                    .map_or(Precision::Absent, |n| Precision::Exact(n as usize)),
                min_value: bound("minValue"),
                max_value: bound("maxValue"),
                ..ColumnStatistics::new_unknown()
            };
        }

        Ok(ParquetFileDescription {
            path: self.relative_path(file)?,
            size,
            written_at,
            statistics: Statistics {
                num_rows: num_rows
                    .map_or(Precision::Absent, |n| Precision::Exact(n as usize)),
                total_byte_size: total_byte_size
                    .map_or(Precision::Absent, |n| Precision::Exact(n as usize)),
                column_statistics,
            },
        })
    }

    /// Returns the path of `node` relative to the directory of the dataset.
    fn relative_path(
        &self,
        node: NamedOrBlankNodeRef<'_>,
    ) -> Result<String, ParquetDatasetDescriptionError> {
        let NamedOrBlankNodeRef::NamedNode(node) = node else {
            return Err(ParquetDatasetDescriptionError(
                "Quad tables and files must be IRIs".to_owned(),
            ));
        };
        node.as_str()
            .strip_prefix(self.directory)
            .and_then(|path| path.strip_prefix('/'))
            .filter(|path| !path.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                ParquetDatasetDescriptionError(format!(
                    "{node} is not within the directory of the dataset"
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdf_fusion_encoding::QuadStorageEncoding;

    const BASE_IRI: &str = "memory:///dataset/dataset.ttl";

    fn description() -> ParquetDatasetDescription {
        let schema = QuadStorageEncoding::String.quad_schema();
        let mut column_statistics =
            vec![ColumnStatistics::new_unknown(); schema.inner().fields().len()];
        column_statistics[2] = ColumnStatistics {
            null_count: Precision::Exact(0),
            min_value: Precision::Exact(ScalarValue::Utf8(Some(
                "<http://example.org/a>".to_owned(),
            ))),
            max_value: Precision::Inexact(ScalarValue::Utf8(Some(
                "\"with \\\"quotes\\\" and\nline breaks\"".to_owned(),
            ))),
            ..ColumnStatistics::new_unknown()
        };
        let file = ParquetFileDescription {
            path: "gpos/part-00000.parquet".to_owned(),
            size: 1234,
            written_at: Some(
                DateTime::parse_from_rfc3339("2026-10-25T12:34:56.123456789+02:00")
                    .unwrap()
                    .with_timezone(&Utc),
            ),
            statistics: Statistics {
                num_rows: Precision::Exact(10),
                total_byte_size: Precision::Exact(100),
                column_statistics,
            },
        };
        ParquetDatasetDescription {
            encoding: QuadStorageEncodingName::String,
            quad_tables: vec![
                ParquetQuadTableDescription {
                    directory: "gpos/".to_owned(),
                    sort_order: Some("NATIVE(GPOS)".parse().unwrap()),
                    files: vec![file.clone()],
                },
                ParquetQuadTableDescription {
                    directory: "gspo/".to_owned(),
                    sort_order: Some("NATIVE(GSPO)".parse().unwrap()),
                    files: vec![ParquetFileDescription {
                        path: "gspo/part-00000.parquet".to_owned(),
                        ..file
                    }],
                },
            ],
        }
    }

    #[test]
    fn description_round_trips_through_turtle() {
        let schema = QuadStorageEncoding::String.quad_schema();
        let description = description();
        let turtle = description.to_turtle(schema.inner());
        let parsed = ParquetDatasetDescription::from_turtle(
            turtle.as_bytes(),
            BASE_IRI,
            schema.inner(),
        )
        .unwrap();
        assert_eq!(parsed, description, "{turtle}");
    }

    #[test]
    fn rejects_unknown_format_versions() {
        let schema = QuadStorageEncoding::String.quad_schema();
        let turtle = description()
            .to_turtle(schema.inner())
            .replace("rfp:formatVersion 1", "rfp:formatVersion 99");
        assert!(
            ParquetDatasetDescription::from_turtle(
                turtle.as_bytes(),
                BASE_IRI,
                schema.inner(),
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_files_outside_of_the_dataset() {
        let schema = QuadStorageEncoding::String.quad_schema();
        let turtle = description()
            .to_turtle(schema.inner())
            .replace("<gpos/part-00000.parquet>", "<../other/part-00000.parquet>");
        assert!(
            ParquetDatasetDescription::from_turtle(
                turtle.as_bytes(),
                BASE_IRI,
                schema.inner(),
            )
            .is_err()
        );
    }

    #[test]
    fn directory_names() {
        assert_eq!(
            ParquetQuadTableDescription::directory_for(Some(
                &"NATIVE(GPOS)".parse().unwrap()
            )),
            "gpos/"
        );
        assert_eq!(
            ParquetQuadTableDescription::directory_for(Some(&"SPO".parse().unwrap())),
            "sparql-spo/"
        );
        assert_eq!(ParquetQuadTableDescription::directory_for(None), "quads/");
    }
}
