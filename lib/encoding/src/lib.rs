#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo-thumbnail.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! Contains the [RDF Funion's](https://docs.rs/rdf-fusion/) term encodings.
//!
//! # Overview
//!
//! RDF term encodings allow us to bridge the gap between the [Resource Description Framework](https://www.w3.org/TR/rdf11-concepts/)
//! and the Arrow type system. Because there is no single best way to represent RDF terms in Arrow,
//! RDF Fusion supports multiple encodings. The [documentation of the main crate](https://docs.rs/rdf-fusion/latest/rdf_fusion/#sparql-on-top-of-datafusion)
//! provides some further details on this aspect.
//!
//! The following table provides an overview of the supported encodings.
//!
//! |                                                             | **Suitable For**           | **Requirements**  | **Term Identity** | **Comment**                   |
//! |-------------------------------------------------------------|----------------------------|-------------------|-------------------|-------------------------------|
//! | [**String Encoding**](string::StringEncoding)               | Simplicity                 | -                 | Yes               |                               |
//! | [**Plain Term**](plain_term::PlainTermEncoding)             | Processing literal terms   | -                 | Yes               | Result visible to users       |
//! | [**Object ID**](object_id::ObjectIdEncoding)                | Joining solution sets      | Object ID Mapping | Yes               | Must be decoded at some point |
//! | [**Typed Family**](typed_family::TypedFamilyEncoding)       | Arithmetic and comparisons | -                 | No                |                               |
//!
//! # Encoding Trait
//!
//! All of the above encodings must implement the [TermEncoding] trait. As a result, each encoding
//! must provide an [EncodingArray] and an [EncodingScalar]. These two types wrap regular Arrow
//! arrays (or scalars) that adhere to a particular encoding. If you want to pass an array to
//! a function that is guaranteed to be of a certain encoding, use these data types.

pub mod compute;
mod encoding;
mod encoding_name;
mod encodings;
mod extension_type;
pub mod object_id;
pub mod plain_term;
mod quad_storage_encoding;
mod scalar_encoder;
pub mod string;
pub mod typed_family;

use crate::object_id::ObjectIdArgs;
use crate::plain_term::{PlainTermArgs, PlainTermQuadsBuilder};
use crate::string::StringArgs;
use crate::typed_family::TypedFamilyArgs;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Field;
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::prelude::{SessionContext, col};
pub use encoding::*;
pub use encoding_name::*;
pub use encodings::*;
pub use extension_type::*;
pub use quad_storage_encoding::*;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{DFResult, Quad};
pub use scalar_encoder::ScalarEncoder;
use std::sync::Arc;

/// Represents a list of arrays that share the same encoding.
pub enum DowncastEncodingArgs {
    /// Arrays of the Object ID encoding
    ObjectId(ObjectIdArgs),
    /// Arrays of the Plain Term encoding
    PlainTerm(PlainTermArgs),
    /// Arrays of the Typed Family encoding
    TypedFamily(TypedFamilyArgs),
    /// Arrays of the String encoding
    String(StringArgs),
}

impl DowncastEncodingArgs {
    /// Tries to create a [`DowncastEncodingArgs`] from a list or arrays.
    pub fn try_from_arrays(
        encodings: &RdfFusionEncodings,
        args: &ScalarFunctionArgs,
    ) -> DFResult<Option<DowncastEncodingArgs>> {
        let Some(encoding_name) =
            detect_encoding_from_fields(encodings, &args.arg_fields)?
        else {
            return Ok(None);
        };

        let result = match encoding_name {
            EncodingName::ObjectId => {
                let encoding = encodings
                    .object_id()
                    .expect("Otherwise encoding cannot be detected");
                let arrays = try_from_arrays_for_encoding(encoding, args)?
                    .into_iter()
                    .map(|d| d.to_array(args.number_rows))
                    .collect::<Vec<_>>();
                DowncastEncodingArgs::ObjectId(ObjectIdArgs::new_unchecked(arrays))
            }
            EncodingName::PlainTerm => {
                let arrays = try_from_arrays_for_encoding(encodings.plain_term(), args)?
                    .into_iter()
                    .map(|d| d.to_array(args.number_rows))
                    .collect::<Vec<_>>();
                DowncastEncodingArgs::PlainTerm(PlainTermArgs::new_unchecked(arrays))
            }
            EncodingName::TypedFamily => {
                let arrays =
                    try_from_arrays_for_encoding(encodings.typed_family(), args)?;
                DowncastEncodingArgs::TypedFamily(TypedFamilyArgs::new_unchecked(
                    args.number_rows,
                    arrays,
                ))
            }
            EncodingName::String => {
                let arrays =
                    try_from_arrays_for_encoding(encodings.string_encoding(), args)?
                        .into_iter()
                        .map(|d| d.to_array(args.number_rows))
                        .collect::<Vec<_>>();
                DowncastEncodingArgs::String(StringArgs::new_unchecked(arrays))
            }
        };
        return Ok(Some(result));

        /// Converts the arrays for a particular encoding.
        fn try_from_arrays_for_encoding<TEncoding: TermEncoding>(
            encoding: &Arc<TEncoding>,
            args: &ScalarFunctionArgs,
        ) -> DFResult<Vec<EncodingDatum<TEncoding>>> {
            args.args
                .iter()
                .map(|a| match a {
                    ColumnarValue::Array(array) => Ok(EncodingDatum::Array(
                        encoding.try_new_array(Arc::clone(array))?,
                    )),
                    ColumnarValue::Scalar(scalar) => Ok(EncodingDatum::Scalar(
                        encoding.try_new_scalar(scalar.clone())?,
                    )),
                })
                .collect()
        }
    }
}

/// Detects the encoding of the given argument fields.
///
/// The encoding is obtained from the extension type of each field (see
/// [RdfFusionEncodings::try_get_encoding_name]). This function verifies that all arguments have the
/// same encoding and returns the name of the encoding. Returns `Ok(None)` if there are no
/// arguments.
pub fn detect_encoding_from_fields(
    encodings: &RdfFusionEncodings,
    arg_fields: &[impl AsRef<Field>],
) -> DFResult<Option<EncodingName>> {
    let Some(first_field) = arg_fields.first() else {
        return Ok(None);
    };

    let first_field = first_field.as_ref();
    let encoding_name =
        encodings
            .try_get_encoding_name(first_field)
            .ok_or_else(|| {
                plan_datafusion_err!(
                    "Cannot extract RDF term encoding from argument '{}' ({}).",
                    first_field.name(),
                    first_field.data_type()
                )
            })?;

    // Verify that all arguments have the same encoding
    for (i, arg_field) in arg_fields.iter().enumerate().skip(1) {
        let other_encoding = encodings.try_get_encoding_name(arg_field.as_ref());
        if other_encoding != Some(encoding_name) {
            return plan_err!(
                "Arguments have different encodings at index 0 and {i}: {encoding_name:?} and {other_encoding:?}"
            );
        }
    }

    Ok(Some(encoding_name))
}

/// Creates a [`RecordBatch`] from the given quads using the plain term encoding.
pub fn quads_to_plain_term_record_batch(quads: &[Quad]) -> RecordBatch {
    let schema = QuadStorageEncoding::PlainTerm.quad_schema();

    let mut builder = PlainTermQuadsBuilder::with_capacity(quads.len());
    for quad in quads {
        builder.append_quad(quad.as_ref());
    }
    let pt_quads = builder.finish();

    let arrays = vec![
        pt_quads.graphs.into_array_ref(),
        pt_quads.subjects.into_array_ref(),
        pt_quads.predicates.into_array_ref(),
        pt_quads.objects.into_array_ref(),
    ];

    RecordBatch::try_new(Arc::clone(schema.inner()), arrays)
        .expect("Failed to create RecordBatch")
}

/// Creates a [`DataFrame`] from the given quads using the plain term encoding.
pub fn quads_to_plain_term_dataframe(
    session: &SessionContext,
    quads: &[Quad],
) -> DataFrame {
    let batch = quads_to_plain_term_record_batch(quads);
    session
        .read_batch(batch)
        .expect("Failed to read batch into DataFrame")
        .select([
            col(COL_GRAPH).alias(COL_GRAPH),
            col(COL_SUBJECT).alias(COL_SUBJECT),
            col(COL_PREDICATE).alias(COL_PREDICATE),
            col(COL_OBJECT).alias(COL_OBJECT),
        ])
        .expect("Valid projection")
}
