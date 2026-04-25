#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
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
//! | [**Plain Term Encoding**](plain_term::PlainTermEncoding)    | Processing literal terms   | -                 | Yes               | Result visible to users       |
//! | [**Object ID Encoding**](object_id::ObjectIdEncoding)       | Joining solution sets      | Object ID Mapping | Yes               | Must be decoded at some point |
//! | [**Typed Value Encoding**](typed_value::TypedValueEncoding) | Arithmetic and comparisons | -                 | No                |                               |
//!
//! # Encoding Trait
//!
//! All of the above encodings must implement the [TermEncoding] trait. As a result, each encoding
//! must provide an [EncodingArray] and an [EncodingScalar]. These two types wrap regular Arrow
//! arrays (or scalars) that adhere to a particular encoding. If you want to pass an array to
//! a function that is guaranteed to be of a certain encoding, use these data types.
//!
//! # Future Plans
//!
//! In the future, we would like that encodings are parameterizable. For example, this [GitHub issue](https://github.com/tobixdev/rdf-fusion/issues/50)
//! tracks the progress of allowing users to specify custom object id lengths. As these parameters
//! will influence what kind of arrays/scalars are valid instances of a given encoding. For example,
//! if the object id contains 4 bytes, an array with 6 bytes is not a valid values. This state needs
//! to be considered when validating arrays/scalars. Therefore, you should use [TermEncoding::try_new_array]
//! or [TermEncoding::try_new_scalar] for creating datum instances, as the static way of creating
//! them will no longer work at some point.

pub mod compute;
mod encoding;
mod encoding_name;
mod encodings;
pub mod object_id;
pub mod plain_term;
mod quad_storage_encoding;
mod scalar_encoder;
pub mod sortable_term;
pub mod string;
pub mod typed_family;

use crate::object_id::ObjectIdArgs;
use crate::plain_term::{PlainTermArgs, PlainTermQuadsBuilder};
use crate::string::StringArgs;
use crate::typed_family::TypedFamilyArgs;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_datafusion_err, plan_err};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::prelude::{SessionContext, col};
pub use encoding::*;
pub use encoding_name::*;
pub use encodings::*;
pub use quad_storage_encoding::*;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::{DFResult, Quad};
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
        let types = args
            .arg_fields
            .iter()
            .map(|a| a.data_type().clone())
            .collect::<Vec<_>>();
        let Some(encoding_name) = detect_encoding_from_types(encodings, &types)? else {
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
            EncodingName::Sortable => {
                return exec_err!(
                    "Sortable encoding is not supported in DowncastEncodingArrays."
                );
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

/// Detects the encoding of the given argument types.
///
/// This function verifies that all argument types have the same encoding and returns the name
/// of the encoding.
pub fn detect_encoding_from_types(
    encodings: &RdfFusionEncodings,
    arg_types: &[DataType],
) -> DFResult<Option<EncodingName>> {
    if arg_types.is_empty() {
        return Ok(None);
    }

    let first_arg_type = &arg_types[0];
    let encoding_name =
        encodings
            .try_get_encoding_name(first_arg_type)
            .ok_or_else(|| {
                plan_datafusion_err!("Cannot extract RDF term encoding from argument.")
            })?;

    // Verify that all arguments have the same encoding
    for (i, arg_type) in arg_types.iter().enumerate().skip(1) {
        let other_encoding = encodings.try_get_encoding_name(arg_type);
        if other_encoding != Some(encoding_name) {
            return plan_err!(
                "Arguments have different encodings at index 0 and {i}: {encoding_name:?} and {other_encoding:?}"
            );
        }
    }

    Ok(Some(encoding_name))
}

/// Creates a [`DataFrame`] from the given quads using the plain term encoding.
pub fn quads_to_plain_term_dataframe(
    session: &SessionContext,
    quads: &[Quad],
) -> DataFrame {
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

    let batch = RecordBatch::try_new(Arc::clone(schema.inner()), arrays)
        .expect("Failed to create RecordBatch");
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
