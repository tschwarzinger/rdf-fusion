use crate::plain_term::{PlainTermArray, PlainTermType};
use crate::sortable_term::{SortableTermArray, SortableTermArrayBuilder};
use crate::typed_family::families::{FamilyArray, TypeClaim, TypedFamily};
use crate::typed_family::{FamilyComparator, TypedFamilyId, make_null_aware_comparator};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::arrow::error::ArrowError;
use rdf_fusion_common::AResult;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

/// A catch-all family for literals that are not claimed by any other registered family. This
/// represents the "unknown" literal types.
///
/// # Layout
///
/// The layout of the unknown family is stored as a struct array with two fields:
/// - `values`: the array of string values
/// - `language`: the array of literal datatypes
///
/// ```text
/// ┌─────────────────────────────┐
/// │ Struct Array                │
/// │                             │
/// │   Value        Data Type    │
/// │  ┌──────────┐  ┌──────────┐ │
/// │  │ "42"     │  │"my:int"  │ │
/// │  │──────────│  │──────────│ │
/// │  │ "true"   │  │"my:bool" │ │
/// │  │──────────│  │──────────│ │
/// │  │ "1.23e4" │  │"my:float"│ │
/// │  └──────────┘  └──────────┘ │
/// └─────────────────────────────┘
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnknownFamily {}

/// The fields of the unknown family.
static FIELDS_UNKNOWN: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("datatype", DataType::Utf8, false),
    ])
});

static DATA_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(FIELDS_UNKNOWN.clone()));

static CLAIM: LazyLock<TypeClaim> = LazyLock::new(|| TypeClaim::UnknownLiterals);

impl UnknownFamily {
    /// The id of the typed value family.
    pub const FAMILY_ID: &'static str = "rdf-fusion.unknown";

    pub fn fields() -> &'static Fields {
        &FIELDS_UNKNOWN
    }
}

impl TypedFamily for UnknownFamily {
    type Array = UnknownFamilyArray;

    const FAMILY_ID: TypedFamilyId = TypedFamilyId::Unknown;

    fn data_type() -> &'static DataType {
        &DATA_TYPE
    }

    fn claim() -> &'static TypeClaim {
        &CLAIM
    }

    fn create_array_from_plain_term(
        array: &PlainTermArray,
    ) -> AResult<UnknownFamilyArray> {
        validate_input(array)?;

        let parts = array.as_parts();

        let res_array = Arc::new(StructArray::try_new(
            FIELDS_UNKNOWN.clone(),
            vec![
                Arc::new(parts.value.clone()),
                Arc::new(parts.data_type.clone()),
            ],
            None,
        )?) as ArrayRef;

        return Ok(UnknownFamilyArray::from_array_unchecked(res_array));

        /// Validates whether the input contains terms that are not claimed by this family.
        fn validate_input(array: &PlainTermArray) -> Result<(), ArrowError> {
            let parts = array.as_parts();

            if parts.struct_array.null_count() > 0 {
                return Err(ArrowError::InvalidArgumentError(
                    "Null value in PlainTermArray".to_string(),
                ));
            }

            for i in 0..parts.struct_array.len() {
                let term_type =
                    PlainTermType::try_from(parts.term_type.value(i)).unwrap();
                if term_type != PlainTermType::Literal {
                    return Err(ArrowError::InvalidArgumentError(
                        "Not a literal".to_string(),
                    ));
                }
            }
            Ok(())
        }
    }
}

impl Debug for UnknownFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(UnknownFamily::FAMILY_ID)
    }
}

/// A family-specific array for the [`UnknownFamily`].
#[derive(Debug, Clone)]
pub struct UnknownFamilyArray {
    array: ArrayRef,
}

impl UnknownFamilyArray {
    /// Creates a new [`UnknownFamilyArray`].
    pub fn try_new(values: StringArray, datatypes: StringArray) -> AResult<Self> {
        if values.len() != datatypes.len() {
            return Err(ArrowError::InvalidArgumentError(
                "Values and datatypes must have the same length".to_owned(),
            ));
        }

        Ok(Self {
            array: Arc::new(
                StructArray::try_new(
                    FIELDS_UNKNOWN.clone(),
                    vec![
                        Arc::new(values) as ArrayRef,
                        Arc::new(datatypes) as ArrayRef,
                    ],
                    None,
                )
                .expect("Valid struct array"),
            ) as ArrayRef,
        })
    }

    /// Returns a reference to the inner [`StructArray`].
    pub fn inner_ref(array: &ArrayRef) -> &StructArray {
        array.as_struct()
    }

    /// Returns a reference to the inner [`StructArray`].
    pub fn struct_array(&self) -> &StructArray {
        Self::inner_ref(&self.array)
    }

    /// Returns a reference to the inner [`StructArray`].
    pub fn inner(&self) -> &StructArray {
        Self::inner_ref(&self.array)
    }

    /// Returns a reference to the values array, downcasting the array.
    pub fn values(&self) -> &StringArray {
        self.struct_array().column(0).as_string()
    }

    /// Returns a reference to the values array.
    pub fn values_ref(&self) -> &ArrayRef {
        self.struct_array().column(0)
    }

    /// Returns a reference to the array of optional language tags, downcasting the array.
    pub fn data_types(&self) -> &StringArray {
        self.struct_array().column(1).as_string()
    }

    /// Returns a reference to the array of optional language tags.
    pub fn data_types_ref(&self) -> &ArrayRef {
        self.struct_array().column(1)
    }
}

impl FamilyArray for UnknownFamilyArray {
    type Family = UnknownFamily;

    fn from_array_unchecked(array: ArrayRef) -> Self {
        Self { array }
    }

    fn inner_ref(&self) -> &ArrayRef {
        &self.array
    }

    fn into_array_ref(self) -> ArrayRef {
        self.array
    }

    fn comparator(&self, other: &Self) -> Option<FamilyComparator> {
        let lhs_values = self.values().clone();
        let lhs_datatypes = self.data_types().clone();
        let lhs_nulls = self.null_buffer();

        let rhs_values = other.values().clone();
        let rhs_datatypes = other.data_types().clone();
        let rhs_nulls = other.null_buffer();

        let inner = Box::new(move |i, j| {
            if lhs_values.value(i) == rhs_values.value(j)
                && lhs_datatypes.value(i) == rhs_datatypes.value(j)
            {
                Some(Ordering::Equal)
            } else {
                None
            }
        });

        if lhs_nulls.null_count() > 0 || rhs_nulls.null_count() > 0 {
            Some(make_null_aware_comparator(lhs_nulls, rhs_nulls, inner))
        } else {
            Some(inner)
        }
    }

    fn pretty_print(&self) -> Result<StringArray, ArrowError> {
        Ok(self.values().clone())
    }

    fn effective_boolean_value(&self) -> Result<BooleanArray, ArrowError> {
        Ok(BooleanArray::new_null(self.inner_ref().len()))
    }

    fn literal_data_types(&self) -> Result<StringArray, ArrowError> {
        Ok(self.data_types().clone())
    }

    fn cast_to_plain_term_array(&self) -> Result<PlainTermArray, ArrowError> {
        let len = self.inner_ref().len();
        let values = self.pretty_print()?;
        let datatypes = self.literal_data_types()?;
        PlainTermArray::try_new_literals(
            values,
            datatypes,
            StringArray::new_null(len),
            None,
        )
    }

    fn cast_to_sortable_array(&self) -> Result<SortableTermArray, ArrowError> {
        let mut builder = SortableTermArrayBuilder::new(self.inner_ref().len());
        for i in 0..self.inner_ref().len() {
            if self.array.is_null(i) {
                builder.append_null();
            } else {
                builder.append_literal(rdf_fusion_common::LiteralRef::new_typed_literal(
                    self.values().value(i),
                    rdf_fusion_common::NamedNodeRef::new_unchecked(
                        self.data_types().value(i),
                    ),
                ));
            }
        }
        Ok(builder.finish().try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;

    #[test]
    fn test_unknown_family_pretty_print() {
        let array = Arc::new(
            StructArray::try_new(
                FIELDS_UNKNOWN.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["42"])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["my:int"])) as ArrayRef,
                ],
                None,
            )
            .unwrap(),
        ) as ArrayRef;

        let family_array = UnknownFamilyArray::from_array_unchecked(array);
        let pretty = family_array.pretty_print().unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("pretty", Arc::new(pretty) as ArrayRef)])
                .unwrap();
        let formatted = pretty_format_batches(&[batch]).unwrap().to_string();

        insta::assert_snapshot!(formatted, @r"
        +--------+
        | pretty |
        +--------+
        | 42     |
        +--------+");
    }
}
