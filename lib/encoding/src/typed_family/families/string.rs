use crate::plain_term::{PlainTermArray, PlainTermType};
use crate::sortable_term::{SortableTermArray, SortableTermArrayBuilder};
use crate::typed_family::families::{
    FamilyArray, FamilyComparator, TypeClaim, TypedFamily,
};
use crate::typed_family::{TypedFamilyId, make_null_aware_comparator};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, StringArray, StringBuilder,
    StructArray,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::compute::{
    and, cast, filter, is_not_null, is_null, not, nullif, or,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::arrow::error::ArrowError;
use datafusion::error::Result as DFResult;
use rdf_fusion_common::AResult;
use rdf_fusion_common::vocab::{rdf, xsd};
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

/// Family for strings and language-tagged strings. The values of the strings are stored in the
/// same array, while those with a language tag have an additional entry in the language array.
///
/// # Layout
///
/// The layout of the resource family is stored as a struct array with two fields:
/// - `values`: the array of string values
/// - `language`: an array of optional language tags
///
/// ```text
/// ┌───────────────────────────┐
/// │ Struct Array              │
/// │                           │
/// │   Values       Language   │
/// │  ┌──────────┐  ┌───────┐  │
/// │  │ "wave"   │  │ NULL  │  │
/// │  │──────────│  │───────│  │
/// │  │ "bye"    │  │"en"   │  │
/// │  │──────────│  │───────│  │
/// │  │ "servus" │  │"de-at"│  │
/// │  └──────────┘  └───────┘  │
/// └───────────────────────────┘
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum StringFamily {}

/// The fields of the string family.
static FIELDS_STRING: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, true),
    ])
});

static DATA_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(FIELDS_STRING.clone()));

static CLAIM: LazyLock<TypeClaim> = LazyLock::new(|| {
    let mut types = BTreeSet::new();
    types.insert(xsd::STRING.into());
    types.insert(rdf::LANG_STRING.into());
    TypeClaim::Literal(types)
});

impl StringFamily {
    pub fn fields() -> &'static Fields {
        &FIELDS_STRING
    }

    /// Creates a new string family array with all values having no language tag.
    pub fn create_simple_strings_array(array: ArrayRef) -> ArrayRef {
        let len = array.len();
        let language_array = Arc::new(StringArray::new_null(len)) as ArrayRef;
        Self::create_strings_array(array, language_array)
    }

    /// Creates a new string family array from the given value and language arrays.
    ///
    /// Uses the [``]
    ///
    /// # Panics
    ///
    /// Panics if the given arrays do not have the same length.
    pub fn create_strings_array(values: ArrayRef, languages: ArrayRef) -> ArrayRef {
        assert_eq!(
            values.len(),
            languages.len(),
            "Both arrays must have the same length"
        );

        let nulls = values.nulls().cloned();
        Arc::new(
            StructArray::try_new(FIELDS_STRING.clone(), vec![values, languages], nulls)
                .expect("Valid struct array"),
        )
    }

    /// Creates a new string family array from the given value and language iterators.
    ///
    /// This is mainly useful for testing.
    pub fn from_iters<S, L>(
        values: impl IntoIterator<Item = S>,
        languages: impl IntoIterator<Item = Option<L>>,
    ) -> ArrayRef
    where
        S: AsRef<str>,
        L: AsRef<str>,
    {
        let values = Arc::new(StringArray::from_iter_values(values)) as ArrayRef;
        let languages = Arc::new(StringArray::from_iter(languages)) as ArrayRef;
        Self::create_strings_array(values, languages)
    }
}

impl TypedFamily for StringFamily {
    type Array = StringFamilyArray;

    const FAMILY_ID: TypedFamilyId = TypedFamilyId::String;

    fn data_type() -> &'static DataType {
        &DATA_TYPE
    }

    fn claim() -> &'static TypeClaim {
        &CLAIM
    }

    fn create_array_from_plain_term(
        array: &PlainTermArray,
    ) -> AResult<StringFamilyArray> {
        validate_input(array)?;

        let parts = array.as_parts();
        let len = parts.struct_array.len();

        let mut values = StringBuilder::new();
        let mut languages = StringBuilder::new();

        for i in 0..len {
            if parts.struct_array.is_null(i) {
                values.append_null();
                languages.append_null();
                continue;
            }

            let value = parts.value.value(i);
            let lang = if parts.language_tag.is_null(i) {
                None
            } else {
                Some(parts.language_tag.value(i))
            };

            values.append_value(value);
            languages.append_option(lang);
        }

        let res_array = StringFamily::create_strings_array(
            Arc::new(values.finish()) as ArrayRef,
            Arc::new(languages.finish()) as ArrayRef,
        );

        return Ok(StringFamilyArray::from_array_unchecked(res_array));

        /// Validates whether the input contains terms that are not claimed by this family.
        fn validate_input(array: &PlainTermArray) -> Result<(), ArrowError> {
            let parts = array.as_parts();
            for i in 0..parts.struct_array.len() {
                if parts.struct_array.is_null(i) {
                    return Err(ArrowError::InvalidArgumentError(
                        "Null value in PlainTermArray".to_string(),
                    ));
                }
                let term_type =
                    PlainTermType::try_from(parts.term_type.value(i)).unwrap();
                if term_type != PlainTermType::Literal {
                    return Err(ArrowError::InvalidArgumentError(
                        "Not a literal".to_string(),
                    ));
                }
                let datatype = parts.data_type.value(i);
                if !CLAIM.is_responsible_for_datatype(datatype) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Wrong datatype: {datatype}"
                    )));
                }
            }
            Ok(())
        }
    }
}

impl Debug for StringFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(Self::FAMILY_ID.as_str())
    }
}

/// A family-specific array for the [`StringFamily`].
#[derive(Debug, Clone)]
pub struct StringFamilyArray {
    array: ArrayRef,
}

impl StringFamilyArray {
    /// Creates a new string family array with all values having no language tag.
    pub fn new_null(len: usize) -> StringFamilyArray {
        StringFamilyArray {
            array: Arc::new(StructArray::new_null(FIELDS_STRING.clone(), len)),
        }
    }

    /// Creates a new string family array with all values having no language tag.
    pub fn new_simple(array: StringArray) -> StringFamilyArray {
        let len = array.len();
        let language_array = StringArray::new_null(len);
        Self::try_new(array, language_array).expect("Valid string array")
    }

    /// Creates a new string family array from the given value and language arrays.
    ///
    /// # Panics
    ///
    /// Panics if the given arrays do not have the same length.
    pub fn try_new(
        values: StringArray,
        languages: StringArray,
    ) -> AResult<StringFamilyArray> {
        assert_eq!(
            values.len(),
            languages.len(),
            "Both arrays must have the same length"
        );

        let nulls = values.nulls().cloned();
        StructArray::try_new(
            FIELDS_STRING.clone(),
            vec![Arc::new(values), Arc::new(languages)],
            nulls,
        )
        .map(|arr| StringFamilyArray {
            array: Arc::new(arr),
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

    /// Returns the array of string values.
    pub fn value_array(&self) -> &StringArray {
        self.struct_array().column(0).as_string()
    }

    /// Returns the array of optional language tags.
    pub fn language_array(&self) -> &StringArray {
        self.language_array_ref().as_string()
    }

    /// Returns the array of optional language tags.
    pub fn language_array_ref(&self) -> &ArrayRef {
        self.struct_array().column(1)
    }

    /// Returns a boolean array indicating whether each string is not empty.
    pub fn is_not_empty(&self) -> DFResult<BooleanArray> {
        let values = self.value_array();
        Ok(BooleanArray::from_iter(
            (0..values.len()).map(|i| Some(!values.value(i).is_empty())),
        ))
    }

    /// Applies a unary string operation to each value in the array.
    ///
    /// The language tag is preserved.
    pub fn apply_unary<F>(&self, op: F) -> ArrayRef
    where
        F: Fn(&str) -> String,
    {
        let values = self.value_array();
        let new_values = Arc::new(StringArray::from_iter_values(
            (0..values.len()).map(|i| op(values.value(i))),
        ));
        StringFamily::create_strings_array(
            new_values,
            Arc::new(self.language_array().clone()),
        )
    }

    /// Applies a binary boolean operation to each pair of values in the arrays.
    ///
    /// If the second argument has a language tag, it must match the first argument's language tag.
    /// Otherwise, the result is null (representing a SPARQL error).
    pub fn apply_binary_boolean_element_wise<F>(
        &self,
        other: &Self,
        op: F,
    ) -> BooleanArray
    where
        F: Fn(&str, &str) -> bool,
    {
        let values = self.value_array();
        let other_values = other.value_array();
        let languages = self.language_array();
        let other_languages = other.language_array();

        let mut builder = BooleanBuilder::with_capacity(values.len());
        for i in 0..values.len() {
            let lhs_lang = if languages.is_null(i) {
                None
            } else {
                Some(languages.value(i))
            };
            let rhs_lang = if other_languages.is_null(i) {
                None
            } else {
                Some(other_languages.value(i))
            };

            if rhs_lang.is_none() || lhs_lang == rhs_lang {
                builder.append_value(op(values.value(i), other_values.value(i)));
            } else {
                builder.append_null();
            }
        }
        builder.finish()
    }

    /// Optimized version of apply_binary_boolean that uses a kernel for the comparison.
    pub fn apply_binary_boolean<F>(
        &self,
        other: &Self,
        kernel: F,
    ) -> AResult<BooleanArray>
    where
        F: Fn(&StringArray, &StringArray) -> AResult<BooleanArray>,
    {
        let values = self.value_array();
        let other_values = other.value_array();
        let languages = self.language_array();
        let other_languages = other.language_array();

        let res = kernel(values, other_values)?;

        let is_rhs_lang_null = is_null(other_languages)?;
        let is_lang_eq = eq(languages, other_languages)?;
        let compatibility_mask = or(&is_rhs_lang_null, &is_lang_eq)?;

        let final_res = nullif(&res, &not(&compatibility_mask)?)?;
        Ok(final_res.as_boolean().clone())
    }

    /// Applies a binary string operation to each pair of values in the arrays.
    ///
    /// The language tag of the first argument is preserved if the operation is successful.
    /// If the second argument has a language tag, it must match the first argument's language tag.
    /// Otherwise, the result is null (representing a SPARQL error).
    pub fn apply_binary_string<F>(&self, other: &Self, op: F) -> StringFamilyArray
    where
        F: Fn(&str, &str) -> String,
    {
        self.apply_binary_string_full(other, |a, a_lang, b, b_lang| {
            if b_lang.is_none() || a_lang == b_lang {
                Some((op(a, b), a_lang.map(|s| s.to_string())))
            } else {
                None
            }
        })
    }

    /// Applies a binary string operation to each pair of values in the arrays.
    ///
    /// This is a more flexible version of [`Self::apply_binary_string`] that allows the operation
    /// to decide the language tag of the result.
    pub fn apply_binary_string_full<F>(&self, other: &Self, op: F) -> StringFamilyArray
    where
        F: Fn(&str, Option<&str>, &str, Option<&str>) -> Option<(String, Option<String>)>,
    {
        let values = self.value_array();
        let other_values = other.value_array();
        let languages = self.language_array();
        let other_languages = other.language_array();

        let mut values_builder =
            StringBuilder::with_capacity(values.len(), values.len() * 10);
        let mut language_builder =
            StringBuilder::with_capacity(values.len(), values.len() * 2);
        let mut valid_builder = BooleanBuilder::with_capacity(values.len());

        for i in 0..values.len() {
            let lhs_lang = if languages.is_null(i) {
                None
            } else {
                Some(languages.value(i))
            };
            let rhs_lang = if other_languages.is_null(i) {
                None
            } else {
                Some(other_languages.value(i))
            };

            if let Some((val, lang)) =
                op(values.value(i), lhs_lang, other_values.value(i), rhs_lang)
            {
                values_builder.append_value(val);
                if let Some(l) = lang {
                    language_builder.append_value(l);
                } else {
                    language_builder.append_null();
                }
                valid_builder.append_value(true);
            } else {
                values_builder.append_null();
                language_builder.append_null();
                valid_builder.append_value(false);
            }
        }

        let values = values_builder.finish();
        let languages = language_builder.finish();
        StringFamilyArray::try_new(values, languages).expect("Valid string array")
    }

    /// Optimized version of apply_binary_string that uses a kernel for the operation.
    pub fn apply_binary_string_kernel<F>(
        &self,
        other: &Self,
        kernel: F,
    ) -> DFResult<(NullBuffer, ArrayRef)>
    where
        F: Fn(&StringArray, &StringArray) -> DFResult<StringArray>,
    {
        let values = self.value_array();
        let other_values = other.value_array();
        let languages = self.language_array();
        let other_languages = other.language_array();

        let res = kernel(values, other_values)?;

        let is_rhs_lang_null = is_null(other_languages)?;
        let is_lang_eq = eq(languages, other_languages)?;
        let is_lang_eq_non_null = and(&is_lang_eq, &is_not_null(&is_lang_eq)?)?;
        let compatibility_mask = or(&is_rhs_lang_null, &is_lang_eq_non_null)?;

        let valid_mask = and(&compatibility_mask, &is_not_null(&res)?)?;

        let filtered_values = filter(&res, &valid_mask)?;
        let filtered_languages = filter(languages, &valid_mask)?;

        let null_buffer = NullBuffer::from(valid_mask.values().clone());

        Ok((
            null_buffer,
            StringFamily::create_strings_array(filtered_values, filtered_languages),
        ))
    }

    /// Casts all simple literal values of this string family into a single dense array of the
    /// given target data type. The array can have null values indicating that the cast was not
    /// successful.
    ///
    /// Language-tagged strings are always cast to null. For simple literals, the success
    /// of the cast depends on the target data type and the string value.
    pub fn cast(&self, target_data_type: &DataType) -> DFResult<ArrayRef> {
        let values = self.value_array();
        let cast = cast(values, target_data_type)?;
        let values_with_language = is_not_null(self.language_array())?;
        let result = nullif(cast.as_ref(), &values_with_language)?;
        Ok(result)
    }
}

impl FamilyArray for StringFamilyArray {
    type Family = StringFamily;

    fn from_array_unchecked(array: ArrayRef) -> Self {
        Self { array }
    }

    fn inner_ref(&self) -> &ArrayRef {
        &self.array
    }

    fn into_array_ref(self) -> ArrayRef {
        self.array
    }

    fn comparator(&self, rhs: &Self) -> Option<FamilyComparator> {
        let lhs_val_arr = self.value_array().clone();
        let lhs_lang_arr = self.language_array().clone();
        let lhs_nulls = self.null_buffer();

        let rhs_val_arr = rhs.value_array().clone();
        let rhs_lang_arr = rhs.language_array().clone();
        let rhs_nulls = rhs.null_buffer();

        let inner: FamilyComparator = Box::new(move |lhs_idx, rhs_idx| {
            let lhs_lang = if lhs_lang_arr.is_null(lhs_idx) {
                None
            } else {
                Some(lhs_lang_arr.value(lhs_idx))
            };
            let rhs_lang = if rhs_lang_arr.is_null(rhs_idx) {
                None
            } else {
                Some(rhs_lang_arr.value(rhs_idx))
            };

            if lhs_lang == rhs_lang {
                Some(lhs_val_arr.value(lhs_idx).cmp(rhs_val_arr.value(rhs_idx)))
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

    fn pretty_print(&self) -> AResult<StringArray> {
        Ok(self.value_array().clone())
    }

    fn effective_boolean_value(&self) -> AResult<BooleanArray> {
        let mut builder = BooleanBuilder::with_capacity(self.inner_ref().len());
        let values = self.value_array();
        let languages = self.language_array();
        for i in 0..values.len() {
            if languages.is_null(i) {
                builder.append_value(!values.value(i).is_empty());
            } else {
                builder.append_null();
            }
        }
        Ok(builder.finish())
    }

    fn literal_data_types(&self) -> AResult<StringArray> {
        let languages = self.language_array();
        Ok(StringArray::from_iter_values((0..languages.len()).map(
            |i| {
                if languages.is_valid(i) {
                    rdf::LANG_STRING.as_str()
                } else {
                    xsd::STRING.as_str()
                }
            },
        )))
    }

    fn cast_to_plain_term_array(&self) -> AResult<PlainTermArray> {
        let values = self.value_array().clone();
        let datatypes = self.literal_data_types()?;
        PlainTermArray::try_new_literals(
            values,
            datatypes,
            self.language_array().clone(),
            None,
        )
    }

    fn cast_to_sortable_array(&self) -> Result<SortableTermArray, ArrowError> {
        let mut builder = SortableTermArrayBuilder::new(self.inner_ref().len());
        let is_null = self.null_buffer();
        for i in 0..self.inner_ref().len() {
            if is_null.is_null(i) {
                builder.append_null();
            } else {
                builder.append_string(self.value_array().value(i));
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
    fn test_string_family_literal_data_types() {
        let values = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let languages = Arc::new(StringArray::from(vec![None, Some("en")])) as ArrayRef;
        let array = StringFamily::create_strings_array(values, languages);

        let family_array = StringFamilyArray::from_array_unchecked(array);
        let datatypes = family_array.literal_data_types().unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "datatype",
            Arc::new(datatypes) as ArrayRef,
        )])
        .unwrap();
        let formatted = pretty_format_batches(&[batch]).unwrap().to_string();

        insta::assert_snapshot!(formatted, @r"
            +-------------------------------------------------------+
            | datatype                                              |
            +-------------------------------------------------------+
            | http://www.w3.org/2001/XMLSchema#string               |
            | http://www.w3.org/1999/02/22-rdf-syntax-ns#langString |
            +-------------------------------------------------------+
            ");
    }

    #[test]
    fn test_string_family_ebv() {
        let values = Arc::new(StringArray::from(vec!["a", "", "b"])) as ArrayRef;
        let languages =
            Arc::new(StringArray::from(vec![None, None, Some("en")])) as ArrayRef;
        let array = StringFamily::create_strings_array(values, languages);

        let family_array = StringFamilyArray::from_array_unchecked(array);
        let ebv = family_array.effective_boolean_value().unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("ebv", Arc::new(ebv) as ArrayRef)]).unwrap();
        let formatted = pretty_format_batches(&[batch]).unwrap().to_string();

        insta::assert_snapshot!(formatted, @r"
        +-------+
        | ebv   |
        +-------+
        | true  |
        | false |
        |       |
        +-------+");
    }
}
