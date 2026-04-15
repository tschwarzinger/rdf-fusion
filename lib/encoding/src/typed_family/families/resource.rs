use crate::plain_term::PlainTermType;
use crate::sortable_term::{SortableTermArray, SortableTermArrayBuilder};
use crate::typed_family::families::{
    FamilyArray, FamilyComparator, TypeClaim, TypedFamily,
};
use crate::typed_family::{TypedFamilyId, make_null_aware_comparator};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int8Array, StringArray, StringBuilder,
    UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use datafusion::arrow::error::ArrowError;
use rdf_fusion_model::AResult;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::iter::repeat_n;
use std::sync::{Arc, LazyLock};

/// Family of IRIs and blank node identifiers.
///
/// # Layout
///
/// The layout of the resource family is another dense union array.
///
/// ```text
/// ┌───────────────────────────────────┐
/// │ Union Array (Dense)               │
/// │                                   │
/// │  Type Ids     IRIs    Blank Nodes │
/// │  ┌───────┐  ┌───────┐  ┌───────┐  │
/// │  │ 0     │  │ <v1>  │  │ _:b2  │  │
/// │  │───────│  │───────│  └───────┘  │
/// │  │ 1     │  │ <i2>  │             │
/// │  │───────│  └───────┘             │
/// │  │ 0     │                        │
/// │  └───────┘                        │
/// └───────────────────────────────────┘
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceFamily {}

static FIELDS_TYPE: LazyLock<UnionFields> = LazyLock::new(|| {
    let fields = vec![
        Field::new("named_node", DataType::Utf8, false),
        Field::new("blank_node", DataType::Utf8, false),
    ];
    UnionFields::try_new(vec![0, 1], fields).expect("Valid fields")
});

static DATA_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Union(FIELDS_TYPE.clone(), UnionMode::Dense));

static CLAIM: LazyLock<TypeClaim> = LazyLock::new(|| TypeClaim::Resources);

impl ResourceFamily {
    /// The type if for named nodes.
    pub const NAMED_NODES_TYPE_ID: i8 = 0;

    /// The type if for blank nodes.
    pub const BLANK_NODES_TYPE_ID: i8 = 1;

    /// Returns the fields of this family.
    pub fn fields() -> &'static UnionFields {
        &FIELDS_TYPE
    }

    /// Creates a new numeric family array with all values being named nodes.
    ///
    /// # Errors
    ///
    /// May return an error if the `array` is longer than the offsets representable in a union
    /// array.
    pub fn create_named_nodes_array(array: StringArray) -> AResult<ResourceFamilyArray> {
        ResourceArrayBuilder::new_for_single_type(Self::NAMED_NODES_TYPE_ID, array.len())?
            .with_named_nodes(array)
            .finish()
    }

    /// Creates a new numeric family array with all values being blank nodes.
    ///
    /// # Errors
    ///
    /// May return an error if the `array` is longer than the offsets representable in a union
    /// array.
    ///
    /// # Panics
    ///
    /// Panics if the given `array` does not have the [`DataType::Utf8`].
    pub fn create_blank_nodes_array(array: StringArray) -> AResult<ResourceFamilyArray> {
        ResourceArrayBuilder::new_for_single_type(Self::BLANK_NODES_TYPE_ID, array.len())?
            .with_blank_nodes(array)
            .finish()
    }
}

impl TypedFamily for ResourceFamily {
    type Array = ResourceFamilyArray;

    const FAMILY_ID: TypedFamilyId = TypedFamilyId::Resource;

    fn data_type() -> &'static DataType {
        &DATA_TYPE
    }

    fn claim() -> &'static TypeClaim {
        &CLAIM
    }

    fn create_array_from_plain_term(
        array: &crate::plain_term::PlainTermArray,
    ) -> AResult<ResourceFamilyArray> {
        validate_input(array)?;

        let parts = array.as_parts();
        let len = parts.struct_array.len();

        let mut res_tids = Vec::new();
        let mut res_offsets = Vec::new();
        let mut named_nodes_count = 0;
        let mut named_nodes_builder = StringBuilder::new();
        let mut blank_nodes_count = 0;
        let mut blank_nodes_builder = StringBuilder::new();

        for i in 0..len {
            let term_type = PlainTermType::try_from(parts.term_type.value(i)).unwrap();
            let value = parts.value.value(i);

            match term_type {
                PlainTermType::NamedNode => {
                    res_tids.push(ResourceFamily::NAMED_NODES_TYPE_ID);
                    res_offsets.push(named_nodes_count);
                    named_nodes_builder.append_value(value);
                    named_nodes_count += 1;
                }
                PlainTermType::BlankNode => {
                    res_tids.push(ResourceFamily::BLANK_NODES_TYPE_ID);
                    res_offsets.push(blank_nodes_count);
                    blank_nodes_builder.append_value(value);
                    blank_nodes_count += 1;
                }
                _ => unreachable!("Validation should have caught this"),
            }
        }

        let res_array = UnionArray::try_new(
            FIELDS_TYPE.clone(),
            res_tids.into(),
            Some(res_offsets.into()),
            vec![
                Arc::new(named_nodes_builder.finish()) as ArrayRef,
                Arc::new(blank_nodes_builder.finish()) as ArrayRef,
            ],
        )?;

        return Ok(ResourceFamilyArray::from_array_unchecked(Arc::new(
            res_array,
        )));

        /// Validates whether the input contains terms that are not claimed by this family.
        fn validate_input(
            array: &crate::plain_term::PlainTermArray,
        ) -> Result<(), ArrowError> {
            let parts = array.as_parts();
            for i in 0..parts.struct_array.len() {
                if parts.struct_array.is_null(i) {
                    return Err(ArrowError::InvalidArgumentError(
                        "Null value in PlainTermArray".to_string(),
                    ));
                }
                let term_type =
                    PlainTermType::try_from(parts.term_type.value(i)).unwrap();
                if term_type != PlainTermType::NamedNode
                    && term_type != PlainTermType::BlankNode
                {
                    return Err(ArrowError::InvalidArgumentError(
                        "Not a resource".to_string(),
                    ));
                }
            }
            Ok(())
        }
    }
}

impl Debug for ResourceFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(Self::FAMILY_ID.as_str())
    }
}

/// A family-specific array for the [`ResourceFamily`].
#[derive(Debug, Clone)]
pub struct ResourceFamilyArray {
    array: ArrayRef,
}

impl ResourceFamilyArray {
    /// Returns a reference to the inner [`UnionArray`].
    pub fn inner_ref(array: &ArrayRef) -> &UnionArray {
        array.as_union()
    }

    /// Returns a reference to the inner [`UnionArray`].
    pub fn union_array(&self) -> &UnionArray {
        Self::inner_ref(&self.array)
    }

    /// Returns a reference to the inner [`UnionArray`].
    pub fn inner(&self) -> &UnionArray {
        Self::inner_ref(&self.array)
    }

    /// Returns the array of IRIs.
    pub fn iris(&self) -> &StringArray {
        self.union_array().child(0).as_string()
    }

    /// Returns the array of blank nodes.
    pub fn blank_nodes(&self) -> &StringArray {
        self.union_array().child(1).as_string()
    }

    /// Returns an array that indicates whether an element is a named node.
    pub fn is_named_node(&self) -> BooleanArray {
        self.union_array()
            .type_ids()
            .iter()
            .map(|tid| *tid == ResourceFamily::NAMED_NODES_TYPE_ID)
            .collect()
    }

    /// Returns an array that indicates whether an element is a blank node.
    pub fn is_blank_node(&self) -> BooleanArray {
        self.union_array()
            .type_ids()
            .iter()
            .map(|tid| *tid == ResourceFamily::BLANK_NODES_TYPE_ID)
            .collect()
    }
}

impl FamilyArray for ResourceFamilyArray {
    type Family = ResourceFamily;

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
        let lhs = self.clone();
        let lhs_nulls = self.null_buffer();

        let rhs = other.clone();
        let rhs_nulls = other.null_buffer();

        let inner: FamilyComparator = Box::new(move |lhs_idx, rhs_idx| {
            let lhs_tid = lhs.union_array().type_id(lhs_idx);
            let rhs_tid = rhs.union_array().type_id(rhs_idx);

            if lhs_tid != rhs_tid {
                // Ordering: BlankNode < NamedNode.
                // BlankNode is 1, NamedNode is 0.
                if lhs_tid == ResourceFamily::BLANK_NODES_TYPE_ID {
                    Some(Ordering::Less)
                } else {
                    Some(Ordering::Greater)
                }
            } else {
                let lhs_off = lhs.union_array().value_offset(lhs_idx);
                let rhs_off = rhs.union_array().value_offset(rhs_idx);
                if lhs_tid == ResourceFamily::NAMED_NODES_TYPE_ID {
                    Some(lhs.iris().value(lhs_off).cmp(rhs.iris().value(rhs_off)))
                } else {
                    Some(
                        lhs.blank_nodes()
                            .value(lhs_off)
                            .cmp(rhs.blank_nodes().value(rhs_off)),
                    )
                }
            }
        });

        if lhs_nulls.null_count() > 0 || rhs_nulls.null_count() > 0 {
            Some(make_null_aware_comparator(lhs_nulls, rhs_nulls, inner))
        } else {
            Some(inner)
        }
    }

    fn pretty_print(&self) -> Result<StringArray, ArrowError> {
        let len = self.inner_ref().len();
        Ok((0..len)
            .map(|i| {
                if self.union_array().is_valid(i) {
                    let type_id = self.union_array().type_id(i);
                    let offset = self.union_array().value_offset(i);
                    if type_id == ResourceFamily::NAMED_NODES_TYPE_ID {
                        Some(self.iris().value(offset).to_string())
                    } else {
                        Some(self.blank_nodes().value(offset).to_string())
                    }
                } else {
                    None
                }
            })
            .collect::<StringArray>())
    }

    fn effective_boolean_value(&self) -> Result<BooleanArray, ArrowError> {
        Ok(BooleanArray::new_null(self.inner_ref().len()))
    }

    fn literal_data_types(&self) -> Result<StringArray, ArrowError> {
        Ok(StringArray::new_null(self.inner_ref().len()))
    }

    fn cast_to_plain_term_array(
        &self,
    ) -> Result<crate::plain_term::PlainTermArray, ArrowError> {
        let len = self.inner_ref().len();
        let term_type =
            Int8Array::from_iter(self.union_array().type_ids().iter().copied());

        // We want the literal representation of IRIs and BNodes
        let mut values = Vec::with_capacity(len);
        for i in 0..len {
            let type_id = self.union_array().type_id(i);
            let offset = self.union_array().value_offset(i);
            if type_id == ResourceFamily::NAMED_NODES_TYPE_ID {
                values.push(Some(self.iris().value(offset).to_string()));
            } else {
                values.push(Some(self.blank_nodes().value(offset).to_string()));
            }
        }
        let values = StringArray::from(values);

        Ok(crate::plain_term::PlainTermArray::try_new(
            term_type,
            values,
            StringArray::new_null(len),
            StringArray::new_null(len),
            None,
        )
        .unwrap())
    }

    fn cast_to_sortable_array(&self) -> Result<SortableTermArray, ArrowError> {
        let mut builder = SortableTermArrayBuilder::new(self.inner_ref().len());
        let is_null = self.null_buffer();
        for i in 0..self.inner_ref().len() {
            if is_null.is_null(i) {
                builder.append_null();
            } else {
                let type_id = self.union_array().type_id(i);
                let offset = self.union_array().value_offset(i);
                if type_id == ResourceFamily::NAMED_NODES_TYPE_ID {
                    builder.append_named_node(
                        rdf_fusion_model::NamedNodeRef::new_unchecked(
                            self.iris().value(offset),
                        ),
                    );
                } else {
                    builder.append_blank_node(
                        rdf_fusion_model::BlankNodeRef::new_unchecked(
                            self.blank_nodes().value(offset),
                        ),
                    );
                }
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
    fn test_resource_family_pretty_print() {
        let iris = StringArray::from(vec!["http://example.org/a"]);
        let family_array = ResourceFamily::create_named_nodes_array(iris).unwrap();

        let pretty = family_array.pretty_print().unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("pretty", Arc::new(pretty) as ArrayRef)])
                .unwrap();
        let formatted = pretty_format_batches(&[batch]).unwrap().to_string();

        insta::assert_snapshot!(formatted, @r"
        +----------------------+
        | pretty               |
        +----------------------+
        | http://example.org/a |
        +----------------------+");
    }

    #[test]
    fn test_resource_family_is_named_node() {
        let iris = StringArray::from(vec!["http://example.org/a"]);
        let family_array = ResourceFamily::create_named_nodes_array(iris).unwrap();

        let is_named = family_array.is_named_node();
        let batch = RecordBatch::try_from_iter(vec![(
            "is_named",
            Arc::new(is_named) as ArrayRef,
        )])
        .unwrap();
        let formatted = pretty_format_batches(&[batch]).unwrap().to_string();

        insta::assert_snapshot!(formatted, @r"
        +----------+
        | is_named |
        +----------+
        | true     |
        +----------+");
    }
}

/// A builder for creating an array of the [`ResourceFamily`].
#[derive(Clone)]
pub struct ResourceArrayBuilder {
    type_ids: ScalarBuffer<i8>,
    offsets: ScalarBuffer<i32>,
    named_nodes: Option<ArrayRef>,
    blank_nodes: Option<ArrayRef>,
}

impl ResourceArrayBuilder {
    /// Returns the number of elements in this builder.
    pub fn current_len(&self) -> usize {
        self.type_ids.len()
    }

    /// Creates a new [`ResourceArrayBuilder`].
    ///
    /// The `type_ids` and `offsets` must already be the final version.
    pub fn new(type_ids: ScalarBuffer<i8>, offsets: ScalarBuffer<i32>) -> Self {
        Self {
            type_ids,
            offsets,
            named_nodes: None,
            blank_nodes: None,
        }
    }

    /// Creates a new [`ResourceArrayBuilder`] for a single type.
    pub fn new_for_single_type(type_id: i8, len: usize) -> AResult<Self> {
        let len_i32 = i32::try_from(len).map_err(|_| {
            ArrowError::ArithmeticOverflow(
                "Len is too long for creating numeric array".to_owned(),
            )
        })?;

        Ok(Self::new(
            repeat_n(type_id, len).collect(),
            (0..len_i32).collect(),
        ))
    }

    /// Sets the float array for this builder.
    pub fn with_named_nodes(self, array: StringArray) -> Self {
        Self {
            named_nodes: Some(Arc::new(array)),
            ..self
        }
    }

    /// Sets the float array for this builder.
    pub fn with_blank_nodes(self, array: StringArray) -> Self {
        Self {
            blank_nodes: Some(Arc::new(array)),
            ..self
        }
    }

    /// Builds the array.
    pub fn finish(&self) -> AResult<ResourceFamilyArray> {
        let array = UnionArray::try_new(
            FIELDS_TYPE.clone(),
            self.type_ids.clone(),
            Some(self.offsets.clone()),
            vec![
                self.named_nodes
                    .clone()
                    .unwrap_or_else(|| Arc::new(StringArray::new_null(0))),
                self.blank_nodes
                    .clone()
                    .unwrap_or_else(|| Arc::new(StringArray::new_null(0))),
            ],
        )?;
        Ok(ResourceFamilyArray::from_array_unchecked(Arc::new(array)))
    }
}
