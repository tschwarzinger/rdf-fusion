use crate::plain_term::{PlainTermArray, PlainTermType};
use crate::sortable_term::{SortableTermArray, SortableTermArrayBuilder};
use crate::typed_family::families::{
    FamilyArray, FamilyComparator, TypeClaim, TypedFamily,
};
use crate::typed_family::{FamilyScalar, TypedFamilyId, make_null_aware_comparator};
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, AsArray, BooleanArray, BooleanBuilder,
    Decimal128Array, Decimal128Builder, Float32Array, Float32Builder, Float64Array,
    Float64Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, StringArray,
    UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use datafusion::arrow::error::ArrowError;
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{
    AResult, Decimal, LiteralRef, NamedNodeRef, Numeric, ThinResult, TypedValueRef,
};
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::iter::repeat_n;
use std::sync::{Arc, LazyLock};

/// Family of numeric values, including `xsd:float`, `xsd:double`, `xsd:decimal`, `xsd:int` and
/// `xsd:integer`. Numeric types that are not part of this family are promoted to one of the
/// supported types.
///
/// # Layout
///
/// The family is encoded as a dense union of the following types:
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────┐
/// │ Union Array (Dense)                                            │
/// │                                                                │
/// │  Type Ids     Float      Double    Decimal    Int     Integer  │
/// │  ┌───────┐   ┌──────┐   ┌──────┐   ┌─────┐   ┌────┐   ┌───┐    │
/// │  │ 0     │   │ 1.2  │   │ 3.4  │   │ 5.6 │   │ 7  │   │ 8 │    │
/// │  │───────│   └──────┘   └──────┘   └─────┘   └────┘   └───┘    │
/// │  │ 1     │                                                     │
/// │  │───────│                                                     │
/// │  │ 2     │                                                     │
/// │  │───────│                                                     │
/// │  │ 3     │                                                     │
/// │  │───────│                                                     │
/// │  │ 4     │                                                     │
/// │  └───────┘                                                     │
/// └────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum NumericFamily {}

/// The fields of the numeric family.
static FIELDS_TYPE: LazyLock<UnionFields> = LazyLock::new(|| {
    let type_ids = vec![0, 1, 2, 3, 4];
    let fields = vec![
        Field::new("float", DataType::Float32, true),
        Field::new("double", DataType::Float64, true),
        Field::new(
            "decimal",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            true,
        ),
        Field::new("int", DataType::Int32, true),
        Field::new("integer", DataType::Int64, true),
    ];
    UnionFields::try_new(type_ids, fields).expect("Valid union fields")
});

static DATA_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Union(FIELDS_TYPE.clone(), UnionMode::Dense));

static CLAIM: LazyLock<TypeClaim> = LazyLock::new(|| {
    let mut types = BTreeSet::new();
    types.insert(xsd::DECIMAL.into());
    types.insert(xsd::DOUBLE.into());
    types.insert(xsd::FLOAT.into());
    types.insert(xsd::INT.into());
    types.insert(xsd::INTEGER.into());
    types.insert(xsd::NON_POSITIVE_INTEGER.into());
    types.insert(xsd::NEGATIVE_INTEGER.into());
    types.insert(xsd::LONG.into());
    types.insert(xsd::SHORT.into());
    types.insert(xsd::BYTE.into());
    types.insert(xsd::NON_NEGATIVE_INTEGER.into());
    types.insert(xsd::UNSIGNED_LONG.into());
    types.insert(xsd::UNSIGNED_INT.into());
    types.insert(xsd::UNSIGNED_SHORT.into());
    types.insert(xsd::UNSIGNED_BYTE.into());
    types.insert(xsd::POSITIVE_INTEGER.into());
    TypeClaim::Literal(types)
});

impl NumericFamily {
    pub const FLOAT_TYPE_ID: i8 = 0;
    pub const DOUBLE_TYPE_ID: i8 = 1;
    pub const DECIMAL_TYPE_ID: i8 = 2;
    pub const INT_TYPE_ID: i8 = 3;
    pub const INTEGER_TYPE_ID: i8 = 4;
}

impl TypedFamily for NumericFamily {
    type Array = NumericFamilyArray;

    const FAMILY_ID: TypedFamilyId = TypedFamilyId::Numeric;

    fn data_type() -> &'static DataType {
        &DATA_TYPE
    }

    fn claim() -> &'static TypeClaim {
        &CLAIM
    }

    fn create_array_from_plain_term(
        array: &PlainTermArray,
    ) -> AResult<NumericFamilyArray> {
        let parts = array.as_parts();
        let len = parts.struct_array.len();

        let mut numeric_builder = NumericFamilyArrayElementBuilder::with_capacity(len);

        for i in 0..len {
            if parts.struct_array.is_null(i) {
                numeric_builder.append_null();
                continue;
            }

            let term_type = PlainTermType::try_from(parts.term_type.value(i)).unwrap();
            if term_type != PlainTermType::Literal {
                return Err(ArrowError::InvalidArgumentError(
                    "Not a literal".to_string(),
                ));
            }

            let value = parts.value.value(i);
            let datatype = parts.data_type.value(i);

            let literal = LiteralRef::new_typed_literal(
                value,
                NamedNodeRef::new_unchecked(datatype),
            );
            let typed_value = TypedValueRef::try_from(literal).ok();

            match typed_value {
                Some(TypedValueRef::NumericLiteral(n)) => {
                    numeric_builder.append_numeric(n);
                }
                _ => {
                    numeric_builder.append_null();
                }
            }
        }

        Ok(numeric_builder.finish())
    }
}

impl Debug for NumericFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(Self::FAMILY_ID.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct NumericFamilyArray {
    array: ArrayRef,
}

impl NumericFamilyArray {
    /// Creates a new [`NumericFamilyArray`] with the given components.
    pub fn create_numeric_array(
        type_ids: Vec<i8>,
        offsets: Vec<i32>,
        children: Vec<ArrayRef>,
    ) -> ArrayRef {
        Arc::new(
            UnionArray::try_new(
                FIELDS_TYPE.clone(),
                type_ids.into(),
                Some(offsets.into()),
                children,
            )
            .expect("Valid union array"),
        )
    }

    /// Creates a new [`NumericFamilyArray`] with all floats.
    pub fn new_floats(array: Float32Array) -> NumericFamilyArray {
        NumericFamilyArrayBuilder::new_for_single_type(
            NumericFamily::FLOAT_TYPE_ID,
            array.len(),
        )
        .expect("Valid numeric array builder")
        .with_floats(Arc::new(array))
        .finish()
        .expect("Valid numeric array")
    }

    /// Creates a new [`NumericFamilyArray`] with all doubles.
    pub fn new_doubles(array: Float64Array) -> NumericFamilyArray {
        NumericFamilyArrayBuilder::new_for_single_type(
            NumericFamily::DOUBLE_TYPE_ID,
            array.len(),
        )
        .expect("Valid numeric array builder")
        .with_doubles(Arc::new(array))
        .finish()
        .expect("Valid numeric array")
    }

    /// Creates a new [`NumericFamilyArray`] with all decimals.
    pub fn try_new_decimals(array: Decimal128Array) -> AResult<NumericFamilyArray> {
        if array.precision() != Decimal::PRECISION || array.scale() != Decimal::SCALE {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Unexpected decimal precision ({}) or scale ({})",
                array.precision(),
                array.scale()
            )));
        }

        let result = NumericFamilyArrayBuilder::new_for_single_type(
            NumericFamily::DECIMAL_TYPE_ID,
            array.len(),
        )
        .expect("Valid numeric array builder")
        .with_decimals(Arc::new(array))
        .finish()
        .expect("Valid numeric array");
        Ok(result)
    }

    /// Creates a new [`NumericFamilyArray`] with all ints.
    pub fn new_ints(array: Int32Array) -> NumericFamilyArray {
        NumericFamilyArrayBuilder::new_for_single_type(
            NumericFamily::INT_TYPE_ID,
            array.len(),
        )
        .expect("Valid numeric array builder")
        .with_ints(Arc::new(array))
        .finish()
        .expect("Valid numeric array")
    }

    /// Creates a new [`NumericFamilyArray`] with a single scalar.
    pub fn new_int_scalar(value: i32) -> FamilyScalar<NumericFamilyArray> {
        let array = Self::new_ints(Int32Array::new_scalar(value).into_inner());
        FamilyScalar::new(array)
    }

    /// Creates a new [`NumericFamilyArray`] with a single integer (i64) scalar.
    pub fn new_integer_scalar(value: i64) -> FamilyScalar<NumericFamilyArray> {
        let array = Self::new_integers(Int64Array::new_scalar(value).into_inner());
        FamilyScalar::new(array)
    }

    /// Creates a new [`NumericFamilyArray`] with a single float (f32) scalar.
    pub fn new_float_scalar(value: f32) -> FamilyScalar<NumericFamilyArray> {
        let array = Self::new_floats(Float32Array::new_scalar(value).into_inner());
        FamilyScalar::new(array)
    }

    /// Creates a new [`NumericFamilyArray`] with a single double (f64) scalar.
    pub fn new_double_scalar(value: f64) -> FamilyScalar<NumericFamilyArray> {
        let array = Self::new_doubles(Float64Array::new_scalar(value).into_inner());
        FamilyScalar::new(array)
    }

    /// Creates a new [`NumericFamilyArray`] with a single decimal scalar.
    pub fn new_decimal_scalar(value: Decimal) -> FamilyScalar<NumericFamilyArray> {
        let array = Self::try_new_decimals(
            Decimal128Array::from(vec![i128::from_be_bytes(value.to_be_bytes())])
                .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
                .expect("Valid decimal array"),
        )
        .expect("Valid precision and scale");
        FamilyScalar::new(array)
    }

    /// Creates a new [`NumericFamilyArray`] with a single numeric scalar.
    pub fn new_scalar_from_numeric(value: Numeric) -> FamilyScalar<NumericFamilyArray> {
        match value {
            Numeric::Int(value) => Self::new_int_scalar(value.into()),
            Numeric::Integer(value) => Self::new_integer_scalar(value.into()),
            Numeric::Float(value) => Self::new_float_scalar(value.into()),
            Numeric::Double(value) => Self::new_double_scalar(value.into()),
            Numeric::Decimal(value) => Self::new_decimal_scalar(value),
        }
    }

    /// Creates a new [`NumericFamilyArray`] with a single null scalar.
    pub fn new_null_scalar() -> FamilyScalar<NumericFamilyArray> {
        let array = Self::new_ints(Int32Array::new_null(1));
        FamilyScalar::new(array)
    }

    /// Creates a new [`NumericFamilyArray`] with all integers.
    pub fn new_integers(array: Int64Array) -> NumericFamilyArray {
        NumericFamilyArrayBuilder::new_for_single_type(
            NumericFamily::INTEGER_TYPE_ID,
            array.len(),
        )
        .expect("Valid numeric array builder")
        .with_integers(Arc::new(array))
        .finish()
        .expect("Valid numeric array")
    }

    /// Creates a new [`NumericFamilyArray`] from the given [`ArrayRef`], matching the given array
    /// based on its data type.
    pub fn try_from_primitive(array: ArrayRef) -> AResult<NumericFamilyArray> {
        let result = match array.data_type() {
            DataType::Float32 => {
                let array = array.as_primitive();
                NumericFamilyArray::new_floats(array.clone())
            }
            DataType::Float64 => {
                let array = array.as_primitive();
                NumericFamilyArray::new_doubles(array.clone())
            }
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE) => {
                let array = array.as_primitive();
                NumericFamilyArray::try_new_decimals(array.clone())
                    .expect("Scale and precision checked")
            }
            DataType::Int32 => {
                let array = array.as_primitive();
                NumericFamilyArray::new_ints(array.clone())
            }
            DataType::Int64 => {
                let array = array.as_primitive();
                NumericFamilyArray::new_integers(array.clone())
            }
            _ => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Unsupported numeric type: {:?}",
                    array.data_type()
                )));
            }
        };
        Ok(result)
    }

    pub fn union_array(&self) -> &UnionArray {
        self.array.as_union()
    }

    pub fn inner(&self) -> &ArrayRef {
        &self.array
    }

    pub fn floats(&self) -> &Float32Array {
        self.union_array().child(0).as_primitive()
    }

    pub fn doubles(&self) -> &Float64Array {
        self.union_array().child(1).as_primitive()
    }

    pub fn decimals(&self) -> &Decimal128Array {
        self.union_array().child(2).as_primitive()
    }

    pub fn ints(&self) -> &Int32Array {
        self.union_array().child(3).as_primitive()
    }

    pub fn integers(&self) -> &Int64Array {
        self.union_array().child(4).as_primitive()
    }

    /// Tries to extract a homogenous type id from the array.
    pub fn try_get_homogenous_type_id_for_fast_path(&self) -> Option<i8> {
        if self.is_empty() {
            return None;
        }

        let type_ids = self.union_array().type_ids();
        let first_id = type_ids[0];
        let all_ids_equal = type_ids.iter().all(|id| *id == first_id);
        if all_ids_equal { Some(first_id) } else { None }
    }

    /// Returns the length of this array.
    pub fn len(&self) -> usize {
        self.union_array().len()
    }

    /// Returns whether this array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get_numeric_opt(&self, index: usize) -> Option<Numeric> {
        let union = self.union_array();
        let type_id = union.type_id(index);
        let offset = union.value_offset(index);
        let child = union.child(type_id);
        if child.is_null(offset) {
            return None;
        }

        match type_id {
            NumericFamily::FLOAT_TYPE_ID => {
                Some(Numeric::Float(self.floats().value(offset).into()))
            }
            NumericFamily::DOUBLE_TYPE_ID => {
                Some(Numeric::Double(self.doubles().value(offset).into()))
            }
            NumericFamily::DECIMAL_TYPE_ID => Some(Numeric::Decimal(
                Decimal::from_be_bytes(self.decimals().value(offset).to_be_bytes()),
            )),
            NumericFamily::INT_TYPE_ID => {
                Some(Numeric::Int(self.ints().value(offset).into()))
            }
            NumericFamily::INTEGER_TYPE_ID => {
                Some(Numeric::Integer(self.integers().value(offset).into()))
            }
            _ => unreachable!(),
        }
    }

    pub fn get_numeric(&self, index: usize) -> Numeric {
        self.get_numeric_opt(index).expect("Value is not null")
    }

    pub fn sum(&self) -> FamilyScalar<NumericFamilyArray> {
        let mut sum = Numeric::Integer(0.into());
        let is_null = self.null_buffer();

        if is_null.null_count() > 0 {
            return NumericFamilyArray::new_null_scalar();
        }

        for i in 0..self.union_array().len() {
            sum = match sum.checked_add(self.get_numeric(i)) {
                Ok(v) => v,
                Err(_) => return NumericFamilyArray::new_null_scalar(),
            }
        }
        NumericFamilyArray::new_scalar_from_numeric(sum)
    }

    fn apply_unary<F>(&self, f: F) -> AResult<Self>
    where
        F: Fn(Numeric) -> ThinResult<Numeric>,
    {
        let len = self.union_array().len();
        let is_null = self.null_buffer();
        let mut builder = NumericFamilyArrayElementBuilder::with_capacity(len);
        for i in 0..len {
            if is_null.is_null(i) {
                builder.append_null();
            } else {
                let res = f(self.get_numeric(i));
                match res {
                    Ok(v) => builder.append_numeric(v),
                    Err(_) => builder.append_null(),
                }
            }
        }
        Ok(builder.finish())
    }

    pub fn abs(&self) -> AResult<Self> {
        self.apply_unary(|v| v.abs())
    }

    pub fn neg(&self) -> AResult<Self> {
        self.apply_unary(|v| v.neg())
    }

    pub fn ceil(&self) -> AResult<Self> {
        self.apply_unary(|v| match v {
            Numeric::Float(f) => Ok(Numeric::Float(f.ceil())),
            Numeric::Double(d) => Ok(Numeric::Double(d.ceil())),
            Numeric::Decimal(d) => d.checked_ceil().map(Numeric::Decimal),
            _ => Ok(v),
        })
    }

    pub fn floor(&self) -> AResult<Self> {
        self.apply_unary(|v| match v {
            Numeric::Float(f) => Ok(Numeric::Float(f.floor())),
            Numeric::Double(d) => Ok(Numeric::Double(d.floor())),
            Numeric::Decimal(d) => d.checked_floor().map(Numeric::Decimal),
            _ => Ok(v),
        })
    }

    pub fn round(&self) -> AResult<Self> {
        self.apply_unary(|v| match v {
            Numeric::Float(f) => Ok(Numeric::Float(f.round())),
            Numeric::Double(d) => Ok(Numeric::Double(d.round())),
            Numeric::Decimal(d) => d.checked_round().map(Numeric::Decimal),
            _ => Ok(v),
        })
    }

    pub fn is_not_zero(&self) -> AResult<BooleanArray> {
        let mut builder = BooleanBuilder::with_capacity(self.inner().len());
        let is_null = self.null_buffer();
        for i in 0..self.inner().len() {
            if is_null.is_null(i) {
                builder.append_null();
            } else {
                let is_not_zero = !self.get_numeric(i).is_zero();
                builder.append_value(is_not_zero);
            }
        }
        Ok(builder.finish())
    }

    /// Returns the [`NumericFamilyArrayParts`] for this array.
    pub fn as_parts(&self) -> NumericFamilyArrayParts<'_> {
        NumericFamilyArrayParts {
            type_ids: self.union_array().type_ids(),
            offsets: self.union_array().offsets().expect("Dense union"),
            floats: self.floats(),
            doubles: self.doubles(),
            decimals: self.decimals(),
            ints: self.ints(),
            integers: self.integers(),
        }
    }
}

impl FamilyArray for NumericFamilyArray {
    type Family = NumericFamily;

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
            let lhs_val = lhs.get_numeric(lhs_idx);
            let rhs_val = rhs.get_numeric(rhs_idx);
            lhs_val.partial_cmp(&rhs_val)
        });

        if lhs_nulls.null_count() > 0 || rhs_nulls.null_count() > 0 {
            Some(make_null_aware_comparator(lhs_nulls, rhs_nulls, inner))
        } else {
            Some(inner)
        }
    }

    fn pretty_print(&self) -> Result<StringArray, ArrowError> {
        let len = self.inner_ref().len();
        let is_null = self.null_buffer();
        Ok((0..len)
            .map(|i| {
                if is_null.is_null(i) {
                    None
                } else {
                    Some(self.get_numeric(i).to_string())
                }
            })
            .collect::<StringArray>())
    }

    fn effective_boolean_value(&self) -> Result<BooleanArray, ArrowError> {
        self.is_not_zero()
    }

    fn literal_data_types(&self) -> Result<StringArray, ArrowError> {
        let is_null = self.null_buffer();
        Ok(StringArray::from_iter((0..self.union_array().len()).map(
            |i| {
                if is_null.is_null(i) {
                    None
                } else {
                    match self.union_array().type_id(i) {
                        NumericFamily::FLOAT_TYPE_ID => Some(xsd::FLOAT.as_str()),
                        NumericFamily::DOUBLE_TYPE_ID => Some(xsd::DOUBLE.as_str()),
                        NumericFamily::DECIMAL_TYPE_ID => Some(xsd::DECIMAL.as_str()),
                        NumericFamily::INT_TYPE_ID => Some(xsd::INT.as_str()),
                        NumericFamily::INTEGER_TYPE_ID => Some(xsd::INTEGER.as_str()),
                        _ => unreachable!(),
                    }
                }
            },
        )))
    }

    fn cast_to_plain_term_array(&self) -> Result<PlainTermArray, ArrowError> {
        let values = self.pretty_print()?;
        let datatypes = self.literal_data_types()?;
        let len = self.inner_ref().len();
        PlainTermArray::try_new_literals(
            values,
            datatypes,
            StringArray::new_null(len),
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
                let n = self.get_numeric(i);
                builder.append_numeric(n, n.to_string().as_bytes());
            }
        }
        Ok(builder.finish().try_into().unwrap())
    }
}

pub struct NumericFamilyArrayParts<'arr> {
    pub type_ids: &'arr ScalarBuffer<i8>,
    pub offsets: &'arr ScalarBuffer<i32>,
    pub floats: &'arr Float32Array,
    pub doubles: &'arr Float64Array,
    pub decimals: &'arr Decimal128Array,
    pub ints: &'arr Int32Array,
    pub integers: &'arr Int64Array,
}

/// A builder for creating an array of the [`NumericFamily`].
#[derive(Clone)]
pub struct NumericFamilyArrayBuilder {
    type_ids: ScalarBuffer<i8>,
    offsets: ScalarBuffer<i32>,
    floats: Option<ArrayRef>,
    doubles: Option<ArrayRef>,
    decimals: Option<ArrayRef>,
    ints: Option<ArrayRef>,
    integers: Option<ArrayRef>,
}

impl NumericFamilyArrayBuilder {
    /// Returns the number of elements in this builder.
    pub fn current_len(&self) -> usize {
        self.type_ids.len()
    }

    /// Creates a new [`NumericFamilyArrayBuilder`].
    ///
    /// The `type_ids` and `offsets` must already be the final version.
    pub fn new(type_ids: ScalarBuffer<i8>, offsets: ScalarBuffer<i32>) -> Self {
        Self {
            type_ids,
            offsets,
            floats: None,
            doubles: None,
            decimals: None,
            ints: None,
            integers: None,
        }
    }

    /// Creates a new [`NumericFamilyArrayBuilder`] for a single type.
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
    pub fn with_floats(self, floats: ArrayRef) -> Self {
        assert_eq!(floats.data_type(), &DataType::Float32);
        Self {
            floats: Some(floats),
            ..self
        }
    }

    /// Sets the double array for this builder.
    pub fn with_doubles(self, doubles: ArrayRef) -> Self {
        assert_eq!(doubles.data_type(), &DataType::Float64);
        Self {
            doubles: Some(doubles),
            ..self
        }
    }

    /// Sets the decimal array for this builder.
    pub fn with_decimals(self, decimals: ArrayRef) -> Self {
        assert_eq!(
            decimals.data_type(),
            &DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE)
        );
        Self {
            decimals: Some(decimals),
            ..self
        }
    }

    /// Sets the int array for this builder.
    pub fn with_ints(self, ints: ArrayRef) -> Self {
        assert_eq!(ints.data_type(), &DataType::Int32);
        Self {
            ints: Some(ints),
            ..self
        }
    }

    /// Sets the integer array for this builder.
    pub fn with_integers(self, integers: ArrayRef) -> Self {
        assert_eq!(integers.data_type(), &DataType::Int64);
        Self {
            integers: Some(integers),
            ..self
        }
    }

    /// Builds the array.
    pub fn finish(&self) -> AResult<NumericFamilyArray> {
        let union_array = UnionArray::try_new(
            FIELDS_TYPE.clone(),
            self.type_ids.clone(),
            Some(self.offsets.clone()),
            vec![
                self.floats
                    .clone()
                    .unwrap_or_else(|| Arc::new(Float32Array::new_null(0))),
                self.doubles
                    .clone()
                    .unwrap_or_else(|| Arc::new(Float64Array::new_null(0))),
                self.decimals.clone().unwrap_or_else(|| {
                    Arc::new(
                        Decimal128Array::new_null(0)
                            .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
                            .unwrap(),
                    )
                }),
                self.ints
                    .clone()
                    .unwrap_or_else(|| Arc::new(Int32Array::new_null(0))),
                self.integers
                    .clone()
                    .unwrap_or_else(|| Arc::new(Int64Array::new_null(0))),
            ],
        )
        .map(|arr| Arc::new(arr) as ArrayRef)
        .expect("NumericArrayBuilder::finish: Valid union array");

        Ok(NumericFamilyArray::from_array_unchecked(union_array))
    }
}

pub struct NumericFamilyArrayElementBuilder {
    floats: Float32Builder,
    doubles: Float64Builder,
    decimals: Decimal128Builder,
    ints: Int32Builder,
    integers: Int64Builder,
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
}

impl NumericFamilyArrayElementBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            floats: Float32Builder::with_capacity(0),
            doubles: Float64Builder::with_capacity(0),
            decimals: Decimal128Builder::with_capacity(0)
                .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
                .unwrap(),
            ints: Int32Builder::with_capacity(0),
            integers: Int64Builder::with_capacity(0),
            type_ids: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity),
        }
    }

    pub fn append_numeric(&mut self, value: Numeric) {
        match value {
            Numeric::Float(v) => {
                let offset = self.floats.len() as i32;
                self.floats.append_value(v.into());
                self.type_ids.push(NumericFamily::FLOAT_TYPE_ID);
                self.offsets.push(offset);
            }
            Numeric::Double(v) => {
                let offset = self.doubles.len() as i32;
                self.doubles.append_value(v.into());
                self.type_ids.push(NumericFamily::DOUBLE_TYPE_ID);
                self.offsets.push(offset);
            }
            Numeric::Decimal(v) => {
                let offset = self.decimals.len() as i32;
                self.decimals
                    .append_value(i128::from_be_bytes(v.to_be_bytes()));
                self.type_ids.push(NumericFamily::DECIMAL_TYPE_ID);
                self.offsets.push(offset);
            }
            Numeric::Int(v) => {
                let offset = self.ints.len() as i32;
                self.ints.append_value(v.into());
                self.type_ids.push(NumericFamily::INT_TYPE_ID);
                self.offsets.push(offset);
            }
            Numeric::Integer(v) => {
                let offset = self.integers.len() as i32;
                self.integers.append_value(v.into());
                self.type_ids.push(NumericFamily::INTEGER_TYPE_ID);
                self.offsets.push(offset);
            }
        }
    }

    pub fn append_null(&mut self) {
        let offset = self.floats.len() as i32;
        self.floats.append_null();
        self.type_ids.push(NumericFamily::FLOAT_TYPE_ID);
        self.offsets.push(offset);
    }

    pub fn finish_ref(mut self) -> ArrayRef {
        Arc::new(
            UnionArray::try_new(
                FIELDS_TYPE.clone(),
                self.type_ids.into(),
                Some(self.offsets.into()),
                vec![
                    Arc::new(self.floats.finish()) as ArrayRef,
                    Arc::new(self.doubles.finish()) as ArrayRef,
                    Arc::new(self.decimals.finish()) as ArrayRef,
                    Arc::new(self.ints.finish()) as ArrayRef,
                    Arc::new(self.integers.finish()) as ArrayRef,
                ],
            )
            .expect("Valid union array"),
        ) as ArrayRef
    }

    pub fn finish(self) -> NumericFamilyArray {
        let raw = self.finish_ref();
        NumericFamilyArray::from_array_unchecked(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use insta::assert_snapshot;

    #[test]
    fn test_numeric_family_pretty_print() {
        let array = NumericFamilyArray::new_integers(Int64Array::from(vec![1, 2]));
        let pretty = array.pretty_print().unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("pretty", Arc::new(pretty) as ArrayRef)])
                .unwrap();
        let formatted = pretty_format_batches(&[batch]).unwrap().to_string();

        assert_snapshot!(formatted, @r"
        +--------+
        | pretty |
        +--------+
        | 1      |
        | 2      |
        +--------+");
    }

    #[test]
    fn test_numeric_family_is_null_ill_formed() {
        let values = StringArray::from(vec!["1", "abc"]);
        let datatypes = StringArray::from(vec![
            "http://www.w3.org/2001/XMLSchema#integer",
            "http://www.w3.org/2001/XMLSchema#integer",
        ]);
        let plain_terms = PlainTermArray::try_new_literals(
            values,
            datatypes,
            StringArray::new_null(2),
            None,
        )
        .unwrap();

        let family_array =
            NumericFamily::create_array_from_plain_term(&plain_terms).unwrap();
        let null_buffer = family_array.null_buffer();

        assert!(!null_buffer.is_null(0));
        assert!(null_buffer.is_null(1));
    }
}
