mod boolean;
mod date_time;
mod decimal;
mod double;
mod duration;
mod float;
mod int;
mod integer;
mod numeric;

pub use boolean::*;
pub use date_time::*;
pub use decimal::*;
pub use double::*;
pub use duration::*;
pub use float::*;
pub use int::*;
pub use integer::*;
pub use numeric::*;
use oxrdf::NamedNodeRef;
use oxrdf::vocab::xsd;

/// Checks if the datatype is a numeric datatype.
pub fn is_numeric_datatype(datatype: NamedNodeRef<'_>) -> bool {
    static NUMERIC_DATATYPES: &[NamedNodeRef<'_>; 13] = &[
        xsd::INTEGER,
        xsd::BYTE,
        xsd::SHORT,
        xsd::INT,
        xsd::LONG,
        xsd::UNSIGNED_BYTE,
        xsd::UNSIGNED_SHORT,
        xsd::UNSIGNED_INT,
        xsd::UNSIGNED_LONG,
        xsd::POSITIVE_INTEGER,
        xsd::NEGATIVE_INTEGER,
        xsd::NON_POSITIVE_INTEGER,
        xsd::NON_NEGATIVE_INTEGER,
    ];
    NUMERIC_DATATYPES.contains(&datatype)
}
