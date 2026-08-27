use crate::local_object_ids::LocalDictionaryTerm;
use quick_cache::Equivalent;
use redb::{Key, TypeName, Value};
use std::cmp::Ordering;
use std::sync::Arc;

impl<'a> From<&'a LocalDictionaryTerm> for RedbTerm<'a> {
    fn from(value: &'a LocalDictionaryTerm) -> Self {
        RedbTerm {
            term_type: value.term_type,
            value: &value.value,
            data_type: value.data_type.as_deref(),
            language: value.language.as_deref(),
        }
    }
}

/// A borrowed representation of [`LocalDictionaryTerm`] used for zero-copy redb key/value storage
/// and cache lookups without triggering unnecessary String allocations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RedbTerm<'a> {
    pub term_type: i8,
    pub value: &'a str,
    pub data_type: Option<&'a str>,
    pub language: Option<&'a str>,
}

impl<'a> RedbTerm<'a> {
    /// Creates a [`LocalDictionaryTerm`] based on this redb term.
    pub fn as_local_dictionary_term(&self) -> LocalDictionaryTerm {
        LocalDictionaryTerm {
            term_type: self.term_type,
            value: self.value.to_string(),
            data_type: self.data_type.map(|s| s.to_string()),
            language: self.language.map(|s| s.to_string()),
        }
    }
}

impl<'a> Equivalent<LocalDictionaryTerm> for RedbTerm<'a> {
    fn equivalent(&self, key: &LocalDictionaryTerm) -> bool {
        self.term_type == key.term_type
            && self.value == key.value
            && self.data_type == key.data_type.as_deref()
            && self.language == key.language.as_deref()
    }
}

impl<'a> Equivalent<Arc<LocalDictionaryTerm>> for RedbTerm<'a> {
    fn equivalent(&self, key: &Arc<LocalDictionaryTerm>) -> bool {
        self.term_type == key.term_type
            && self.value == key.value
            && self.data_type == key.data_type.as_deref()
            && self.language == key.language.as_deref()
    }
}

impl Value for RedbTerm<'_> {
    type SelfType<'a>
        = RedbTerm<'a>
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let term_type = data[0] as i8;
        let val_len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
        let dt_len = i32::from_le_bytes(data[5..9].try_into().unwrap());
        let lang_len = i32::from_le_bytes(data[9..13].try_into().unwrap());

        let mut offset = 13;
        let value = core::str::from_utf8(&data[offset..offset + val_len]).unwrap();
        offset += val_len;

        let data_type = if dt_len >= 0 {
            let len = dt_len as usize;
            let s = core::str::from_utf8(&data[offset..offset + len]).unwrap();
            offset += len;
            Some(s)
        } else {
            None
        };

        let language = if lang_len >= 0 {
            let len = lang_len as usize;
            let s = core::str::from_utf8(&data[offset..offset + len]).unwrap();
            Some(s)
        } else {
            None
        };

        RedbTerm {
            term_type,
            value,
            data_type,
            language,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let val_bytes = value.value.as_bytes();
        let dt_bytes = value.data_type.map(|s| s.as_bytes());
        let lang_bytes = value.language.map(|s| s.as_bytes());

        let total_size = 1
            + 4
            + 4
            + 4
            + val_bytes.len()
            + dt_bytes.map_or(0, |b| b.len())
            + lang_bytes.map_or(0, |b| b.len());

        let mut buf = Vec::with_capacity(total_size);
        buf.push(value.term_type as u8);
        buf.extend_from_slice(&(val_bytes.len() as u32).to_le_bytes());
        match dt_bytes {
            Some(b) => buf.extend_from_slice(&(b.len() as i32).to_le_bytes()),
            None => buf.extend_from_slice(&(-1i32).to_le_bytes()),
        }
        match lang_bytes {
            Some(b) => buf.extend_from_slice(&(b.len() as i32).to_le_bytes()),
            None => buf.extend_from_slice(&(-1i32).to_le_bytes()),
        }

        buf.extend_from_slice(val_bytes);
        if let Some(b) = dt_bytes {
            buf.extend_from_slice(b);
        }
        if let Some(b) = lang_bytes {
            buf.extend_from_slice(b);
        }

        buf
    }

    fn type_name() -> TypeName {
        TypeName::new("rdf_fusion_storage::RedbTerm")
    }
}

impl Key for RedbTerm<'_> {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let type_cmp = (data1[0] as i8).cmp(&(data2[0] as i8));
        if type_cmp != Ordering::Equal {
            return type_cmp;
        }

        let (val1, dt1, lang1) = get_slices(data1);
        let (val2, dt2, lang2) = get_slices(data2);

        let val_cmp = val1.cmp(val2);
        if val_cmp != Ordering::Equal {
            return val_cmp;
        }

        let dt_cmp = dt1.cmp(&dt2);
        if dt_cmp != Ordering::Equal {
            return dt_cmp;
        }

        return lang1.cmp(&lang2);

        fn get_slices(data: &[u8]) -> (&[u8], Option<&[u8]>, Option<&[u8]>) {
            let val_len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
            let dt_len = i32::from_le_bytes(data[5..9].try_into().unwrap());
            let lang_len = i32::from_le_bytes(data[9..13].try_into().unwrap());

            let mut offset = 13;
            let val_bytes = &data[offset..offset + val_len];
            offset += val_len;

            let dt_bytes = if dt_len >= 0 {
                let len = dt_len as usize;
                let b = &data[offset..offset + len];
                offset += len;
                Some(b)
            } else {
                None
            };

            let lang_bytes = if lang_len >= 0 {
                let len = lang_len as usize;
                let b = &data[offset..offset + len];
                Some(b)
            } else {
                None
            };

            (val_bytes, dt_bytes, lang_bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redb_term_serialization_roundtrip() {
        let cases = [
            RedbTerm {
                term_type: 0,
                value: "http://example.org/subject",
                data_type: None,
                language: None,
            },
            RedbTerm {
                term_type: 1,
                value: "hello world",
                data_type: None,
                language: None,
            },
            RedbTerm {
                term_type: 1,
                value: "12345",
                data_type: Some("http://www.w3.org/2001/XMLSchema#integer"),
                language: None,
            },
            RedbTerm {
                term_type: 1,
                value: "bonjour",
                data_type: None,
                language: Some("fr"),
            },
            RedbTerm {
                term_type: 1,
                value: "custom",
                data_type: Some("http://example.org/datatype"),
                language: Some("en-US"),
            },
            RedbTerm {
                term_type: 0,
                value: "",
                data_type: Some(""),
                language: Some(""),
            },
            RedbTerm {
                term_type: 2,
                value: "🦀 Unicode: ñoño 🚀",
                data_type: Some("http://example.org/dt#ümlaut"),
                language: Some("es-ES"),
            },
        ];

        for original in cases {
            let bytes = RedbTerm::as_bytes(&original);
            let deserialized = RedbTerm::from_bytes(&bytes);

            assert_eq!(original, deserialized);
            assert_eq!(original.term_type, deserialized.term_type);
            assert_eq!(original.value, deserialized.value);
            assert_eq!(original.data_type, deserialized.data_type);
            assert_eq!(original.language, deserialized.language);
        }
    }

    #[test]
    fn test_redb_term_zero_copy_deserialization() {
        let original = RedbTerm {
            term_type: 1,
            value: "borrowed string slice",
            data_type: Some("http://www.w3.org/2001/XMLSchema#string"),
            language: Some("en"),
        };

        let bytes = RedbTerm::as_bytes(&original);
        let deserialized = RedbTerm::from_bytes(&bytes);

        // Verify pointers into the `bytes` buffer for zero-copy guarantee
        let buffer_start = bytes.as_ptr() as usize;
        let buffer_end = buffer_start + bytes.len();

        let val_ptr = deserialized.value.as_ptr() as usize;
        assert!(
            val_ptr >= buffer_start && val_ptr + deserialized.value.len() <= buffer_end
        );

        let dt = deserialized.data_type.unwrap();
        let dt_ptr = dt.as_ptr() as usize;
        assert!(dt_ptr >= buffer_start && dt_ptr + dt.len() <= buffer_end);

        let lang = deserialized.language.unwrap();
        let lang_ptr = lang.as_ptr() as usize;
        assert!(lang_ptr >= buffer_start && lang_ptr + lang.len() <= buffer_end);
    }

    #[test]
    fn test_redb_term_key_compare() {
        let term_a = RedbTerm {
            term_type: 0,
            value: "http://example.org/a",
            data_type: None,
            language: None,
        };
        let term_b = RedbTerm {
            term_type: 0,
            value: "http://example.org/b",
            data_type: None,
            language: None,
        };

        let bytes_a = RedbTerm::as_bytes(&term_a);
        let bytes_b = RedbTerm::as_bytes(&term_b);

        assert_eq!(RedbTerm::compare(&bytes_a, &bytes_b), term_a.cmp(&term_b));
        assert_eq!(RedbTerm::compare(&bytes_a, &bytes_a), Ordering::Equal);
        assert_eq!(RedbTerm::compare(&bytes_b, &bytes_a), term_b.cmp(&term_a));
    }

    #[test]
    fn test_redb_term_equivalent() {
        let redb_term = RedbTerm {
            term_type: 1,
            value: "test",
            data_type: Some("http://example.org/dt"),
            language: Some("en"),
        };

        let owned_term = redb_term.as_local_dictionary_term();
        let arc_term = Arc::new(owned_term.clone());

        assert!(redb_term.equivalent(&owned_term));
        assert!(redb_term.equivalent(&arc_term));

        let different_term = LocalDictionaryTerm {
            term_type: 1,
            value: "different".to_owned(),
            data_type: Some("http://example.org/dt".to_owned()),
            language: Some("en".to_owned()),
        };
        assert!(!redb_term.equivalent(&different_term));
    }

    #[test]
    fn test_redb_term_key_compare_edge_cases() {
        // Helper to check both forward and reverse comparisons match the struct's Ord
        let assert_cmp = |a: &RedbTerm, b: &RedbTerm| {
            let bytes_a = RedbTerm::as_bytes(a);
            let bytes_b = RedbTerm::as_bytes(b);

            assert_eq!(
                RedbTerm::compare(&bytes_a, &bytes_b),
                a.cmp(b),
                "\nForward comparison failed.\nA: {a:?}\nB: {b:?}",
            );

            assert_eq!(
                RedbTerm::compare(&bytes_b, &bytes_a),
                b.cmp(a),
                "\nReverse comparison failed.\nA: {a:?}\nB: {b:?}",
            );
        };

        // 1. Differing term_types (should short-circuit early)
        assert_cmp(
            &RedbTerm {
                term_type: 0,
                value: "apple",
                data_type: None,
                language: None,
            },
            &RedbTerm {
                term_type: 1,
                value: "apple",
                data_type: None,
                language: None,
            },
        );

        // 2. Differing values, same term_type
        assert_cmp(
            &RedbTerm {
                term_type: 1,
                value: "apple",
                data_type: None,
                language: None,
            },
            &RedbTerm {
                term_type: 1,
                value: "banana",
                data_type: None,
                language: None,
            },
        );

        // 3. Prefix matches
        assert_cmp(
            &RedbTerm {
                term_type: 1,
                value: "banan",
                data_type: None,
                language: None,
            },
            &RedbTerm {
                term_type: 1,
                value: "banana",
                data_type: None,
                language: None,
            },
        );

        // 4. Unicode strings
        assert_cmp(
            &RedbTerm {
                term_type: 1,
                value: "äpple",
                data_type: None,
                language: None,
            },
            &RedbTerm {
                term_type: 1,
                value: "apple",
                data_type: None,
                language: None,
            },
        );
        assert_cmp(
            &RedbTerm {
                term_type: 1,
                value: "🦀",
                data_type: None,
                language: None,
            },
            &RedbTerm {
                term_type: 1,
                value: "🚀",
                data_type: None,
                language: None,
            },
        );

        // 5. None vs Some
        assert_cmp(
            &RedbTerm {
                term_type: 2,
                value: "test",
                data_type: None,
                language: None,
            },
            &RedbTerm {
                term_type: 2,
                value: "test",
                data_type: Some("dt_a"),
                language: None,
            },
        );

        // 6. Differing Option values
        assert_cmp(
            &RedbTerm {
                term_type: 2,
                value: "test",
                data_type: Some("dt_a"),
                language: None,
            },
            &RedbTerm {
                term_type: 2,
                value: "test",
                data_type: Some("dt_b"),
                language: None,
            },
        );

        // 7. Language differences
        assert_cmp(
            &RedbTerm {
                term_type: 2,
                value: "test",
                data_type: Some("dt"),
                language: Some("en"),
            },
            &RedbTerm {
                term_type: 2,
                value: "test",
                data_type: Some("dt"),
                language: Some("es"),
            },
        );

        // 8. Empty strings vs None
        assert_cmp(
            &RedbTerm {
                term_type: 3,
                value: "",
                data_type: None,
                language: None,
            },
            &RedbTerm {
                term_type: 3,
                value: "",
                data_type: Some(""),
                language: None,
            },
        );
    }
}
