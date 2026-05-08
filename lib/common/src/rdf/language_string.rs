use std::cmp::Ordering;

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct LanguageString {
    pub value: String,
    pub language: String,
}

impl LanguageString {
    pub fn as_ref(&self) -> LanguageStringRef<'_> {
        LanguageStringRef {
            value: &self.value,
            language: &self.language,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct LanguageStringRef<'value> {
    pub value: &'value str,
    pub language: &'value str,
}

impl<'value> LanguageStringRef<'value> {
    /// Creates a new [LanguageStringRef].
    pub fn new(value: &'value str, language: &'value str) -> Self {
        Self { value, language }
    }

    /// Returns whether the value of the langauge string is empty.
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Converts `self` to an owned [LanguageString].
    pub fn into_owned(self) -> LanguageString {
        LanguageString {
            value: self.value.to_owned(),
            language: self.language.to_owned(),
        }
    }
}

impl PartialOrd for LanguageStringRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.language == other.language {
            self.value.partial_cmp(other.value)
        } else {
            None
        }
    }
}
