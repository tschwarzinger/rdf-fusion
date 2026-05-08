use crate::RdfFusionExprBuilderContext;
use crate::expr_builder_context::decide_input_encoding;
use datafusion::common::{ScalarValue, plan_datafusion_err, plan_err};
use datafusion::functions_aggregate::first_last::first_value;
use datafusion::logical_expr::{Expr, ExprSchemable, lit};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::{Iri, LiteralRef, TermRef, ThinError};
use rdf_fusion_encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArrayElementBuilder, PlainTermScalar,
};
use rdf_fusion_encoding::{EncodingArray, EncodingName, EncodingScalar};
use rdf_fusion_extensions::functions::{BuiltinName, FunctionName};
use std::ops::Not;

/// A builder for expressions that make use of RDF Fusion built-ins.
///
/// Users of RDF Fusion can override all built-ins with custom implementations. As a result,
/// constructing expressions requires access to some `state` that holds the set of registered
/// built-ins. This struct provides an abstraction over using this `registry`.
#[derive(Debug, Clone)]
pub struct RdfFusionExprBuilder<'root> {
    /// Holds a reference to the factory that created this builder.
    context: RdfFusionExprBuilderContext<'root>,
    /// The expression that is being built
    expr: Expr,
}

impl<'root> RdfFusionExprBuilder<'root> {
    /// Creates a new expression builder.
    ///
    /// Returns an `Err` if the expression does not evaluate to an RDF term.
    pub fn try_new_from_context(
        root: RdfFusionExprBuilderContext<'root>,
        expr: Expr,
    ) -> DFResult<Self> {
        let result = Self {
            context: root,
            expr,
        };
        result.encoding()?;
        Ok(result)
    }

    /// Returns the schema of the input data.
    pub fn context(&self) -> &RdfFusionExprBuilderContext<'root> {
        &self.context
    }

    //
    // Functional Forms
    //

    /// Creates an expression that checks if a variable is bound.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - BOUND](https://www.w3.org/TR/sparql11-query/#func-bound)
    pub fn bound(self) -> DFResult<Self> {
        let name = BuiltinName::Bound;
        self.apply_builtin(name, Vec::new())
    }

    /// Returns an expression that evaluates to either the value of `if_true´ or `if_false`
    /// depending on the effective boolean value of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 IF](https://www.w3.org/TR/sparql11-query/#func-if)
    pub fn sparql_if(self, if_true: Expr, if_false: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::If, vec![if_true, if_false])
    }

    /// Creates a new expression that evaluates to the first argument that does not produce an
    /// error.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Coalesce](https://www.w3.org/TR/sparql11-query/#func-coalesce)
    pub fn coalesce(self, args: Vec<Expr>) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Coalesce, args)
    }

    /// Creates an expression that checks for RDF term equality.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - RDFterm-equal](https://www.w3.org/TR/sparql11-query/#func-RDFterm-equal)
    pub fn rdf_term_equal(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Equal, vec![rhs])
    }

    // TODO In/Not in

    //
    // Functions on RDF Terms
    //

    /// Creates an expression that checks if the inner expression is an IRI.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - isIRI](https://www.w3.org/TR/sparql11-query/#func-isIRI)
    #[allow(clippy::wrong_self_convention)]
    pub fn is_iri(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::IsIri, vec![])
    }

    /// Creates an expression that checks if the inner expression is a blank node.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - isBlank](https://www.w3.org/TR/sparql11-query/#func-isBlank)
    #[allow(clippy::wrong_self_convention)]
    pub fn is_blank(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::IsBlank, vec![])
    }

    /// Creates an expression that checks if the inner expression is a literal.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - isLiteral](https://www.w3.org/TR/sparql11-query/#func-isLiteral)
    #[allow(clippy::wrong_self_convention)]
    pub fn is_literal(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::IsLiteral, vec![])
    }

    /// Creates an expression that checks if the inner expression is a numeric value.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - isNumeric](https://www.w3.org/TR/sparql11-query/#func-isNumeric)
    #[allow(clippy::wrong_self_convention)]
    pub fn is_numeric(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::IsNumeric, vec![])
    }

    /// Creates an expression that returns the string representation of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - STR](https://www.w3.org/TR/sparql11-query/#func-str)
    pub fn str(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Str, vec![])
    }

    /// Creates an expression that returns the language tag of a literal.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - LANG](https://www.w3.org/TR/sparql11-query/#func-lang)
    pub fn lang(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Lang, vec![])
    }

    /// Creates an expression that returns the datatype of a literal.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - DATATYPE](https://www.w3.org/TR/sparql11-query/#func-datatype)
    pub fn datatype(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Datatype, vec![])
    }

    /// Creates an expression that constructs an IRI from a string.
    ///
    /// An optional `base_iri` can be provided to resolve relative IRIs. If no `base_iri` is
    /// provided, relative IRIs will produce an error.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - IRI](https://www.w3.org/TR/sparql11-query/#func-iri)
    pub fn iri(self, base_iri: Option<&Iri<String>>) -> DFResult<Self> {
        let typed_family_encoding = self.context.encodings().typed_family();

        let term_res = match base_iri {
            None => ThinError::expected(),
            Some(value) => Ok(TermRef::Literal(LiteralRef::new_simple_literal(
                value.as_str(),
            ))),
        };

        let mut pt_builder = PlainTermArrayElementBuilder::new();
        match term_res {
            Ok(term) => pt_builder.append_term(term),
            Err(_) => pt_builder.append_null(),
        }
        let pt_array = pt_builder.finish();
        let tf_array = typed_family_encoding.cast_from_plain_term_array(&pt_array)?;
        let arg = tf_array.try_as_scalar(0)?;

        self.apply_builtin_with_args(BuiltinName::Iri, vec![lit(arg.into_scalar_value())])
    }

    /// Creates an expression that constructs a blank node from a string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - BNODE](https://www.w3.org/TR/sparql11-query/#func-bnode)
    pub fn bnode_from(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::BNode, vec![])
    }

    /// Creates a literal with a specified datatype from a simple literal.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - STRDT](https://www.w3.org/TR/sparql11-query/#func-strdt)
    pub fn strdt(self, datatype_iri: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrDt, vec![datatype_iri])
    }

    /// Creates a literal with a specified language tag from a simple literal.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - STRLANG](https://www.w3.org/TR/sparql11-query/#func-strlang)
    pub fn strlang(self, lang_tag: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrLang, vec![lang_tag])
    }

    //
    // Functions on Strings
    //

    /// Creates an expression that returns the length of a string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - STRLEN](https://www.w3.org/TR/sparql11-query/#func-strlen)
    pub fn strlen(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrLen, Vec::new())
    }

    /// Creates an expression that returns a substring of a string, starting at a given location.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SUBSTR](https://www.w3.org/TR/sparql11-query/#func-substr)
    pub fn substr(self, starting_loc: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::SubStr, vec![starting_loc])
    }

    /// Creates an expression that returns a substring of a string, with a given length.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SUBSTR](https://www.w3.org/TR/sparql11-query/#func-substr)
    pub fn substr_with_length(self, starting_loc: Expr, length: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::SubStr, vec![starting_loc, length])
    }

    /// Creates an expression that converts a string to uppercase.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - UCASE](https://www.w3.org/TR/sparql11-query/#func-ucase)
    pub fn ucase(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::UCase, vec![])
    }

    /// Creates an expression that converts a string to lowercase.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - LCASE](https://www.w3.org/TR/sparql11-query/#func-lcase)
    pub fn lcase(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::LCase, vec![])
    }

    /// Creates an expression that checks if a string starts with another string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - STRSTARTS](https://www.w3.org/TR/sparql11-query/#func-strstarts)
    pub fn str_starts(self, arg2: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrStarts, vec![arg2])
    }

    /// Creates an expression that checks if a string ends with another string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - STRENDS](https://www.w3.org/TR/sparql11-query/#func-strends)
    pub fn str_ends(self, arg2: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrEnds, vec![arg2])
    }

    /// Creates an expression that checks if a string contains another string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - CONTAINS](https://www.w3.org/TR/sparql11-query/#func-contains)
    pub fn contains(self, arg2: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Contains, vec![arg2])
    }

    /// Creates an expression that returns the part of a string before the first occurrence of
    /// another string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - STRBEFORE](https://www.w3.org/TR/sparql11-query/#func-strbefore)
    pub fn str_before(self, arg2: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrBefore, vec![arg2])
    }

    /// Creates an expression that returns the part of a string after the first occurrence
    /// of another string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - STRAFTER](https://www.w3.org/TR/sparql11-query/#func-strafter)
    pub fn str_after(self, arg2: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrAfter, vec![arg2])
    }

    /// Creates an expression that encodes a string for use in a URI.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - ENCODE_FOR_URI](https://www.w3.org/TR/sparql11-query/#func-encode)
    pub fn encode_for_uri(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::EncodeForUri, vec![])
    }

    /// Creates an expression that concatenates multiple strings.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - CONCAT](https://www.w3.org/TR/sparql11-query/#func-concat)
    pub fn concat(self, args: Vec<Expr>) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Concat, args)
    }

    /// Creates an expression that checks if a language tag matches a language range.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - LANGMATCHES](https://www.w3.org/TR/sparql11-query/#func-langMatches)
    pub fn lang_matches(self, language_range: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::LangMatches, vec![language_range])
    }

    /// Creates an expression that applies a regular expression to a string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - REGEX](https://www.w3.org/TR/sparql11-query/#func-regex)
    pub fn regex(self, pattern: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Regex, vec![pattern])
    }

    /// Creates an expression that applies a regular expression to a string, with flags.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - REGEX](https://www.w3.org/TR/sparql11-query/#func-regex)
    pub fn regex_with_flags(self, pattern: Expr, flags: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Regex, vec![pattern, flags])
    }

    /// Replaces all occurrences of a pattern with a given replacement.
    ///
    /// If more control about the matching behavior is required, use the [Self::replace_with_flags]
    /// operation.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Replace](https://www.w3.org/TR/sparql11-query/#func-replace)
    pub fn replace(self, pattern: Expr, replacement: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Replace, vec![pattern, replacement])
    }

    /// Replaces all occurrences of a pattern with a given replacement.
    ///
    /// In addition to the regular [Self::replace] functions, this allows providing flags used for
    /// the regex matching process.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Replace](https://www.w3.org/TR/sparql11-query/#func-replace)
    pub fn replace_with_flags(
        self,
        pattern: Expr,
        replacement: Expr,
        flags: Expr,
    ) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Replace, vec![pattern, replacement, flags])
    }

    //
    // Numeric
    //

    /// Compute the absolute value of a numeric literal.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Abs](https://www.w3.org/TR/sparql11-query/#func-abs)
    pub fn abs(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Abs, vec![])
    }

    /// Rounds the inner expression to the nearest integer.
    ///
    /// If the value is exactly between two integers, the integer closer to positive infinity is
    /// used (e.g., 0.5 -> 1).
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Round](https://www.w3.org/TR/sparql11-query/#func-round)
    pub fn round(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Round, vec![])
    }

    /// Computes the ceiling of a numeric literal.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - CEIL](https://www.w3.org/TR/sparql11-query/#func-ceil)
    pub fn ceil(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Ceil, vec![])
    }

    /// Computes the floor of a numeric literal.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - FLOOR](https://www.w3.org/TR/sparql11-query/#func-floor)
    pub fn floor(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Floor, vec![])
    }

    //
    // Dates & Times
    //

    /// Creates a new expression that returns the year component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Year](https://www.w3.org/TR/sparql11-query/#func-year)
    pub fn year(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Year, vec![])
    }

    /// Creates a new expression that returns the month component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Month](https://www.w3.org/TR/sparql11-query/#func-month)
    pub fn month(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Month, vec![])
    }

    /// Creates a new expression that returns the day component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Day](https://www.w3.org/TR/sparql11-query/#func-day)
    pub fn day(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Day, vec![])
    }

    /// Creates a new expression that returns the hours component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Hours](https://www.w3.org/TR/sparql11-query/#func-hours)
    pub fn hours(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Hours, vec![])
    }

    /// Creates a new expression that returns the minutes component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Minutes](https://www.w3.org/TR/sparql11-query/#func-minutes)
    pub fn minutes(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Minutes, vec![])
    }

    /// Creates a new expression that returns the seconds component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Seconds](https://www.w3.org/TR/sparql11-query/#func-seconds)
    pub fn seconds(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Seconds, vec![])
    }

    /// Creates a new expression that returns the timezone of the inner expression.
    ///
    /// This returns the timezone as an `xsd:dayTimeDuration`. For a simple string representation
    /// see [Self::tz].
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Timezone](https://www.w3.org/TR/sparql11-query/#func-timezone)
    pub fn timezone(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Timezone, vec![])
    }

    /// Creates a new expression that returns the timezone of the inner expression.
    ///
    /// This returns the timezone as a simple literal. For a representation as a duration
    /// see [Self::timezone].
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Timezone](https://www.w3.org/TR/sparql11-query/#func-timezone)
    pub fn tz(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Tz, vec![])
    }

    //
    // Hash Functions
    //

    /// Creates a new expression that computes the MD5 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - MD5](https://www.w3.org/TR/sparql11-query/#func-md5)
    pub fn md5(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Md5, vec![])
    }

    /// Creates a new expression that computes the SHA1 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA1](https://www.w3.org/TR/sparql11-query/#func-sha1)
    pub fn sha1(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Sha1, vec![])
    }

    /// Creates a new expression that computes the SHA256 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA256](https://www.w3.org/TR/sparql11-query/#func-sha256)
    pub fn sha256(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Sha256, vec![])
    }

    /// Creates a new expression that computes the SHA384 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA384](https://www.w3.org/TR/sparql11-query/#func-sha384)
    pub fn sha384(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Sha384, vec![])
    }

    /// Creates a new expression that computes the SHA512 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA512](https://www.w3.org/TR/sparql11-query/#func-sha512)
    pub fn sha512(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Sha512, vec![])
    }

    //
    // Constructor Functions
    //

    /// Casts the inner expression to an `xsd:string`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_string(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastString, vec![])
    }

    /// Casts the inner expression to an `xsd:dateTime`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_date_time(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastDateTime, vec![])
    }

    /// Casts the inner expression to an `xsd:decimal`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_decimal(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastDecimal, vec![])
    }

    /// Casts the inner expression to an `xsd:double`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_double(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastDouble, vec![])
    }

    /// Casts the inner expression to an `xsd:float`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_float(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastFloat, vec![])
    }

    /// Casts the inner expression to an `xsd:integer`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_integer(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastInteger, vec![])
    }

    /// Casts the inner expression to an `xsd:int`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_int(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastInt, vec![])
    }

    /// Casts the inner expression to an `xsd:boolean`.
    ///
    /// Note that this does _not_ encode the result as a native boolean array. Use
    /// [Self::build_effective_boolean_value] for this purpose
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_boolean(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastBoolean, vec![])
    }

    //
    // Operators
    //

    /// Creates an expression for the unary plus operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    pub fn unary_plus(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::UnaryPlus, vec![])
    }

    /// Creates an expression for the unary minus operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    pub fn unary_minus(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::UnaryMinus, vec![])
    }

    /// Creates an expression for the addition operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Add, vec![rhs])
    }

    /// Creates an expression for the subtraction operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Sub, vec![rhs])
    }

    /// Creates an expression for the multiplication operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Mul, vec![rhs])
    }

    /// Creates an expression for the division operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    #[allow(clippy::should_implement_trait)]
    pub fn div(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Div, vec![rhs])
    }

    //
    // Comparison Operators
    //

    /// Creates an expression for the equality operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    pub fn equal(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Equal, vec![rhs])
    }

    /// Creates an expression for the greater than operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    pub fn greater_than(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::GreaterThan, vec![rhs])
    }

    /// Creates an expression for the greater than or equal operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    pub fn greater_or_equal(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::GreaterOrEqual, vec![rhs])
    }

    /// Creates an expression for the less than operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    pub fn less_than(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::LessThan, vec![rhs])
    }

    /// Creates an expression for the less than or equal operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    pub fn less_or_equal(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::LessOrEqual, vec![rhs])
    }

    /// Creates an expression for the logical not operator.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Operator Mappings](https://www.w3.org/TR/sparql11-query/#OperatorMapping)
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> DFResult<Self> {
        let context = self.context;
        let expr = self.build_effective_boolean_value()?.not();
        context.native_boolean_as_term(expr)
    }

    //
    // Aggregate Functions
    //

    /// Creates a new aggregate expression that computes the average of the inner expression.
    ///
    /// If `distinct` is true, only distinct values are considered.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Avg](https://www.w3.org/TR/sparql11-query/#defn_aggAvg)
    pub fn avg(self, distinct: bool) -> DFResult<Self> {
        self.apply_builtin_udaf(BuiltinName::Avg, distinct)
    }

    /// Creates a new aggregate expression that computes the average of the inner expression.
    ///
    /// If `distinct` is true, only distinct values are considered.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Count](https://www.w3.org/TR/sparql11-query/#defn_aggCount)
    pub fn count(self, distinct: bool) -> DFResult<Self> {
        self.apply_builtin_udaf(BuiltinName::Count, distinct)
    }

    /// Creates a new aggregate expression that computes the maximum of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Max](https://www.w3.org/TR/sparql11-query/#defn_aggMax)
    pub fn max(self) -> DFResult<Self> {
        self.apply_builtin_udaf(BuiltinName::Max, false)
    }

    /// Creates a new aggregate expression that computes the minimum of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Min](https://www.w3.org/TR/sparql11-query/#defn_aggMin)
    pub fn min(self) -> DFResult<Self> {
        self.apply_builtin_udaf(BuiltinName::Min, false)
    }

    /// Creates a new aggregate expression that returns any value of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Sample](https://www.w3.org/TR/sparql11-query/#defn_aggSample)
    #[allow(
        clippy::unnecessary_wraps,
        reason = "Consistent API, Maybe Sample becomes registerable"
    )]
    pub fn sample(self) -> DFResult<Self> {
        Ok(Self {
            expr: first_value(self.expr, Vec::new()),
            ..self
        })
    }

    /// Creates a new aggregate expression that computes the sum of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Sum](https://www.w3.org/TR/sparql11-query/#defn_aggSum)
    pub fn sum(self, distinct: bool) -> DFResult<Self> {
        self.apply_builtin_udaf(BuiltinName::Sum, distinct)
    }

    /// Creates a new aggregate expression that computes the concatenation of the inner expression.
    ///
    /// The `separator` parameter can be used to use a custom separator for combining strings.
    ///
    /// If `distinct` is true, only distinct values are considered.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - GroupConcat](https://www.w3.org/TR/sparql11-query/#defn_aggGroupConcat)
    pub fn group_concat(self, distinct: bool, separator: Option<&str>) -> DFResult<Self> {
        let typed_family_encoding = self.context.encodings().typed_family();

        let mut pt_builder = PlainTermArrayElementBuilder::new();
        let sep = separator.unwrap_or(" ");
        pt_builder.append_literal(LiteralRef::new_simple_literal(sep));

        let pt_array = pt_builder.finish();
        let tf_array = typed_family_encoding.cast_from_plain_term_array(&pt_array)?;
        let arg = tf_array.try_as_scalar(0)?;

        self.context.apply_builtin_udaf(
            BuiltinName::GroupConcat,
            vec![self.expr, lit(arg.into_scalar_value())],
            distinct,
        )
    }

    //
    // Encodings
    //

    /// Tries to obtain the encoding from a given expression.
    fn encoding(&self) -> DFResult<EncodingName> {
        let field = self.expr.to_field(self.context.schema())?.1;

        self.context.encodings().try_get_encoding_name(field.data_type()).ok_or(plan_datafusion_err!(
            "Expression does not have a valid RDF term encoding. Data Type: {}, Expression: {}.",
            field.data_type(),
            &self.expr
        ))
    }

    /// Equivalent to calling [Self::with_any_encoding] with a `&[target_encoding]`.
    pub fn with_encoding(self, target_encoding: EncodingName) -> DFResult<Self> {
        self.with_any_encoding(&[target_encoding])
    }

    /// Ensures that the expression is one of the given `target_encodings`.
    ///
    /// Generally one of the following things happens:
    /// - The expression already in a target encoding and the builder itself is returns.
    /// - The expression is in another encoding and the builder tries to cast the expression to the
    ///   first encoding in `target_encodings`.
    /// - The expression is not an RDF term and an error is returned.
    pub fn with_any_encoding(self, target_encodings: &[EncodingName]) -> DFResult<Self> {
        if target_encodings.is_empty() {
            return Err(plan_datafusion_err!("Target encodings are empty."));
        }

        let source_encoding = self.encoding()?;
        if target_encodings.contains(&source_encoding) {
            return Ok(self);
        }

        let functions_to_apply =
            Self::functions_for_encoding_change(source_encoding, target_encodings)
                .unwrap();

        let mut expr = self.expr;
        for function in functions_to_apply {
            let udf = self.context.create_builtin_udf(function)?;
            expr = udf.call(vec![expr]);
        }

        Ok(Self { expr, ..self })
    }

    fn functions_for_encoding_change(
        source_encoding: EncodingName,
        target_encodings: &[EncodingName],
    ) -> DFResult<Vec<BuiltinName>> {
        for target_encoding in target_encodings {
            let functions_to_apply = match (source_encoding, target_encoding) {
                (
                    EncodingName::ObjectId
                    | EncodingName::TypedFamily
                    | EncodingName::String,
                    EncodingName::PlainTerm,
                ) => {
                    vec![BuiltinName::WithPlainTermEncoding]
                }
                (
                    EncodingName::PlainTerm | EncodingName::ObjectId,
                    EncodingName::TypedFamily,
                ) => {
                    vec![BuiltinName::WithTypedFamilyEncoding]
                }
                (EncodingName::String, EncodingName::TypedFamily) => vec![
                    BuiltinName::WithPlainTermEncoding,
                    BuiltinName::WithTypedFamilyEncoding,
                ],
                (EncodingName::PlainTerm, EncodingName::String) => {
                    vec![BuiltinName::WithStringEncoding]
                }
                (
                    EncodingName::ObjectId | EncodingName::TypedFamily,
                    EncodingName::String,
                ) => vec![
                    BuiltinName::WithPlainTermEncoding,
                    BuiltinName::WithStringEncoding,
                ],
                (EncodingName::PlainTerm, EncodingName::Sortable) => {
                    vec![
                        BuiltinName::WithTypedFamilyEncoding,
                        BuiltinName::WithSortableEncoding,
                    ]
                }
                (EncodingName::TypedFamily, EncodingName::Sortable) => {
                    vec![BuiltinName::WithSortableEncoding]
                }
                (EncodingName::ObjectId, EncodingName::Sortable) => vec![
                    BuiltinName::WithPlainTermEncoding,
                    BuiltinName::WithTypedFamilyEncoding,
                    BuiltinName::WithSortableEncoding,
                ],
                (EncodingName::String, EncodingName::Sortable) => vec![
                    BuiltinName::WithPlainTermEncoding,
                    BuiltinName::WithTypedFamilyEncoding,
                    BuiltinName::WithSortableEncoding,
                ],
                _ => continue,
            };
            return Ok(functions_to_apply);
        }

        plan_err!(
            "Transformation from '{source_encoding:?}' to '{target_encodings:?}' is not supported."
        )
    }
    //
    // Built-Ins
    //

    fn apply_builtin_udaf(self, name: BuiltinName, distinct: bool) -> DFResult<Self> {
        self.context
            .apply_builtin_udaf(name, vec![self.expr], distinct)
    }

    /// Applies a built-in function to the current expression.
    fn apply_builtin(self, name: BuiltinName, further_args: Vec<Expr>) -> DFResult<Self> {
        self.apply_builtin_with_args(name, further_args)
    }

    /// Applies a built-in function with additional arguments to the current expression.
    fn apply_builtin_with_args(
        self,
        name: BuiltinName,
        further_args: Vec<Expr>,
    ) -> DFResult<Self> {
        let mut args = vec![self.expr];
        args.extend(further_args);
        self.context.apply_builtin_with_args(name, args)
    }

    //
    // Building
    //

    /// Returns the expression that has been build and checks whether it evaluates to an RDF term.
    pub fn build(self) -> DFResult<Expr> {
        self.encoding()?;
        Ok(self.build_any())
    }

    /// Returns the expression that has been build without any validation.
    pub fn build_any(self) -> Expr {
        self.expr
    }

    /// Builds an expression that checks for SPARQL `sameTerm` equality.
    ///
    /// This is a terminating builder function as it no longer produces an RDF term as output.
    pub fn build_same_term(self, rhs: Expr) -> DFResult<Expr> {
        let inputs = [self.expr, rhs];
        let encodings = self.context.get_encodings(&inputs)?;

        // If they share an encoding, just return it.
        if encodings.len() == 1 {
            let [lhs, rhs] = inputs;
            return Ok(lhs.eq(rhs));
        }

        let supported_encodings = &[
            EncodingName::PlainTerm,
            EncodingName::String,
            EncodingName::TypedFamily,
            EncodingName::ObjectId,
        ];
        let input_encoding =
            decide_input_encoding(supported_encodings.as_slice(), &encodings)?;

        let [lhs, rhs] = inputs;
        let lhs = self
            .context
            .try_create_builder(lhs)?
            .with_encoding(input_encoding)?
            .build()?;
        let rhs = self
            .context
            .try_create_builder(rhs)?
            .with_encoding(input_encoding)?
            .build()?;

        Ok(lhs.eq(rhs))
    }

    /// Builds an expression that computes the effective boolean value of the inner expression.
    ///
    /// This is a terminating builder function as it no longer produces an RDF term as output.
    pub fn build_effective_boolean_value(self) -> DFResult<Expr> {
        let args = vec![self.expr.clone()]
            .into_iter()
            .map(|e| {
                self.context
                    .try_create_builder(e)?
                    .with_encoding(EncodingName::TypedFamily)?
                    .build()
            })
            .collect::<DFResult<Vec<_>>>()?;

        let udf = self
            .context
            .create_builtin_udf(BuiltinName::EffectiveBooleanValue)?;
        Ok(udf.call(args))
    }

    /// Builds an expression that checks if two terms are compatible for a join.
    ///
    /// This is a terminating builder function as it no longer produces an RDF term as output.
    pub fn build_is_compatible(self, rhs: Expr) -> DFResult<Expr> {
        let args = vec![self.expr, rhs];
        self.context.apply_with_args_no_builder(
            &FunctionName::Builtin(BuiltinName::IsCompatible),
            args,
        )
    }

    /// Builds an expression that checks for `sameTerm` equality with a scalar value.
    ///
    /// This is a terminating builder function as it no longer produces an RDF term as output.
    pub fn build_same_term_scalar(self, scalar: TermRef<'_>) -> DFResult<Expr> {
        let encoding_name = self.encoding()?;
        let literal = match encoding_name {
            EncodingName::PlainTerm => PLAIN_TERM_ENCODING
                .encode_term(Ok(scalar))?
                .into_scalar_value(),
            EncodingName::TypedFamily => {
                let typed_family_encoding = self.context.encodings().typed_family();
                let mut pt_builder = PlainTermArrayElementBuilder::new();
                pt_builder.append_term(scalar);
                let pt_array = pt_builder.finish();
                let tf_array =
                    typed_family_encoding.cast_from_plain_term_array(&pt_array)?;
                let arg = tf_array.try_as_scalar(0)?;
                arg.into_scalar_value()
            }
            EncodingName::Sortable => {
                return plan_err!("Filtering not supported for Sortable encoding.");
            }
            EncodingName::ObjectId => match self.context.encodings().object_id() {
                None => {
                    return plan_err!("The context has not ObjectID encoding registered");
                }
                Some(encoding) => encoding
                    .encode_scalar(&PlainTermScalar::from(scalar))?
                    .into_scalar_value(),
            },
            EncodingName::String => {
                let turtle = scalar.to_string(); // This should ideally be proper Turtle
                ScalarValue::Utf8(Some(turtle))
            }
        };
        self.build_same_term(lit(literal))
    }
}
