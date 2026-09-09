//! Parsing for SPARQL expressions.
//!
//! This module roughly implements the [expression grammar](https://www.w3.org/TR/sparql11-query/#rExpression)
//! using a Pratt (precedence-climbing) parser, following the approach described
//! in <https://matklad.github.io/2020/04/13/simple-but-powerful-pratt-parsing.html>.

use crate::Diagnostic;
use crate::ast::{Aggregate, Expression, Function, FunctionArg, FunctionName, Iri};
use crate::error::SparqlSyntaxError;
use crate::lexer::Token;
use crate::parser::SparqlParser;
use crate::span::{Span, Spanned};
use rdf_fusion_common::NamedNode;
use rdf_fusion_extensions::functions::{BuiltinName, FunctionName as CommonFunctionName};
use std::borrow::Cow;

enum CallKind {
    Aggregate,
    Scalar,
    Unknown(String),
}

/// Binding powers (higher binds tighter). Right-hand sides reuse the power plus
/// one, which makes every binary operator left-associative.
const REL_BP: u8 = 30;
const UNARY_BP: u8 = 60;

/// A binary infix operator.
#[derive(Clone, Copy)]
enum InfixOp {
    Or,
    And,
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Add,
    Subtract,
    Multiply,
    Divide,
}

impl InfixOp {
    fn binding_power(self) -> (u8, u8) {
        match self {
            Self::Or => (10, 11),
            Self::And => (20, 21),
            Self::Equal
            | Self::NotEqual
            | Self::Less
            | Self::LessOrEqual
            | Self::Greater
            | Self::GreaterOrEqual => (REL_BP, REL_BP + 1),
            Self::Add | Self::Subtract => (40, 41),
            Self::Multiply | Self::Divide => (50, 51),
        }
    }

    /// Returns `true` if this is a relational (non-associative) operator.
    fn is_relational(self) -> bool {
        matches!(
            self,
            Self::Equal
                | Self::NotEqual
                | Self::Less
                | Self::LessOrEqual
                | Self::Greater
                | Self::GreaterOrEqual
        )
    }

    fn make<'a>(
        self,
        lhs: Spanned<Expression<'a>>,
        rhs: Spanned<Expression<'a>>,
    ) -> Expression<'a> {
        let (lhs, rhs) = (Box::new(lhs), Box::new(rhs));
        match self {
            Self::Or => Expression::Or(lhs, rhs),
            Self::And => Expression::And(lhs, rhs),
            Self::Equal => Expression::Equal(lhs, rhs),
            Self::NotEqual => Expression::NotEqual(lhs, rhs),
            Self::Less => Expression::Less(lhs, rhs),
            Self::LessOrEqual => Expression::LessOrEqual(lhs, rhs),
            Self::Greater => Expression::Greater(lhs, rhs),
            Self::GreaterOrEqual => Expression::GreaterOrEqual(lhs, rhs),
            Self::Add => Expression::Add(lhs, rhs),
            Self::Subtract => Expression::Subtract(lhs, rhs),
            Self::Multiply => Expression::Multiply(lhs, rhs),
            Self::Divide => Expression::Divide(lhs, rhs),
        }
    }
}

impl<'a> SparqlParser<'a> {
    /// Parses an `Expression`.
    ///
    /// ```text
    /// Expression ::= ConditionalOrExpression
    /// ```
    pub fn parse_expr(&mut self) -> Result<Spanned<Expression<'a>>, SparqlSyntaxError> {
        self.parse_bp(0)
    }

    /// Pratt core: parse one expression, then consume any infix operators whose
    /// left binding power is at least `min_bp`.
    fn parse_bp(
        &mut self,
        min_bp: u8,
    ) -> Result<Spanned<Expression<'a>>, SparqlSyntaxError> {
        let lhs = self.parse_unary()?;
        self.parse_bp_with_lhs(lhs, min_bp)
    }

    fn parse_bp_with_lhs(
        &mut self,
        mut lhs: Spanned<Expression<'a>>,
        min_bp: u8,
    ) -> Result<Spanned<Expression<'a>>, SparqlSyntaxError> {
        loop {
            if let Some((op, l_bp, r_bp)) = self.peek_binary_infix() {
                if l_bp < min_bp {
                    break;
                }
                // Relational operators are non-associative in SPARQL; reject
                // chaining such as `?a = ?b = ?c` (and mixed `?a < ?b > ?c`).
                if op.is_relational() && Self::is_relational_expr(&lhs.value) {
                    let span = self
                        .peek()
                        .map(|token| token.span)
                        .expect("peeked a binary infix operator");
                    return Err(SparqlSyntaxError::single(Diagnostic::error(
                        span,
                        "Relational operators are non-associative and cannot be chained",
                    )));
                }
                self.bump();
                let rhs = self.parse_bp(r_bp)?;
                let span = Span::new(lhs.span.start, rhs.span.end);
                lhs = Spanned {
                    value: op.make(lhs, rhs),
                    span,
                };
            } else if let Some((op, literal, l_bp, r_bp)) =
                self.peek_binary_signed_numeric()?
            {
                if l_bp < min_bp {
                    break;
                }
                self.bump();
                let span = literal.span();
                let primary = Spanned {
                    value: Expression::Literal(literal),
                    span,
                };
                let rhs = self.parse_bp_with_lhs(primary, r_bp)?;
                let span = Span::new(lhs.span.start, rhs.span.end);
                lhs = Spanned {
                    value: op.make(lhs, rhs),
                    span,
                };
            } else if let Some(negated) = self.peek_in() {
                if REL_BP < min_bp {
                    break;
                }
                self.consume_in(negated)?;
                let list = self.parse_expression_list()?;
                let start = lhs.span.start;
                let end = self.previous().map(|token| token.span.end).unwrap_or(start);
                lhs = Spanned {
                    value: if negated {
                        Expression::NotIn(Box::new(lhs), list)
                    } else {
                        Expression::In(Box::new(lhs), list)
                    },
                    span: Span::new(start, end),
                };
            } else {
                break;
            }
        }

        Ok(lhs)
    }

    /// Returns `true` if `expr` is the result of a relational comparison.
    fn is_relational_expr(expr: &Expression<'a>) -> bool {
        matches!(
            expr,
            Expression::Equal(..)
                | Expression::NotEqual(..)
                | Expression::Less(..)
                | Expression::LessOrEqual(..)
                | Expression::Greater(..)
                | Expression::GreaterOrEqual(..)
        )
    }

    /// Parses a prefix (unary) expression or a primary expression.
    fn parse_unary(&mut self) -> Result<Spanned<Expression<'a>>, SparqlSyntaxError> {
        let value_ctor: fn(Box<Spanned<Expression<'a>>>) -> Expression<'a> =
            if self.peek_operator("!") {
                |e| Expression::Not(e)
            } else if self.peek_operator("-") {
                |e| Expression::UnaryMinus(e)
            } else if self.peek_operator("+") {
                |e| Expression::UnaryPlus(e)
            } else {
                return self.parse_primary();
            };

        let start = self.current_span().start;
        self.bump();
        let operand = self.parse_bp(UNARY_BP)?;
        let span = Span::new(start, operand.span.end);
        Ok(Spanned {
            value: value_ctor(Box::new(operand)),
            span,
        })
    }

    /// Parses a `PrimaryExpression`.
    ///
    /// ```text
    /// PrimaryExpression ::= BrackettedExpression | BuiltInCall | iriOrFunction
    ///                       | RDFLiteral | NumericLiteral | BooleanLiteral | Var
    /// ```
    fn parse_primary(&mut self) -> Result<Spanned<Expression<'a>>, SparqlSyntaxError> {
        match self.peek().map(|token| token.value) {
            Some(Token::Operator("(")) => {
                let open = self.bump().expect("expected '('");
                let inner = self.parse_expr()?;
                let close = self.expect_operator(")").map(|_| {
                    self.previous().map(|t| t.span.end).unwrap_or(open.span.end)
                })?;
                Ok(Spanned {
                    value: inner.value,
                    span: Span::new(open.span.start, close),
                })
            }
            Some(v) if v.is_var() => {
                let var = self.parse_var().expect("peeked a variable token");
                let span = var.span;
                Ok(Spanned {
                    value: Expression::Var(var),
                    span,
                })
            }
            Some(v) if v.is_iri() => self.parse_iri_or_function(),
            _ if self.is_literal_start() => self.parse_literal_expression(),
            Some(Token::Keyword(_)) => self.parse_built_in_call(),
            _ => self.expected("an expression"),
        }
    }

    /// Returns `true` if the current token starts a literal (`RDFLiteral`,
    /// `NumericLiteral` or `BooleanLiteral`).
    fn is_literal_start(&self) -> bool {
        match self.peek().map(|token| token.value) {
            Some(v) => v.is_string() || v.is_numeric_literal() || v.is_boolean(),
            None => false,
        }
    }

    /// Parses a literal into a spanned expression.
    fn parse_literal_expression(
        &mut self,
    ) -> Result<Spanned<Expression<'a>>, SparqlSyntaxError> {
        let literal = self.parse_literal()?;
        let span = literal.span();
        Ok(Spanned {
            value: Expression::Literal(literal),
            span,
        })
    }

    /// Classifies a function name against the registry, preferring an aggregate
    /// (UDAF) over a scalar (UDF) when both are registered.
    fn classify_call(&self, fn_name: &CommonFunctionName) -> Option<CallKind> {
        if self.registry.udf(fn_name).is_ok() {
            Some(CallKind::Scalar)
        } else if self.registry.udaf(fn_name).is_ok() {
            Some(CallKind::Aggregate)
        } else {
            None
        }
    }

    fn check_iri_call_kind(&self, iri: &Iri<'a>) -> Result<CallKind, SparqlSyntaxError> {
        let resolved = self.resolve_iri_string(iri)?;
        let named_node = NamedNode::new_unchecked(resolved.clone());
        let fn_name = CommonFunctionName::Custom(named_node);
        Ok(self
            .classify_call(&fn_name)
            .unwrap_or(CallKind::Unknown(resolved)))
    }

    fn check_builtin_call_kind(&self, kw: &str) -> CallKind {
        let kw_upper = kw.to_ascii_uppercase();
        if kw_upper == "SAMPLE" {
            return CallKind::Aggregate;
        }
        if kw_upper == "NOW" || kw_upper == "SAMETERM" {
            return CallKind::Scalar;
        }

        if let Ok(builtin) = BuiltinName::try_from(kw) {
            let fn_name = CommonFunctionName::Builtin(builtin);
            if let Some(kind) = self.classify_call(&fn_name) {
                return kind;
            }
        }

        let custom_name =
            CommonFunctionName::Custom(NamedNode::new_unchecked(kw.to_string()));
        self.classify_call(&custom_name)
            .unwrap_or_else(|| CallKind::Unknown(kw.to_string()))
    }

    /// Parses an `iriOrFunction`: an IRI, optionally with an argument list.
    ///
    /// ```text
    /// iriOrFunction ::= iri ArgList?
    /// ```
    fn parse_iri_or_function(
        &mut self,
    ) -> Result<Spanned<Expression<'a>>, SparqlSyntaxError> {
        let iri = self.parse_iri()?;
        let start = iri.span().start;
        if self.peek_operator("(") {
            let kind = self.check_iri_call_kind(&iri)?;
            let value = self.parse_call(FunctionName::Iri(iri), kind)?;
            let end = self.previous().map(|token| token.span.end).unwrap_or(start);
            Ok(Spanned {
                value,
                span: Span::new(start, end),
            })
        } else {
            let end = iri.span().end;
            Ok(Spanned {
                value: Expression::Iri(iri),
                span: Span::new(start, end),
            })
        }
    }

    /// Parses a function or aggregate call given its name and kind.
    ///
    /// Only aggregates may carry a leading `DISTINCT` modifier, `;`-separated
    /// scalar parameters (e.g. `GROUP_CONCAT(?x; SEPARATOR = ";")`) or a bare `*`
    /// argument.
    fn parse_call(
        &mut self,
        name: FunctionName<'a>,
        kind: CallKind,
    ) -> Result<Expression<'a>, SparqlSyntaxError> {
        match kind {
            CallKind::Aggregate => {
                self.expect_operator("(")?;
                let distinct = self.parse_keyword("DISTINCT");
                let (star, args) = self.parse_aggregate_args()?;
                self.expect_operator(")")?;
                Ok(Expression::Aggregate(Aggregate {
                    name,
                    distinct,
                    star,
                    args,
                }))
            }
            CallKind::Scalar => {
                let args = self.parse_scalar_args()?;
                Ok(Expression::Function(Function { name, args }))
            }
            CallKind::Unknown(unknown) => {
                Err(SparqlSyntaxError::single(Diagnostic::error(
                    name.span(),
                    format!("Unknown function or aggregate: {unknown}"),
                )))
            }
        }
    }

    /// Parses an aggregate argument list (after `DISTINCT` has been consumed):
    /// a bare `*` or a single expression, optionally followed by `;`-separated
    /// named parameters (`NAME = Expression`).
    ///
    /// ```text
    /// AggregateArgList ::= '*' | Expression ( ';' 'NAME' '=' Expression )*
    /// ```
    fn parse_aggregate_args(
        &mut self,
    ) -> Result<(bool, Vec<FunctionArg<'a>>), SparqlSyntaxError> {
        if self.peek_operator("*") {
            self.bump();
            return Ok((true, Vec::new()));
        }
        let mut args = Vec::new();
        if !self.peek_operator(")") {
            args.push(FunctionArg::Positional(self.parse_expr()?));
            while self.consume_operator(";") {
                if self.peek_operator(")") {
                    break;
                }
                let Some(name) = self.peek_keyword_value() else {
                    return self.expected("a named parameter (NAME = value)");
                };
                let name_span = self.current_span();
                self.bump();
                self.expect_operator("=")?;
                let value = self.parse_expr()?;
                args.push(FunctionArg::Named(
                    Spanned {
                        value: name,
                        span: name_span,
                    },
                    value,
                ));
            }
        }
        Ok((false, args))
    }

    /// Parses a scalar function argument list: a `NIL` or a parenthesized list
    /// of comma-separated positional expressions. Arity is not validated here.
    ///
    /// ```text
    /// ArgList ::= NIL | '(' Expression ( ',' Expression )* ')'
    /// ```
    fn parse_scalar_args(&mut self) -> Result<Vec<FunctionArg<'a>>, SparqlSyntaxError> {
        if self.is_nil() {
            self.parse_nil()?;
            return Ok(Vec::new());
        }
        self.expect_operator("(")?;
        let mut args = Vec::new();
        if !self.peek_operator(")") {
            args.push(FunctionArg::Positional(self.parse_expr()?));
            while self.consume_operator(",") {
                if self.peek_operator(")") {
                    break;
                }
                args.push(FunctionArg::Positional(self.parse_expr()?));
            }
        }
        self.expect_operator(")")?;
        Ok(args)
    }

    /// Parses an `ExpressionList` (used by `IN`, `NOT IN`, `CONCAT`,
    /// `COALESCE`).
    ///
    /// ```text
    /// ExpressionList ::= NIL | '(' Expression ( ',' Expression )* ')'
    /// ```
    fn parse_expression_list(
        &mut self,
    ) -> Result<Vec<Spanned<Expression<'a>>>, SparqlSyntaxError> {
        if self.is_nil() {
            self.parse_nil()?;
            return Ok(Vec::new());
        }
        self.expect_operator("(")?;
        let mut items = vec![self.parse_expr()?];
        while self.consume_operator(",") {
            items.push(self.parse_expr()?);
        }
        self.expect_operator(")")?;
        Ok(items)
    }

    /// Returns the binary infix operator at the current token (if any) along
    /// with its binding powers.
    fn peek_binary_infix(&self) -> Option<(InfixOp, u8, u8)> {
        let op = match self.peek().map(|token| token.value) {
            Some(Token::Operator("||")) => InfixOp::Or,
            Some(Token::Operator("&&")) => InfixOp::And,
            Some(Token::Operator("=")) => InfixOp::Equal,
            Some(Token::Operator("!=")) => InfixOp::NotEqual,
            Some(Token::Operator("<")) => InfixOp::Less,
            Some(Token::Operator("<=")) => InfixOp::LessOrEqual,
            Some(Token::Operator(">")) => InfixOp::Greater,
            Some(Token::Operator(">=")) => InfixOp::GreaterOrEqual,
            Some(Token::Operator("+")) => InfixOp::Add,
            Some(Token::Operator("-")) => InfixOp::Subtract,
            Some(Token::Operator("*")) => InfixOp::Multiply,
            Some(Token::Operator("/")) => InfixOp::Divide,
            _ => return None,
        };
        let (l_bp, r_bp) = op.binding_power();
        Some((op, l_bp, r_bp))
    }

    /// Returns the binary infix operator and unsigned literal for signed numeric tokens
    /// (e.g. `+10` or `-5`) along with its binding powers.
    fn peek_binary_signed_numeric(
        &self,
    ) -> Result<Option<(InfixOp, crate::ast::Literal<'a>, u8, u8)>, SparqlSyntaxError>
    {
        let numeric_span = |span: Span| Span::new(span.start + 1, span.end);
        let (op, unsigned) = match self.peek().map(|t| (t.value, t.span)) {
            Some((Token::IntegerPositive(s), span)) => (
                InfixOp::Add,
                crate::ast::Literal::Integer(
                    Spanned {
                        value: s[1..]
                            .parse()
                            .map_err(|e| self.numeric_error(numeric_span(span), e))?,
                        span: numeric_span(span),
                    },
                    Cow::Borrowed(&s[1..]),
                ),
            ),
            Some((Token::DecimalPositive(s), span)) => (
                InfixOp::Add,
                crate::ast::Literal::Decimal(
                    Spanned {
                        value: s[1..]
                            .parse()
                            .map_err(|e| self.numeric_error(numeric_span(span), e))?,
                        span: numeric_span(span),
                    },
                    Cow::Borrowed(&s[1..]),
                ),
            ),
            Some((Token::DoublePositive(s), span)) => (
                InfixOp::Add,
                crate::ast::Literal::Double(
                    Spanned {
                        value: s[1..]
                            .parse()
                            .map_err(|e| self.numeric_error(numeric_span(span), e))?,
                        span: numeric_span(span),
                    },
                    Cow::Borrowed(&s[1..]),
                ),
            ),
            Some((Token::IntegerNegative(s), span)) => (
                InfixOp::Subtract,
                crate::ast::Literal::Integer(
                    Spanned {
                        value: s[1..]
                            .parse()
                            .map_err(|e| self.numeric_error(numeric_span(span), e))?,
                        span: numeric_span(span),
                    },
                    Cow::Borrowed(&s[1..]),
                ),
            ),
            Some((Token::DecimalNegative(s), span)) => (
                InfixOp::Subtract,
                crate::ast::Literal::Decimal(
                    Spanned {
                        value: s[1..]
                            .parse()
                            .map_err(|e| self.numeric_error(numeric_span(span), e))?,
                        span: numeric_span(span),
                    },
                    Cow::Borrowed(&s[1..]),
                ),
            ),
            Some((Token::DoubleNegative(s), span)) => (
                InfixOp::Subtract,
                crate::ast::Literal::Double(
                    Spanned {
                        value: s[1..]
                            .parse()
                            .map_err(|e| self.numeric_error(numeric_span(span), e))?,
                        span: numeric_span(span),
                    },
                    Cow::Borrowed(&s[1..]),
                ),
            ),
            _ => return Ok(None),
        };
        let (l_bp, r_bp) = op.binding_power();
        Ok(Some((op, unsigned, l_bp, r_bp)))
    }

    /// Builds a diagnostic error for a numeric literal that failed to parse (e.g. an
    /// integer or decimal that overflows its target type).
    fn numeric_error(&self, span: Span, e: impl std::fmt::Display) -> SparqlSyntaxError {
        SparqlSyntaxError::single(Diagnostic::error(
            span,
            format!("Invalid numeric literal: {e}"),
        ))
    }

    /// Returns `Some(negated)` if the current token is `IN` or `NOT IN`.
    fn peek_in(&self) -> Option<bool> {
        if self.peek_keyword("IN") {
            return Some(false);
        }
        if self.peek_keyword("NOT") && self.peek_keyword_at(1, "IN") {
            return Some(true);
        }
        None
    }

    /// Consumes the `IN` or `NOT IN` operator.
    fn consume_in(&mut self, negated: bool) -> Result<(), SparqlSyntaxError> {
        if negated {
            self.expect_keyword("NOT")?;
            self.expect_keyword("IN")?;
        } else {
            self.expect_keyword("IN")?;
        }
        Ok(())
    }

    /// Parses a `BuiltInCall` (including `ExistsFunc`, `NotExistsFunc` and
    /// aggregates), returning a plain (non-spanned) expression; the caller
    /// attaches the span.
    ///
    /// ```text
    /// BuiltInCall ::= Aggregate | 'STR' '(' Expression ')' | ... | ExistsFunc
    ///                 | NotExistsFunc
    /// ```
    fn parse_built_in_call(
        &mut self,
    ) -> Result<Spanned<Expression<'a>>, SparqlSyntaxError> {
        let Some(kw) = self.peek_keyword_value() else {
            return self.expected("a keyword");
        };

        let start = self.current_span().start;
        let kw_span = self.current_span();
        let value = if kw.eq_ignore_ascii_case("EXISTS") {
            self.bump();
            let pattern = self.parse_group_graph_pattern()?;
            Expression::Exists(Box::new(pattern))
        } else if kw.eq_ignore_ascii_case("NOT") && self.peek_keyword_at(1, "EXISTS") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            let pattern = self.parse_group_graph_pattern()?;
            Expression::NotExists(Box::new(pattern))
        } else {
            let kind = self.check_builtin_call_kind(kw);
            self.bump();
            self.parse_call(
                FunctionName::BuiltIn(Spanned {
                    value: kw,
                    span: kw_span,
                }),
                kind,
            )?
        };

        let end = self.previous().map(|token| token.span.end).unwrap_or(start);
        Ok(Spanned {
            value,
            span: Span::new(start, end),
        })
    }

    /// Returns the value of the current token if it is a keyword.
    fn peek_keyword_value(&self) -> Option<&'a str> {
        match self.peek().map(|token| token.value) {
            Some(Token::Keyword(kw)) => Some(kw),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::SparqlParser;
    use crate::parser::test_helpers::*;

    fn render_e(input: &str) -> String {
        render(&parse(input, |p| p.parse_expr()))
    }

    /// Returns `true` if `input` is not a complete, well-formed expression:
    /// either parsing fails, or the whole input was not consumed (e.g. a
    /// trailing `)` is left over).
    fn is_invalid_expr(input: &str) -> bool {
        let mut p = SparqlParser::new(input, default_registry());
        p.parse_expr().is_err() || !p.at_end()
    }

    #[test]
    fn primary_atoms() {
        assert_eq!(render_e("?x"), "?x");
        assert_eq!(render_e("<a>"), "<a>");
        assert_eq!(render_e("ex:a"), "ex:a");
        assert_eq!(render_e("42"), "42");
        assert_eq!(render_e("\"hi\""), "\"hi\"");
        assert_eq!(render_e("true"), "true");
    }

    #[test]
    fn binary() {
        assert_eq!(render_e("?a + ?b"), "(?a + ?b)");
        assert_eq!(render_e("?a - ?b"), "(?a - ?b)");
        assert_eq!(render_e("?a * ?b"), "(?a * ?b)");
        assert_eq!(render_e("?a / ?b"), "(?a / ?b)");
        assert_eq!(render_e("?a < ?b"), "(?a < ?b)");
        assert_eq!(render_e("?a <= ?b"), "(?a <= ?b)");
        assert_eq!(render_e("?a > ?b"), "(?a > ?b)");
        assert_eq!(render_e("?a >= ?b"), "(?a >= ?b)");
        assert_eq!(render_e("?a = ?b"), "(?a = ?b)");
        assert_eq!(render_e("?a != ?b"), "(?a != ?b)");
        assert_eq!(render_e("?a && ?b"), "(?a && ?b)");
        assert_eq!(render_e("?a || ?b"), "(?a || ?b)");
    }

    #[test]
    fn precedence_add_over_mul() {
        assert_eq!(render_e("1 + 2 * 3"), "(1 + (2 * 3))");
        assert_eq!(render_e("1 * 2 + 3"), "((1 * 2) + 3)");
    }

    #[test]
    fn precedence_logical() {
        assert_eq!(render_e("?a + ?b < ?c * ?d"), "((?a + ?b) < (?c * ?d))");
        assert_eq!(render_e("?a < ?b && ?c = ?d"), "((?a < ?b) && (?c = ?d))");
        assert_eq!(render_e("?a && ?b || ?c"), "((?a && ?b) || ?c)");
    }

    #[test]
    fn left_assoc() {
        assert_eq!(render_e("1 - 2 - 3"), "((1 - 2) - 3)");
        assert_eq!(render_e("1 / 2 / 3"), "((1 / 2) / 3)");
    }

    #[test]
    fn parens() {
        assert_eq!(render_e("(1 + 2) * 3"), "((1 + 2) * 3)");
    }

    #[test]
    fn invalid_parens() {
        // Control: balanced parens are fine.
        assert_eq!(render_e("(?x + 1)"), "(?x + 1)");
        assert!(!is_invalid_expr("(?x + 1)"), "balanced parens are valid");

        // Extra closing parenthesis.
        assert!(
            is_invalid_expr("(?x + 1))"),
            "extra `)` after a closed group must fail"
        );
        assert!(
            is_invalid_expr("(x + 1))"),
            "extra `)` after a closed group must fail"
        );

        // Unclosed opening parenthesis.
        assert!(is_invalid_expr("(?x + 1"), "unclosed `(` must fail");
        insta::assert_snapshot!(render_err("(?x + 1", |p| p.parse_expr().err().unwrap()), @"
        error: expected ), found `end of input`
          ┌─ :1:8
          │
        1 │ (?x + 1
          │        ^ expected ), found `end of input`
        ");

        // Closing parenthesis with nothing to close.
        assert!(is_invalid_expr(")(?x"), "leading `)` must fail");
        insta::assert_snapshot!(render_err(")(?x", |p| p.parse_expr().err().unwrap()), @"
        error: expected an expression, found `)`
          ┌─ :1:1
          │
        1 │ )(?x
          │ ^ expected an expression, found `)`
        ");
    }

    #[test]
    fn unary() {
        assert_eq!(render_e("-?a"), "-?a");
        assert_eq!(render_e("+?a"), "+?a");
        assert_eq!(render_e("!?a"), "!?a");
        assert_eq!(render_e("-?a + ?b"), "(-?a + ?b)");
        assert_eq!(render_e("!?a = ?b"), "(!?a = ?b)");
        assert_eq!(render_e("--1"), "--1");
        assert_eq!(render_e("-42"), "-42");
    }

    #[test]
    fn relational_non_associative() {
        assert!(is_invalid_expr("?a = ?b = ?c"));
        assert!(is_invalid_expr("?a < ?b < ?c"));
        assert!(is_invalid_expr("?a = ?b > ?c"));
        assert!(is_invalid_expr("?a != ?b != ?c"));
        assert!(is_invalid_expr("(1 = 2) = 3"));

        assert_eq!(render_e("?a = ?b"), "(?a = ?b)");
        assert_eq!(render_e("?a < ?b"), "(?a < ?b)");
    }

    #[test]
    fn aggregate_multiple_positional_args() {
        assert_eq!(render_e("COUNT(?a)"), "COUNT(?a)");
        assert_eq!(render_e("COUNT(*)"), "COUNT(*)");
        assert_eq!(
            render_e("GROUP_CONCAT(?a; SEPARATOR = \";\")"),
            "GROUP_CONCAT(?a; SEPARATOR = \";\")"
        );
        assert!(is_invalid_expr("COUNT(?a, ?b)"));
        assert!(is_invalid_expr("GROUP_CONCAT(?a, ?b; SEPARATOR = \";\")"));
    }

    #[test]
    fn in_and_not_in() {
        assert_eq!(render_e("?a IN (1, 2)"), "(?a IN (1, 2))");
        assert_eq!(render_e("?a NOT IN (1)"), "(?a NOT IN (1))");
        assert_eq!(render_e("?a IN (1) || ?b"), "((?a IN (1)) || ?b)");
        assert_eq!(render_e("1 + ?a IN (1)"), "((1 + ?a) IN (1))");
    }

    #[test]
    fn function_and_iri() {
        use rdf_fusion_encoding::RdfFusionEncodings;
        use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
        use rdf_fusion_encoding::string::STRING_ENCODING;
        use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
        use rdf_fusion_extensions::functions::RdfFusionFunctionRegistry;
        use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
        use rdf_fusion_functions::scalar::terms::str_udf;
        use std::sync::Arc;

        // A function registered under the full IRI that `ex:f` resolves to, so
        // the (now registry-driven) parser recognizes it as a scalar function.
        fn render_with_prefix(input: &str) -> String {
            let encodings = RdfFusionEncodings::new(
                Arc::clone(&PLAIN_TERM_ENCODING),
                Arc::new(TypedFamilyEncoding::default()),
                None,
                Arc::clone(&STRING_ENCODING),
            );
            let registry: Arc<dyn RdfFusionFunctionRegistry> =
                Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone()));
            let udf = str_udf(encodings)
                .expect("failed to create STR UDF")
                .with_aliases(["http://example.org/f"]);
            registry.register_udf(Arc::new(udf));

            let mut p = SparqlParser::new(input, registry);
            p.prefixes
                .insert("ex".to_string(), "http://example.org/".to_string());
            let expr = p.parse_expr().expect("expected ok");
            assert!(
                p.at_end(),
                "parser did not consume all input: {}",
                p.found()
            );
            render(&expr)
        }

        assert_eq!(render_with_prefix("ex:f(?a, ?b)"), "ex:f(?a, ?b)");
        assert_eq!(render_with_prefix("ex:f()"), "ex:f()");

        // Named (`;`) parameters are aggregate-only; a scalar UDF rejects them.
        let mut p = SparqlParser::new("ex:f(?a; OPTION = 42)", {
            let encodings = RdfFusionEncodings::new(
                Arc::clone(&PLAIN_TERM_ENCODING),
                Arc::new(TypedFamilyEncoding::default()),
                None,
                Arc::clone(&STRING_ENCODING),
            );
            let registry: Arc<dyn RdfFusionFunctionRegistry> =
                Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone()));
            let udf = str_udf(encodings)
                .expect("failed to create STR UDF")
                .with_aliases(["http://example.org/f"]);
            registry.register_udf(Arc::new(udf));
            registry
        });
        p.prefixes
            .insert("ex".to_string(), "http://example.org/".to_string());
        assert!(
            p.parse_expr().is_err(),
            "`;` named parameters are not allowed on scalar functions"
        );
    }

    #[test]
    fn built_ins() {
        assert_eq!(render_e("STR(?a)"), "STR(?a)");
        assert_eq!(render_e("CONTAINS(?a, ?b)"), "CONTAINS(?a, ?b)");
        assert_eq!(render_e("BOUND(?a)"), "BOUND(?a)");
        assert_eq!(render_e("IF(?a, ?b, ?c)"), "IF(?a, ?b, ?c)");
        assert_eq!(render_e("REGEX(?a, \"x\")"), "REGEX(?a, \"x\")");
        assert_eq!(render_e("CONCAT(?a, ?b)"), "CONCAT(?a, ?b)");
        assert_eq!(render_e("NOW()"), "NOW()");
        assert_eq!(render_e("BNODE(?a)"), "BNODE(?a)");
    }

    #[test]
    fn aggregates() {
        assert_eq!(render_e("COUNT(?a)"), "COUNT(?a)");
        assert_eq!(render_e("COUNT(*)"), "COUNT(*)");
        assert_eq!(render_e("SUM(DISTINCT ?a)"), "SUM(DISTINCT ?a)");
        assert_eq!(render_e("AVG(?a)"), "AVG(?a)");
        assert_eq!(
            render_e("GROUP_CONCAT(?a; SEPARATOR = \";\")"),
            "GROUP_CONCAT(?a; SEPARATOR = \";\")"
        );
    }

    #[test]
    fn distinct_only_for_aggregates() {
        assert_eq!(render_e("COUNT(DISTINCT ?a)"), "COUNT(DISTINCT ?a)");
        assert_eq!(render_e("SUM(DISTINCT ?a)"), "SUM(DISTINCT ?a)");
        assert!(
            SparqlParser::new("STR(DISTINCT ?a)", default_registry())
                .parse_expr()
                .is_err(),
            "DISTINCT is not allowed for non-aggregate functions"
        );
    }

    #[test]
    fn exists() {
        assert_eq!(render_e("EXISTS { ?s ?p ?o }"), "EXISTS { ?s ?p ?o }");
        assert_eq!(
            render_e("NOT EXISTS { ?s ?p ?o }"),
            "NOT EXISTS { ?s ?p ?o }"
        );
        assert_eq!(render_e("exists { ?s ?p ?o }"), "EXISTS { ?s ?p ?o }");
        assert_eq!(
            render_e("not exists { ?s ?p ?o }"),
            "NOT EXISTS { ?s ?p ?o }"
        );
    }

    #[test]
    fn infix_signed_numbers() {
        assert_eq!(render_e("?o+10"), "(?o + 10)");
        assert_eq!(render_e("?o+1"), "(?o + 1)");
        assert_eq!(render_e("?s+57"), "(?s + 57)");
        assert_eq!(render_e("?o-10"), "(?o - 10)");
        assert_eq!(render_e("?o+10*2"), "(?o + (10 * 2))");
    }

    #[test]
    fn registry_dispatch() {
        use crate::ast::Expression;
        use rdf_fusion_encoding::RdfFusionEncodings;
        use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
        use rdf_fusion_encoding::string::STRING_ENCODING;
        use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
        use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
        use std::sync::Arc;

        let encodings = RdfFusionEncodings::new(
            Arc::clone(&PLAIN_TERM_ENCODING),
            Arc::new(TypedFamilyEncoding::default()),
            None,
            Arc::clone(&STRING_ENCODING),
        );
        let registry: Arc<
            dyn rdf_fusion_extensions::functions::RdfFusionFunctionRegistry,
        > = Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings));

        // XSD cast IRIs resolve to scalar functions via the registry aliases
        for query in [
            "xsd:string(?x)",
            "xsd:integer(?x)",
            "xsd:int(?x)",
            "xsd:float(?x)",
            "xsd:double(?x)",
            "xsd:decimal(?x)",
            "xsd:dateTime(?x)",
            "xsd:boolean(?x)",
        ] {
            let mut p = SparqlParser::new(query, Arc::clone(&registry));
            p.prefixes.insert(
                "xsd".to_string(),
                "http://www.w3.org/2001/XMLSchema#".to_string(),
            );
            let Ok(expr) = p.parse_expr() else {
                panic!("expected {query} to parse as a scalar function")
            };
            assert!(
                matches!(expr.value, Expression::Function(_)),
                "expected {query} to be a Function"
            );
        }

        // Built-in aggregate recognized
        let mut p = SparqlParser::new("COUNT(?x)", Arc::clone(&registry));
        let Ok(expr) = p.parse_expr() else {
            panic!("expected ok")
        };
        assert!(matches!(expr.value, Expression::Aggregate(_)));

        // Built-in scalar recognized
        let mut p = SparqlParser::new("STR(?x)", Arc::clone(&registry));
        let Ok(expr) = p.parse_expr() else {
            panic!("expected ok")
        };
        assert!(matches!(expr.value, Expression::Function(_)));

        // Case-insensitive built-in scalar recognized
        let mut p = SparqlParser::new("isiri(?x)", Arc::clone(&registry));
        let Ok(expr) = p.parse_expr() else {
            panic!("expected ok")
        };
        assert!(matches!(expr.value, Expression::Function(_)));

        // Unknown function fails with error pointing to the span
        let mut p = SparqlParser::new("UNKNOWN_FN(?x)", Arc::clone(&registry));
        let Err(err) = p.parse_expr() else {
            panic!("expected err")
        };
        assert_eq!(err.0[0].diagnostics[0].span.start, 0);
        assert_eq!(err.0[0].diagnostics[0].span.end, 10);
        assert!(err.0[0].diagnostics[0].message.contains("UNKNOWN_FN"));

        // Unknown IRI function fails with error pointing to the span
        let mut p = SparqlParser::new("<http://example.org/unknown>(?x)", registry);
        let Err(err) = p.parse_expr() else {
            panic!("expected err")
        };
        assert!(
            err.0[0].diagnostics[0]
                .message
                .contains("http://example.org/unknown")
        );
    }
}
