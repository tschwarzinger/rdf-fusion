//! Explicit SPARQL pretty-printing for the AST.

use crate::ast::{
    Aggregate, AskQuery, BlankNode, ConstructQuery, DataBlockValue, DescribeQuery,
    DescribeTargets, Expression, Function, FunctionArg, FunctionName, GraphClause,
    GraphNode, GraphNodePath, GraphOrDefault, GraphPattern, GraphPatternElement,
    GraphRefAll, Iri, IriRef, LimitOffsetClauses, Literal, Object, ObjectPath,
    OrderCondition, Path, PathOneInPropertySet, PrefixedName, PrologueDecl, PropertyList,
    PropertyListPath, QuadPatterns, Query, QueryQuery, SelectClause, SelectQuery,
    SelectVariables, SelectionOption, SolutionModifier, String as AstString, SubSelect,
    Update, Update1, ValuesClause, Var, VarOrIri, VarOrPath, VarOrTerm, Verb,
};
use crate::span::Spanned;
use itertools::Itertools;
use std::fmt;
use std::fmt::{Display, Formatter};

/// Creates a new [`SparqlPrettyPrinter`] that can pretty-print AST nodes.
pub fn pretty_printable<TNode: SparqlPrettyPrintable>(
    node: TNode,
) -> SparqlPrettyPrinter<TNode> {
    SparqlPrettyPrinter(node)
}

/// A value that can be pretty-printed in the context of a SPARQL query.
pub trait SparqlPrettyPrintable {
    /// Pretty-print this value to `f`.
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result;
}

/// A wrapper around a pretty-printable value that implements [`Display`]. See [`pretty_printable`].
pub struct SparqlPrettyPrinter<T: ?Sized>(pub T);

impl<T: ?Sized + SparqlPrettyPrintable> Display for SparqlPrettyPrinter<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.pretty_print(f)
    }
}

impl<T: ?Sized + SparqlPrettyPrintable> SparqlPrettyPrintable for &T {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        (*self).pretty_print(f)
    }
}

impl<T: ?Sized + SparqlPrettyPrintable> SparqlPrettyPrintable for Box<T> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        (**self).pretty_print(f)
    }
}

/// Rendering a [`Spanned`] value forwards to its inner value, ignoring the span.
impl<T: SparqlPrettyPrintable> SparqlPrettyPrintable for Spanned<T> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.value.pretty_print(f)
    }
}

impl SparqlPrettyPrintable for Iri<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Iri::IriRef(iri) => iri.pretty_print(f),
            Iri::PrefixedName(name) => name.pretty_print(f),
        }
    }
}

impl SparqlPrettyPrintable for IriRef<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "<{}>", self.value)
    }
}

impl SparqlPrettyPrintable for PrefixedName<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.namespace.is_empty() {
            write!(f, ":")?;
        } else {
            write!(f, "{}:", self.namespace)?;
        }
        for c in self.local.chars() {
            if matches!(
                c,
                '_' | '~'
                    | '.'
                    | '-'
                    | '!'
                    | '$'
                    | '&'
                    | '\''
                    | '('
                    | ')'
                    | '*'
                    | '+'
                    | ','
                    | ';'
                    | '='
                    | '/'
                    | '?'
                    | '#'
                    | '@'
                    | '%'
            ) {
                write!(f, "\\{c}")?;
            } else {
                write!(f, "{c}")?;
            }
        }
        Ok(())
    }
}

impl SparqlPrettyPrintable for Var<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "?{}", self.value)
    }
}

impl SparqlPrettyPrintable for AstString<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "\"")?;
        for c in self.value.chars() {
            match c {
                '\\' => write!(f, "\\\\")?,
                '"' => write!(f, "\\\"")?,
                '\n' => write!(f, "\\n")?,
                '\r' => write!(f, "\\r")?,
                '\t' => write!(f, "\\t")?,
                _ => write!(f, "{c}")?,
            }
        }
        write!(f, "\"")
    }
}

impl SparqlPrettyPrintable for BlankNode<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "_:{}", self.0.unwrap_or(""))
    }
}

impl SparqlPrettyPrintable for Literal<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Boolean(b) => write!(f, "{}", b.value),
            Literal::Integer(_n, lexeme) => write!(f, "{lexeme}"),
            Literal::Decimal(_n, lexeme) => write!(f, "{lexeme}"),
            Literal::Double(_n, lexeme) => write!(f, "{lexeme}"),
            Literal::String(s) => s.pretty_print(f),
            Literal::LangString(s, lang) => {
                s.pretty_print(f)?;
                write!(f, "@{}", lang.value)
            }
            Literal::Typed(s, iri) => {
                s.pretty_print(f)?;
                write!(f, "^^")?;
                iri.pretty_print(f)
            }
        }
    }
}

impl SparqlPrettyPrintable for VarOrTerm<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            VarOrTerm::Var(v) => v.pretty_print(f),
            VarOrTerm::Iri(i) => i.pretty_print(f),
            VarOrTerm::Literal(l) => l.pretty_print(f),
            VarOrTerm::BlankNode(b) => b.pretty_print(f),
            VarOrTerm::Nil => write!(f, "()"),
        }
    }
}

impl SparqlPrettyPrintable for Verb<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Verb::A => write!(f, "a"),
            Verb::Var(v) => v.pretty_print(f),
            Verb::Iri(i) => i.pretty_print(f),
        }
    }
}

impl SparqlPrettyPrintable for Object<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.graph_node.pretty_print(f)
    }
}

impl SparqlPrettyPrintable for GraphNode<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GraphNode::VarOrTerm(t) => t.pretty_print(f),
            GraphNode::Collection(items) => {
                write!(f, "(")?;
                write!(
                    f,
                    "{}",
                    items.value.iter().map(pretty_printable).format(" ")
                )?;
                write!(f, ")")
            }
            GraphNode::BlankNodePropertyList(props) => {
                write!(f, "[")?;
                props.value.pretty_print(f)?;
                write!(f, "]")
            }
        }
    }
}

/// Renders a property list as semicolon-separated `verb object...` groups, with
/// the objects of each verb space-separated.
fn render_property_groups<'a, V, O>(
    f: &mut Formatter<'_>,
    groups: impl IntoIterator<Item = &'a (V, Vec<O>)>,
) -> fmt::Result
where
    V: SparqlPrettyPrintable + 'a,
    O: SparqlPrettyPrintable + 'a,
{
    for (i, (verb, objects)) in groups.into_iter().enumerate() {
        if i > 0 {
            write!(f, " ; ")?;
        }
        write!(
            f,
            "{} {}",
            pretty_printable(verb),
            objects.iter().map(pretty_printable).format(" ")
        )?;
    }
    Ok(())
}

impl SparqlPrettyPrintable for Path<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render_path(f, self, 0)
    }
}

/// Precedence of a [`Path`] node, used when re-inserting parentheses during
/// pretty-printing (lower binds looser).
const ALT_LEVEL: u8 = 1;
const SEQ_LEVEL: u8 = 2;
const UNARY_LEVEL: u8 = 3;
const ATOM_LEVEL: u8 = 4;

/// Renders `path`, surrounding it in parentheses if its precedence is weaker
/// than `parent_level` (the context it appears in).
fn render_path(f: &mut Formatter<'_>, path: &Path<'_>, parent_level: u8) -> fmt::Result {
    match path {
        Path::Alternative(left, right) => write_parens(f, ALT_LEVEL, parent_level, |f| {
            render_path(f, left, ALT_LEVEL)?;
            write!(f, "|")?;
            render_path(f, right, ALT_LEVEL)
        }),
        Path::Sequence(left, right) => write_parens(f, SEQ_LEVEL, parent_level, |f| {
            render_path(f, left, SEQ_LEVEL)?;
            write!(f, "/")?;
            render_path(f, right, SEQ_LEVEL)
        }),
        Path::Inverse(path) => write_parens(f, UNARY_LEVEL, parent_level, |f| {
            write!(f, "^")?;
            render_path(f, path, UNARY_LEVEL)
        }),
        Path::ZeroOrOne(path) => write_parens(f, UNARY_LEVEL, parent_level, |f| {
            render_path(f, path, UNARY_LEVEL)?;
            write!(f, "?")
        }),
        Path::ZeroOrMore(path) => write_parens(f, UNARY_LEVEL, parent_level, |f| {
            render_path(f, path, UNARY_LEVEL)?;
            write!(f, "*")
        }),
        Path::OneOrMore(path) => write_parens(f, UNARY_LEVEL, parent_level, |f| {
            render_path(f, path, UNARY_LEVEL)?;
            write!(f, "+")
        }),
        Path::Iri(iri) => {
            write_parens(f, ATOM_LEVEL, parent_level, |f| iri.pretty_print(f))
        }
        Path::A => write_parens(f, ATOM_LEVEL, parent_level, |f| write!(f, "a")),
        Path::NegatedPropertySet(entries) => {
            write_parens(f, ATOM_LEVEL, parent_level, |f| {
                write!(f, "!")?;
                if entries.len() == 1 {
                    entries[0].pretty_print(f)
                } else {
                    write!(f, "(")?;
                    for (i, entry) in entries.iter().enumerate() {
                        if i > 0 {
                            write!(f, "|")?;
                        }
                        entry.pretty_print(f)?;
                    }
                    write!(f, ")")
                }
            })
        }
    }
}

/// Writes `body` wrapped in parentheses if `level < parent_level`.
fn write_parens(
    f: &mut Formatter<'_>,
    level: u8,
    parent_level: u8,
    body: impl FnOnce(&mut Formatter<'_>) -> fmt::Result,
) -> fmt::Result {
    if level < parent_level {
        write!(f, "(")?;
        body(f)?;
        write!(f, ")")
    } else {
        body(f)
    }
}

impl SparqlPrettyPrintable for PathOneInPropertySet<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PathOneInPropertySet::Iri(iri) => iri.pretty_print(f),
            PathOneInPropertySet::A => write!(f, "a"),
            PathOneInPropertySet::InverseIri(iri) => {
                write!(f, "^")?;
                iri.pretty_print(f)
            }
            PathOneInPropertySet::InverseA => write!(f, "^a"),
        }
    }
}

impl SparqlPrettyPrintable for VarOrPath<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            VarOrPath::Var(v) => v.pretty_print(f),
            VarOrPath::Path(p) => p.pretty_print(f),
        }
    }
}

impl SparqlPrettyPrintable for ObjectPath<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.graph_node.pretty_print(f)
    }
}

impl SparqlPrettyPrintable for GraphNodePath<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GraphNodePath::VarOrTerm(t) => t.pretty_print(f),
            GraphNodePath::Collection(items) => {
                write!(f, "(")?;
                write!(
                    f,
                    "{}",
                    items.value.iter().map(pretty_printable).format(" ")
                )?;
                write!(f, ")")
            }
            GraphNodePath::BlankNodePropertyList(props) => {
                write!(f, "[")?;
                props.pretty_print(f)?;
                write!(f, "]")
            }
        }
    }
}

impl SparqlPrettyPrintable for PropertyListPath<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render_property_groups(f, self.iter())
    }
}

impl SparqlPrettyPrintable for GraphPattern<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GraphPattern::Group(elements) => {
                write!(f, "{{")?;
                for element in elements {
                    write!(f, " ")?;
                    element.pretty_print(f)?;
                }
                write!(f, " }}")
            }
            GraphPattern::SubSelect(sub_select) => {
                write!(f, "{{ ")?;
                sub_select.pretty_print(f)?;
                write!(f, " }}")
            }
        }
    }
}

impl SparqlPrettyPrintable for SubSelect<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.select_clause.pretty_print(f)?;
        write!(f, " WHERE ")?;
        self.where_clause.pretty_print(f)?;
        render_solution_modifier(f, &self.solution_modifier)?;
        if let Some(values) = &self.values_clause {
            write!(f, " ")?;
            values.pretty_print(f)?;
        }
        Ok(())
    }
}

impl SparqlPrettyPrintable for GraphPatternElement<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GraphPatternElement::Triples(triples) => {
                for (i, (subject, properties)) in triples.iter().enumerate() {
                    if i > 0 {
                        write!(f, " . ")?;
                    }
                    subject.pretty_print(f)?;
                    for (j, (verb, objects)) in properties.iter().enumerate() {
                        if j > 0 {
                            write!(f, " ;")?;
                        }
                        write!(f, " {}", pretty_printable(verb))?;
                        write!(
                            f,
                            " {}",
                            objects.iter().map(pretty_printable).format(", ")
                        )?;
                    }
                }
                Ok(())
            }
            GraphPatternElement::Optional(pattern) => {
                write!(f, "OPTIONAL ")?;
                pattern.pretty_print(f)
            }
            GraphPatternElement::Minus(pattern) => {
                write!(f, "MINUS ")?;
                pattern.pretty_print(f)
            }
            GraphPatternElement::Union(patterns) => {
                for (i, pattern) in patterns.iter().enumerate() {
                    if i > 0 {
                        write!(f, " UNION ")?;
                    }
                    pattern.pretty_print(f)?;
                }
                Ok(())
            }
            GraphPatternElement::Filter(e) => {
                write!(f, "FILTER ")?;
                let already_has_parens = matches!(
                    e.value,
                    Expression::Function(_)
                        | Expression::Exists(_)
                        | Expression::NotExists(_)
                        | Expression::Equal(..)
                        | Expression::NotEqual(..)
                        | Expression::Less(..)
                        | Expression::LessOrEqual(..)
                        | Expression::Greater(..)
                        | Expression::GreaterOrEqual(..)
                        | Expression::And(..)
                        | Expression::Or(..)
                        | Expression::Add(..)
                        | Expression::Subtract(..)
                        | Expression::Multiply(..)
                        | Expression::Divide(..)
                        | Expression::In(..)
                        | Expression::NotIn(..)
                );
                if !already_has_parens {
                    write!(f, "(")?;
                }
                e.pretty_print(f)?;
                if !already_has_parens {
                    write!(f, ")")?;
                }
                Ok(())
            }
            GraphPatternElement::Bind(e, var) => {
                write!(f, "BIND(")?;
                e.pretty_print(f)?;
                write!(f, " AS ")?;
                var.pretty_print(f)?;
                write!(f, ")")
            }
            GraphPatternElement::Service {
                silent,
                name,
                pattern,
            } => {
                write!(f, "SERVICE ")?;
                if *silent {
                    write!(f, "SILENT ")?;
                }
                name.pretty_print(f)?;
                write!(f, " ")?;
                pattern.pretty_print(f)
            }
            GraphPatternElement::Graph { name, pattern } => {
                write!(f, "GRAPH ")?;
                name.pretty_print(f)?;
                write!(f, " ")?;
                pattern.pretty_print(f)
            }
            GraphPatternElement::Values(values) => values.pretty_print(f),
        }
    }
}

impl SparqlPrettyPrintable for VarOrIri<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            VarOrIri::Var(v) => v.pretty_print(f),
            VarOrIri::Iri(i) => i.pretty_print(f),
        }
    }
}

impl SparqlPrettyPrintable for ValuesClause<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "VALUES ")?;
        match self.variables.len() {
            // An `InlineDataOneVar` block: bare variable and bare values.
            1 => {
                self.variables[0].pretty_print(f)?;
                write!(f, " {{")?;
                for row in self.values.value.iter() {
                    write!(f, " ")?;
                    if let Some(value) = row.first() {
                        value.pretty_print(f)?;
                    }
                }
                write!(f, " }}")
            }
            // An `InlineDataFull` block: parenthesized variables and rows.
            _ => {
                write!(f, "(")?;
                for (i, var) in self.variables.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    var.pretty_print(f)?;
                }
                write!(f, ") {{")?;
                for row in self.values.value.iter() {
                    write!(f, " ")?;
                    if row.is_empty() {
                        write!(f, "()")?;
                    } else {
                        write!(f, "(")?;
                        for (j, value) in row.iter().enumerate() {
                            if j > 0 {
                                write!(f, " ")?;
                            }
                            value.pretty_print(f)?;
                        }
                        write!(f, ")")?;
                    }
                }
                write!(f, " }}")
            }
        }
    }
}

impl SparqlPrettyPrintable for DataBlockValue<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DataBlockValue::Iri(iri) => iri.pretty_print(f),
            DataBlockValue::Literal(literal) => literal.pretty_print(f),
            DataBlockValue::Undef => write!(f, "UNDEF"),
        }
    }
}

impl SparqlPrettyPrintable for PropertyList<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render_property_groups(f, self.iter())
    }
}

impl SparqlPrettyPrintable for Expression<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Or(l, r) => {
                write!(f, "({} || {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::And(l, r) => {
                write!(f, "({} && {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::Equal(l, r) => {
                write!(f, "({} = {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::NotEqual(l, r) => {
                write!(f, "({} != {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::Less(l, r) => {
                write!(f, "({} < {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::LessOrEqual(l, r) => {
                write!(f, "({} <= {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::Greater(l, r) => {
                write!(f, "({} > {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::GreaterOrEqual(l, r) => {
                write!(f, "({} >= {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::Add(l, r) => {
                write!(f, "({} + {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::Subtract(l, r) => {
                write!(f, "({} - {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::Multiply(l, r) => {
                write!(f, "({} * {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::Divide(l, r) => {
                write!(f, "({} / {})", pretty_printable(&l), pretty_printable(&r))
            }
            Expression::UnaryPlus(e) => write!(f, "+{}", pretty_printable(&e)),
            Expression::UnaryMinus(e) => write!(f, "-{}", pretty_printable(&e)),
            Expression::Not(e) => write!(f, "!{}", pretty_printable(&e)),
            Expression::Aggregate(a) => a.pretty_print(f),
            Expression::Iri(i) => i.pretty_print(f),
            Expression::Literal(l) => l.pretty_print(f),
            Expression::Var(v) => v.pretty_print(f),
            Expression::Function(fun) => fun.pretty_print(f),
            Expression::Exists(pattern) => {
                write!(f, "EXISTS ")?;
                pattern.pretty_print(f)
            }
            Expression::NotExists(pattern) => {
                write!(f, "NOT EXISTS ")?;
                pattern.pretty_print(f)
            }
            Expression::In(lhs, list) => {
                write!(f, "({} IN ", pretty_printable(&lhs))?;
                render_expr_list(f, list)?;
                write!(f, ")")
            }
            Expression::NotIn(lhs, list) => {
                write!(f, "({} NOT IN ", pretty_printable(&lhs))?;
                render_expr_list(f, list)?;
                write!(f, ")")
            }
        }
    }
}

fn render_expr_list(
    f: &mut Formatter<'_>,
    list: &[Spanned<Expression<'_>>],
) -> fmt::Result {
    write!(f, "({})", list.iter().map(pretty_printable).format(", "))
}

impl SparqlPrettyPrintable for Function<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.name.pretty_print(f)?;
        render_function_args(f, &self.args)
    }
}

impl SparqlPrettyPrintable for FunctionName<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FunctionName::Iri(iri) => iri.pretty_print(f),
            FunctionName::BuiltIn(kw) => f.write_str(kw.value),
        }
    }
}

impl SparqlPrettyPrintable for FunctionArg<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArg::Positional(expr) => expr.pretty_print(f),
            FunctionArg::Named(name, value) => {
                write!(f, "{} = ", name.value)?;
                value.pretty_print(f)
            }
        }
    }
}

fn render_function_args(f: &mut Formatter<'_>, args: &[FunctionArg<'_>]) -> fmt::Result {
    if args.is_empty() {
        return write!(f, "()");
    }
    write!(f, "({})", args.iter().map(pretty_printable).format(", "))
}

impl SparqlPrettyPrintable for Aggregate<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.name.pretty_print(f)?;
        write!(f, "(")?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        if self.star {
            write!(f, "*")?;
        } else {
            for (i, arg) in self.args.iter().enumerate() {
                if i > 0 {
                    match arg {
                        FunctionArg::Named(..) => write!(f, "; ")?,
                        FunctionArg::Positional(..) => write!(f, ", ")?,
                    }
                }
                arg.pretty_print(f)?;
            }
        }
        write!(f, ")")
    }
}

impl SparqlPrettyPrintable for QuadPatterns<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        for (graph, triples) in self.iter() {
            match graph {
                Some(name) => {
                    write!(f, " GRAPH ")?;
                    name.pretty_print(f)?;
                    write!(f, " {{")?;
                    render_triples(f, &triples.patterns)?;
                    write!(f, " }}")?;
                }
                None => render_triples(f, &triples.patterns)?,
            }
        }
        write!(f, " }}")
    }
}

/// Renders a list of `(subject, property list)` triples as a
/// `TriplesSameSubject ('.' TriplesSameSubject)*` block. Objects of the same
/// predicate are comma-separated and verb/object groups of the same subject are
/// semicolon-separated, so the output re-parses cleanly.
fn render_triples(
    f: &mut Formatter<'_>,
    triples: &[(GraphNode<'_>, PropertyList<'_>)],
) -> fmt::Result {
    for (i, (subject, properties)) in triples.iter().enumerate() {
        if i > 0 {
            write!(f, " . ")?;
        }
        write!(f, " ")?;
        subject.pretty_print(f)?;
        for (j, (verb, objects)) in properties.iter().enumerate() {
            if j > 0 {
                write!(f, " ;")?;
            }
            write!(f, " {}", pretty_printable(verb))?;
            write!(f, " {}", objects.iter().map(pretty_printable).format(", "))?;
        }
    }
    Ok(())
}

impl SparqlPrettyPrintable for Update<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (i, (prologue, operation)) in self.operations.iter().enumerate() {
            if i > 0 {
                write!(f, " ; ")?;
            }
            for decl in prologue {
                decl.pretty_print(f)?;
                write!(f, " ")?;
            }
            operation.pretty_print(f)?;
        }
        for decl in &self.trailing_prologue {
            if !self.operations.is_empty() {
                write!(f, " ; ")?;
            }
            decl.pretty_print(f)?;
        }
        Ok(())
    }
}

impl SparqlPrettyPrintable for PrologueDecl<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PrologueDecl::Base(iri) => {
                write!(f, "BASE ")?;
                iri.pretty_print(f)
            }
            PrologueDecl::Prefix(name, iri) => {
                write!(f, "PREFIX {name}: ")?;
                iri.pretty_print(f)
            }
        }
    }
}

impl SparqlPrettyPrintable for Update1<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Update1::Load { silent, from, to } => {
                write!(f, "LOAD ")?;
                if *silent {
                    write!(f, "SILENT ")?;
                }
                from.pretty_print(f)?;
                if let Some(graph) = to {
                    write!(f, " INTO GRAPH ")?;
                    graph.pretty_print(f)?;
                }
                Ok(())
            }
            Update1::Clear { silent, graph } => {
                write!(f, "CLEAR ")?;
                if *silent {
                    write!(f, "SILENT ")?;
                }
                graph.pretty_print(f)
            }
            Update1::Drop { silent, graph } => {
                write!(f, "DROP ")?;
                if *silent {
                    write!(f, "SILENT ")?;
                }
                graph.pretty_print(f)
            }
            Update1::Create { silent, graph } => {
                write!(f, "CREATE ")?;
                if *silent {
                    write!(f, "SILENT ")?;
                }
                write!(f, "GRAPH ")?;
                graph.pretty_print(f)
            }
            Update1::Add { silent, from, to } => {
                render_graph_target(f, "ADD", *silent, from, to)
            }
            Update1::Move { silent, from, to } => {
                render_graph_target(f, "MOVE", *silent, from, to)
            }
            Update1::Copy { silent, from, to } => {
                render_graph_target(f, "COPY", *silent, from, to)
            }
            Update1::DeleteWhere { pattern } => {
                write!(f, "DELETE WHERE ")?;
                pattern.pretty_print(f)
            }
            Update1::Modify {
                with,
                delete,
                insert,
                using,
                r#where,
            } => {
                if let Some(iri) = with {
                    write!(f, "WITH ")?;
                    iri.pretty_print(f)?;
                    write!(f, " ")?;
                }
                if !delete.is_empty() {
                    write!(f, "DELETE ")?;
                    delete.pretty_print(f)?;
                    write!(f, " ")?;
                }
                if !insert.is_empty() {
                    write!(f, "INSERT ")?;
                    insert.pretty_print(f)?;
                    write!(f, " ")?;
                }
                for clause in using {
                    write!(f, "USING ")?;
                    clause.pretty_print(f)?;
                    write!(f, " ")?;
                }
                write!(f, "WHERE ")?;
                r#where.pretty_print(f)
            }
            Update1::InsertData { quads } => {
                write!(f, "INSERT DATA ")?;
                quads.pretty_print(f)
            }
            Update1::DeleteData { quads } => {
                write!(f, "DELETE DATA ")?;
                quads.pretty_print(f)
            }
        }
    }
}

/// Renders an `ADD`/`MOVE`/`COPY` operation, which all share the shape
/// `'<KEY>' 'SILENT'? GraphOrDefault 'TO' GraphOrDefault`.
fn render_graph_target(
    f: &mut Formatter<'_>,
    keyword: &str,
    silent: bool,
    from: &GraphOrDefault<'_>,
    to: &GraphOrDefault<'_>,
) -> fmt::Result {
    write!(f, "{keyword} ")?;
    if silent {
        write!(f, "SILENT ")?;
    }
    from.pretty_print(f)?;
    write!(f, " TO ")?;
    to.pretty_print(f)
}

impl SparqlPrettyPrintable for GraphRefAll<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GraphRefAll::Graph(iri) => {
                write!(f, "GRAPH ")?;
                iri.pretty_print(f)
            }
            GraphRefAll::Default => write!(f, "DEFAULT"),
            GraphRefAll::Named => write!(f, "NAMED"),
            GraphRefAll::All => write!(f, "ALL"),
        }
    }
}

impl SparqlPrettyPrintable for GraphOrDefault<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GraphOrDefault::Graph(iri) => iri.pretty_print(f),
            GraphOrDefault::Default => write!(f, "DEFAULT"),
        }
    }
}

impl SparqlPrettyPrintable for GraphClause<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GraphClause::Default(iri) => iri.pretty_print(f),
            GraphClause::Named(iri) => {
                write!(f, "NAMED ")?;
                iri.pretty_print(f)
            }
        }
    }
}

impl SparqlPrettyPrintable for Query<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for decl in &self.prologue {
            decl.pretty_print(f)?;
            write!(f, " ")?;
        }
        self.variant.pretty_print(f)?;
        if let Some(values) = &self.values_clause {
            write!(f, " ")?;
            values.pretty_print(f)?;
        }
        Ok(())
    }
}

impl SparqlPrettyPrintable for QueryQuery<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            QueryQuery::Select(q) => q.pretty_print(f),
            QueryQuery::Construct(q) => q.pretty_print(f),
            QueryQuery::Describe(q) => q.pretty_print(f),
            QueryQuery::Ask(q) => q.pretty_print(f),
        }
    }
}

impl SparqlPrettyPrintable for SelectQuery<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.select_clause.pretty_print(f)?;
        for clause in &self.dataset_clause {
            write!(f, " FROM ")?;
            clause.pretty_print(f)?;
        }
        write!(f, " WHERE ")?;
        self.where_clause.pretty_print(f)?;
        render_solution_modifier(f, &self.solution_modifier)
    }
}

impl SparqlPrettyPrintable for ConstructQuery<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CONSTRUCT ")?;
        if let Some(where_clause) = &self.where_clause {
            write!(f, "{{")?;
            render_triples(f, &self.template.value)?;
            write!(f, " }}")?;
            for clause in &self.dataset_clause {
                write!(f, " FROM ")?;
                clause.pretty_print(f)?;
            }
            write!(f, " WHERE ")?;
            where_clause.pretty_print(f)?;
        } else {
            for clause in &self.dataset_clause {
                write!(f, " FROM ")?;
                clause.pretty_print(f)?;
            }
            write!(f, "WHERE {{")?;
            render_triples(f, &self.template.value)?;
            write!(f, " }}")?;
        }
        render_solution_modifier(f, &self.solution_modifier)
    }
}

impl SparqlPrettyPrintable for DescribeQuery<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DESCRIBE ")?;
        match &self.targets.value {
            DescribeTargets::Star => write!(f, "*")?,
            DescribeTargets::Explicit(targets) => {
                for (i, target) in targets.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    target.pretty_print(f)?;
                }
            }
        }
        for clause in &self.dataset_clause {
            write!(f, " FROM ")?;
            clause.pretty_print(f)?;
        }
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE ")?;
            where_clause.pretty_print(f)?;
        }
        render_solution_modifier(f, &self.solution_modifier)
    }
}

impl SparqlPrettyPrintable for AskQuery<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ASK ")?;
        for clause in &self.dataset_clause {
            write!(f, " FROM ")?;
            clause.pretty_print(f)?;
        }
        write!(f, " WHERE ")?;
        self.where_clause.pretty_print(f)?;
        render_solution_modifier(f, &self.solution_modifier)
    }
}

impl SparqlPrettyPrintable for SelectClause<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT ")?;
        match self.option {
            SelectionOption::Distinct => write!(f, "DISTINCT ")?,
            SelectionOption::Reduced => write!(f, "REDUCED ")?,
            SelectionOption::Default => {}
        }
        self.bindings.pretty_print(f)
    }
}

impl SparqlPrettyPrintable for SelectVariables<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SelectVariables::Star => write!(f, "*"),
            SelectVariables::Explicit(vars) => {
                for (i, item) in vars.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    let select_var = &item.value;
                    match &select_var.expression {
                        Some(e) => write!(
                            f,
                            "({} AS {})",
                            pretty_printable(&e),
                            pretty_printable(&select_var.variable)
                        )?,
                        None => select_var.variable.pretty_print(f)?,
                    }
                }
                Ok(())
            }
        }
    }
}

impl SparqlPrettyPrintable for SolutionModifier<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render_solution_modifier(f, self)
    }
}

impl SparqlPrettyPrintable for OrderCondition<'_> {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            OrderCondition::Asc(e) => write!(f, "ASC({})", pretty_printable(&e)),
            OrderCondition::Desc(e) => write!(f, "DESC({})", pretty_printable(&e)),
            OrderCondition::Plain(e) => e.pretty_print(f),
        }
    }
}

impl SparqlPrettyPrintable for LimitOffsetClauses {
    fn pretty_print(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut written = false;
        if let Some(limit) = self.limit {
            write!(f, "LIMIT {}", limit.value)?;
            written = true;
        }
        if let Some(offset) = self.offset {
            if written {
                write!(f, " ")?;
            }
            write!(f, "OFFSET {}", offset.value)?;
        }
        Ok(())
    }
}

/// Renders a `SolutionModifier` prefixed by a single space when it has content.
fn render_solution_modifier(
    f: &mut Formatter<'_>,
    modifier: &SolutionModifier<'_>,
) -> fmt::Result {
    let mut written = false;

    if !modifier.group_clause.is_empty() {
        write!(f, " GROUP BY")?;
        for (expr, var) in &modifier.group_clause {
            match var {
                Some(v) => write!(
                    f,
                    " ({} AS {})",
                    pretty_printable(&expr),
                    pretty_printable(&v)
                )?,
                None => write!(f, " {}", pretty_printable(&expr))?,
            }
        }
        written = true;
    }

    if !modifier.having_clause.is_empty() {
        if written {
            write!(f, " ")?;
        }
        write!(f, " HAVING")?;
        for expr in &modifier.having_clause {
            write!(f, " {}", pretty_printable(&expr))?;
        }
        written = true;
    }

    if !modifier.order_clause.is_empty() {
        if written {
            write!(f, " ")?;
        }
        write!(f, " ORDER BY")?;
        for condition in &modifier.order_clause {
            write!(f, " ")?;
            condition.pretty_print(f)?;
        }
        written = true;
    }

    if let Some(ref limit_offset) = modifier.limit_offset_clauses {
        if written {
            write!(f, " ")?;
        }
        write!(f, " ")?;
        limit_offset.pretty_print(f)?;
        written = true;
    }

    if written {
        write!(f, " ")?;
    }
    Ok(())
}
