//! The Abstract Syntax Tree (AST) of SPARQL queries and updates.
//!
//! This is based on the AST of [spargebra](https://crates.io/crates/spargebra).
//!
//! The node types below map closely to the grammar productions of the
//! W3C specs: [SPARQL 1.1 Query](https://www.w3.org/TR/sparql11-query/) and
//! [SPARQL 1.1 Update](https://www.w3.org/TR/sparql11-update/). Most structs and
//! enums carry the plain-CASES rendering of the syntax they represent.

pub mod pretty_print;

pub use pretty_print::{SparqlPrettyPrintable, SparqlPrettyPrinter, pretty_printable};
use std::borrow::Cow;

use crate::span::{Span, Spanned};

/// A full SPARQL query: prologue, the query form, and a trailing `VALUES` clause.
///
/// ```sparql
/// PREFIX ex: <http://example.org/>
/// SELECT ?name WHERE { ?s ex:name ?name }
/// ```
#[derive(Clone)]
pub struct Query<'a> {
    pub prologue: Vec<PrologueDecl<'a>>,
    pub variant: QueryQuery<'a>,
    pub values_clause: Option<ValuesClause<'a>>,
}

/// A `PREFIX` or `BASE` declaration from the
/// [prologue](https://www.w3.org/TR/sparql11-query/#rPrologue).
#[derive(Clone)]
pub enum PrologueDecl<'a> {
    /// `BASE <http://example.org/>`
    Base(IriRef<'a>),
    /// `PREFIX ex: <http://example.org/>`
    Prefix(&'a str, IriRef<'a>),
}

/// The four [query forms](https://www.w3.org/TR/sparql11-query/#rQuery) of SPARQL.
#[derive(Clone)]
pub enum QueryQuery<'a> {
    Select(SelectQuery<'a>),
    Construct(ConstructQuery<'a>),
    Describe(DescribeQuery<'a>),
    Ask(AskQuery<'a>),
}

/// A [`SELECT` query](https://www.w3.org/TR/sparql11-query/#select).
///
/// ```sparql
/// SELECT DISTINCT ?name
/// WHERE { ?s <http://example.org/name> ?name }
/// ```
#[derive(Clone)]
pub struct SelectQuery<'a> {
    pub select_clause: SelectClause<'a>,
    pub dataset_clause: Vec<GraphClause<'a>>,
    pub where_clause: GraphPattern<'a>,
    pub solution_modifier: SolutionModifier<'a>,
}

/// A nested [`SELECT`](https://www.w3.org/TR/sparql11-query/#rSubSelect) in a
/// graph pattern — `{ SELECT ... WHERE ... }` — which scopes its variables.
#[derive(Clone)]
pub struct SubSelect<'a> {
    pub select_clause: SelectClause<'a>,
    pub where_clause: GraphPattern<'a>,
    pub solution_modifier: SolutionModifier<'a>,
    pub values_clause: Option<ValuesClause<'a>>,
}

/// The `SELECT` keyword and its list of projected bindings.
#[derive(Clone)]
pub struct SelectClause<'a> {
    pub option: SelectionOption,
    pub bindings: Spanned<SelectVariables<'a>>,
}

/// A single binding in a `SELECT` clause: an optional expression (for a
/// `(expr AS var)` projection) paired with its target variable.
#[derive(Clone)]
pub struct SelectVariable<'a> {
    pub expression: Option<Spanned<Expression<'a>>>,
    pub variable: Var<'a>,
}

#[derive(Clone)]
pub enum SelectVariables<'a> {
    /// `SELECT *`
    Star,
    Explicit(Vec<Spanned<SelectVariable<'a>>>),
}

/// Whether / how duplicate rows are removed
/// ([`SELECT` modifiers](https://www.w3.org/TR/sparql11-query/#modDuplicates)).
#[derive(Clone)]
pub enum SelectionOption {
    Default,
    Distinct,
    Reduced,
}

/// A [`CONSTRUCT` query](https://www.w3.org/TR/sparql11-query/#construct).
///
/// ```sparql
/// CONSTRUCT { ?s <http://example.org/p> ?o }
/// WHERE { ?s <http://example.org/q> ?o }
/// ```
#[derive(Clone)]
pub struct ConstructQuery<'a> {
    pub template: Spanned<Vec<(GraphNode<'a>, PropertyList<'a>)>>,
    pub dataset_clause: Vec<GraphClause<'a>>,
    pub where_clause: Option<GraphPattern<'a>>,
    pub solution_modifier: SolutionModifier<'a>,
}

/// A [`DESCRIBE` query](https://www.w3.org/TR/sparql11-query/#describe)
/// returning a description of the given resources (`*` or a list of vars/IRIs).
#[derive(Clone)]
pub struct DescribeQuery<'a> {
    pub targets: Spanned<DescribeTargets<'a>>,
    pub dataset_clause: Vec<GraphClause<'a>>,
    pub where_clause: Option<GraphPattern<'a>>,
    pub solution_modifier: SolutionModifier<'a>,
}

#[derive(Clone)]
pub enum DescribeTargets<'a> {
    /// `DESCRIBE *`
    Star,
    Explicit(Vec<VarOrIri<'a>>),
}

/// An [`ASK` query](https://www.w3.org/TR/sparql11-query/#ask).
#[derive(Clone)]
pub struct AskQuery<'a> {
    pub dataset_clause: Vec<GraphClause<'a>>,
    pub where_clause: GraphPattern<'a>,
    pub solution_modifier: SolutionModifier<'a>,
}

/// A `FROM` / `FROM NAMED` graph clause selecting the
/// [dataset](https://www.w3.org/TR/sparql11-query/#rDatasetClause) to query.
#[derive(Clone)]
pub enum GraphClause<'a> {
    /// `FROM <iri>`
    Default(Iri<'a>),
    /// `FROM NAMED <iri>`
    Named(Iri<'a>),
}

/// The trailing `GROUP BY` / `HAVING` / `ORDER BY` / `LIMIT` / `OFFSET`
/// [solution modifier](https://www.w3.org/TR/sparql11-query/#rSolutionModifier) clauses.
#[derive(Clone)]
pub struct SolutionModifier<'a> {
    pub group_clause: Vec<(Spanned<Expression<'a>>, Option<Var<'a>>)>,
    pub having_clause: Vec<Spanned<Expression<'a>>>,
    pub order_clause: Vec<OrderCondition<'a>>,
    pub limit_offset_clauses: Option<LimitOffsetClauses>,
}

/// An `ORDER BY` key with its `ASC` / `DESC` direction.
#[derive(Clone)]
pub enum OrderCondition<'a> {
    Asc(Spanned<Expression<'a>>),
    Desc(Spanned<Expression<'a>>),
    Plain(Spanned<Expression<'a>>),
}

/// Numeric `LIMIT` / `OFFSET`, parsed to `u64`
/// ([`OFFSET`](https://www.w3.org/TR/sparql11-query/#rOffsetClause),
/// [`LIMIT`](https://www.w3.org/TR/sparql11-query/#rLimitClause)). The span
/// points at the integer token.
#[derive(Clone)]
pub struct LimitOffsetClauses {
    pub offset: Option<Spanned<u64>>,
    pub limit: Option<Spanned<u64>>,
}

/// A `VALUES` clause with a [single variable](https://www.w3.org/TR/sparql11-query/#rInlineDataOneVar)
/// or [multiple variables](https://www.w3.org/TR/sparql11-query/#rInlineDataFull) providing inline
/// data bindings.
///
/// ```sparql
/// VALUES ?name { "Alice" "Bob" }
/// ```
#[derive(Clone)]
pub struct ValuesClause<'a> {
    pub variables: Vec<Var<'a>>,
    pub values: Spanned<Vec<Vec<DataBlockValue<'a>>>>,
}

/// One entry inside a `VALUES` row.
#[derive(Clone)]
pub enum DataBlockValue<'a> {
    Iri(Iri<'a>),
    Literal(Literal<'a>),
    /// The `UNDEF` marker (unbound) in a row.
    Undef,
}

/// A group graph pattern — a list of elements — or a nested `SELECT`
/// ([graph pattern](https://www.w3.org/TR/sparql11-query/#rGroupGraphPattern)).
#[derive(Clone)]
pub enum GraphPattern<'a> {
    /// `{ element1 . element2 . ... }`
    Group(Vec<Spanned<GraphPatternElement<'a>>>),
    SubSelect(Box<SubSelect<'a>>),
}

/// A single element inside a group graph pattern.
#[derive(Clone)]
pub enum GraphPatternElement<'a> {
    /// `FILTER (expr)`
    Filter(Spanned<Expression<'a>>),
    /// `{ p1 } UNION { p2 }`
    Union(Vec<GraphPattern<'a>>),
    /// `MINUS { pattern }`
    Minus(Box<GraphPattern<'a>>),
    /// `VALUES ...` (inline data)
    Values(ValuesClause<'a>),
    /// `BIND(expr AS ?var)`
    Bind(Spanned<Expression<'a>>, Var<'a>),
    /// `SERVICE [SILENT] <iri> { ... }`
    Service {
        silent: bool,
        name: VarOrIri<'a>,
        pattern: Box<GraphPattern<'a>>,
    },
    /// `GRAPH <iri>|?var { ... }`
    Graph {
        name: VarOrIri<'a>,
        pattern: Box<GraphPattern<'a>>,
    },
    /// `OPTIONAL { pattern }`
    Optional(Box<GraphPattern<'a>>),
    /// Bare triples (possibly with path verbs), e.g. `?s ?p ?o`.
    Triples(Vec<(GraphNodePath<'a>, PropertyListPath<'a>)>),
}

/// A list of `verb → object list` entries making up the triples of one subject.
/// The path variant of a property list
/// ([`PropertyListPath`](https://www.w3.org/TR/sparql11-query/#rPropertyListPath)).
///
/// ```sparql
/// ?s ex:name ?name ; ex:age ?age      -- two entries
/// ```
#[derive(Clone, Default)]
pub struct PropertyListPath<'a> {
    pub properties: Vec<(VarOrPath<'a>, Vec<ObjectPath<'a>>)>,
}

impl<'a> PropertyListPath<'a> {
    pub fn iter(&self) -> std::slice::Iter<'_, (VarOrPath<'a>, Vec<ObjectPath<'a>>)> {
        self.properties.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.properties.is_empty()
    }

    pub fn len(&self) -> usize {
        self.properties.len()
    }

    pub fn push(&mut self, verb: VarOrPath<'a>, objects: Vec<ObjectPath<'a>>) {
        self.properties.push((verb, objects));
    }
}

/// A non-path property list: `verb → object list` entries for one subject
/// ([`PropertyList`](https://www.w3.org/TR/sparql11-query/#rPropertyList)).
#[derive(Clone, Debug, Default)]
pub struct PropertyList<'a> {
    pub properties: Vec<(Verb<'a>, Vec<Object<'a>>)>,
}

impl<'a> PropertyList<'a> {
    pub fn iter(&self) -> std::slice::Iter<'_, (Verb<'a>, Vec<Object<'a>>)> {
        self.properties.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.properties.is_empty()
    }

    pub fn len(&self) -> usize {
        self.properties.len()
    }

    pub fn push(&mut self, verb: Verb<'a>, objects: Vec<Object<'a>>) {
        self.properties.push((verb, objects));
    }
}

/// A triple predicate: a variable, an IRI, or the `a` keyword.
#[derive(Clone, Debug)]
pub enum Verb<'a> {
    Var(Var<'a>),
    Iri(Iri<'a>),
    /// `a` (`rdf:type`)
    A,
}

/// A predicate that may be a property path rather than a single verb.
#[derive(Clone)]
pub enum VarOrPath<'a> {
    Var(Var<'a>),
    Path(Path<'a>),
}

/// An object in a path triple, wrapping its graph node.
#[derive(Clone)]
pub struct ObjectPath<'a> {
    pub graph_node: GraphNodePath<'a>,
}

/// An object in a plain triple, wrapping its graph node.
#[derive(Clone, Debug)]
pub struct Object<'a> {
    pub graph_node: GraphNode<'a>,
}

/// A [property path](https://www.w3.org/TR/sparql11-query/#rPropertyPath):
/// a navigation expression over predicates.
///
/// ```text
/// ?a ex:knows/ex:name ?n      -- Sequence
/// ?s !ex:p1|ex:p2 ?o          -- NegatedPropertySet
/// ex:q+ ?x                    -- OneOrMore
/// ```
#[derive(Clone)]
pub enum Path<'a> {
    Alternative(Box<Self>, Box<Self>),
    Sequence(Box<Self>, Box<Self>),
    Inverse(Box<Self>),
    ZeroOrOne(Box<Self>),
    ZeroOrMore(Box<Self>),
    OneOrMore(Box<Self>),
    Iri(Iri<'a>),
    A,
    NegatedPropertySet(Vec<PathOneInPropertySet<'a>>),
}

/// A single predicate inside a negated property set (`!p` / `^p` / `a`).
#[derive(Clone)]
pub enum PathOneInPropertySet<'a> {
    Iri(Iri<'a>),
    A,
    InverseIri(Iri<'a>),
    InverseA,
}

/// An RDF term used as a graph node in a path triple.
#[derive(Clone)]
pub enum GraphNodePath<'a> {
    VarOrTerm(VarOrTerm<'a>),
    /// RDF list `( item1 item2 )`
    Collection(Spanned<Vec<GraphNodePath<'a>>>),
    /// `[ :p1 1 ; :p2 2 ]`
    BlankNodePropertyList(Spanned<PropertyListPath<'a>>),
}

/// An RDF term used as a graph node in a plain triple.
#[derive(Clone, Debug)]
pub enum GraphNode<'a> {
    VarOrTerm(VarOrTerm<'a>),
    /// RDF list `( item1 item2 )`
    Collection(Spanned<Vec<GraphNode<'a>>>),
    /// `[ :p1 1 ; :p2 2 ]`
    BlankNodePropertyList(Spanned<PropertyList<'a>>),
}

impl<'a> GraphNode<'a> {
    pub fn span(&self) -> Option<Span> {
        match self {
            GraphNode::VarOrTerm(vot) => vot.span(),
            GraphNode::Collection(c) => Some(c.span),
            GraphNode::BlankNodePropertyList(b) => Some(b.span),
        }
    }
}

/// A variable or an IRI, as used for graph names and `DESCRIBE` targets.
#[derive(Clone)]
pub enum VarOrIri<'a> {
    Var(Var<'a>),
    Iri(Iri<'a>),
}

impl<'a> VarOrIri<'a> {
    pub fn span(&self) -> Span {
        match self {
            VarOrIri::Var(v) => v.span,
            VarOrIri::Iri(i) => i.span(),
        }
    }
}

/// A variable, an IRI, a literal, a blank node, or `()` — the RDF terms usable
/// as a subject/object graph node.
#[derive(Clone, Debug)]
pub enum VarOrTerm<'a> {
    Var(Var<'a>),
    Iri(Iri<'a>),
    Literal(Literal<'a>),
    BlankNode(Spanned<BlankNode<'a>>),
    /// The empty list `()`
    Nil,
}

impl<'a> VarOrTerm<'a> {
    pub fn span(&self) -> Option<Span> {
        match self {
            VarOrTerm::Var(v) => Some(v.span),
            VarOrTerm::Iri(i) => Some(i.span()),
            VarOrTerm::Literal(l) => Some(l.span()),
            VarOrTerm::BlankNode(b) => Some(b.span),
            VarOrTerm::Nil => None,
        }
    }
}

/// A [SPARQL expression](https://www.w3.org/TR/sparql11-query/#rExpression):
/// operators, function/aggregate calls, RDF terms, and `EXISTS`.
#[derive(Clone)]
pub enum Expression<'a> {
    Or(Box<Spanned<Self>>, Box<Spanned<Self>>),
    And(Box<Spanned<Self>>, Box<Spanned<Self>>),
    Equal(Box<Spanned<Self>>, Box<Spanned<Self>>),
    NotEqual(Box<Spanned<Self>>, Box<Spanned<Self>>),
    Less(Box<Spanned<Self>>, Box<Spanned<Self>>),
    LessOrEqual(Box<Spanned<Self>>, Box<Spanned<Self>>),
    Greater(Box<Spanned<Self>>, Box<Spanned<Self>>),
    GreaterOrEqual(Box<Spanned<Self>>, Box<Spanned<Self>>),
    In(Box<Spanned<Self>>, Vec<Spanned<Self>>),
    NotIn(Box<Spanned<Self>>, Vec<Spanned<Self>>),
    Add(Box<Spanned<Self>>, Box<Spanned<Self>>),
    Subtract(Box<Spanned<Self>>, Box<Spanned<Self>>),
    Multiply(Box<Spanned<Self>>, Box<Spanned<Self>>),
    Divide(Box<Spanned<Self>>, Box<Spanned<Self>>),
    UnaryPlus(Box<Spanned<Self>>),
    UnaryMinus(Box<Spanned<Self>>),
    Not(Box<Spanned<Self>>),
    Aggregate(Aggregate<'a>),
    Iri(Iri<'a>),
    Literal(Literal<'a>),
    Var(Var<'a>),
    Function(Function<'a>),
    Exists(Box<GraphPattern<'a>>),
    NotExists(Box<GraphPattern<'a>>),
}

/// A function call, be it a built-in like `STR(...)` or a user-defined function
/// addressed by IRI (`ex:f(...)`).
///
/// Argument count/arity and argument names are **not** validated here; that is
/// deferred to LogicalPlan construction. Any function may carry positional
/// and/or named arguments (`NAME = value`, like `GROUP_CONCAT`'s `SEPARATOR`).
#[derive(Clone)]
pub struct Function<'a> {
    pub name: FunctionName<'a>,
    pub args: Vec<FunctionArg<'a>>,
}

/// The name of a function call.
#[derive(Clone)]
pub enum FunctionName<'a> {
    /// A user-defined/property function addressed by IRI.
    Iri(Iri<'a>),
    /// A SPARQL built-in function keyword such as `STR` or `CONTAINS`.
    BuiltIn(Spanned<&'a str>),
}

impl<'a> FunctionName<'a> {
    pub fn span(&self) -> Span {
        match self {
            FunctionName::Iri(iri) => iri.span(),
            FunctionName::BuiltIn(s) => s.span,
        }
    }
}

/// A single argument of a function call or aggregate.
#[derive(Clone)]
pub enum FunctionArg<'a> {
    /// A positional argument (an expression).
    Positional(Spanned<Expression<'a>>),
    /// A named argument, e.g. `SEPARATOR = ";"`.
    Named(Spanned<&'a str>, Spanned<Expression<'a>>),
}

/// An aggregate call (`COUNT`, `SUM`, ...).
///
/// Only aggregates may carry the `DISTINCT` modifier. `star` records a bare
/// `*` argument (`COUNT(*)`). Argument names are not validated here.
#[derive(Clone)]
pub struct Aggregate<'a> {
    /// The aggregate keyword or IRI.
    pub name: FunctionName<'a>,
    /// Whether `DISTINCT` was given.
    pub distinct: bool,
    /// Whether the argument is a bare `*` (only meaningful for `COUNT`).
    pub star: bool,
    pub args: Vec<FunctionArg<'a>>,
}

/// A variable in an expression or projection, carrying its own source span.
#[derive(Clone, Debug)]
pub struct Var<'a> {
    pub value: &'a str,
    pub span: Span,
}

impl<'a> Var<'a> {
    pub fn span(&self) -> Span {
        self.span
    }
}

/// An [RDF literal](https://www.w3.org/TR/sparql11-query/#rRDFLiteral).
///
/// Booleans are parsed into their typed value. Numeric literals keep both their
/// parsed typed value (used by expressions) and their original source text (used
/// as the RDF term's lexical form, so that e.g. `123.0` matches `"123.0"^^xsd:decimal`).
#[derive(Clone, Debug)]
pub enum Literal<'a> {
    Boolean(Spanned<bool>),
    Integer(Spanned<rdf_fusion_common::Integer>, Cow<'a, str>),
    Decimal(Spanned<rdf_fusion_common::Decimal>, Cow<'a, str>),
    Double(Spanned<rdf_fusion_common::Double>, Cow<'a, str>),
    /// A plain string `"..."`
    String(String<'a>),
    /// A language-tagged string `"..."@en`
    LangString(String<'a>, Spanned<&'a str>),
    /// A data-typed string `"..."^^xsd:dateTime`
    Typed(String<'a>, Iri<'a>),
}

impl<'a> Literal<'a> {
    pub fn span(&self) -> Span {
        match self {
            Literal::Boolean(s) => s.span,
            Literal::Integer(s, _) => s.span,
            Literal::Decimal(s, _) => s.span,
            Literal::Double(s, _) => s.span,
            Literal::String(s) => s.span,
            Literal::LangString(s, lang) => s.span.union(lang.span),
            Literal::Typed(s, iri) => s.span.union(iri.span()),
        }
    }
}

/// A string literal value, carrying its own source span.
#[derive(Clone, Debug)]
pub struct String<'a> {
    pub value: Cow<'a, str>,
    pub span: Span,
}

/// An IRI dereferenced against the prologue: either an absolute `<...>` ref or
/// a `prefix:local` prefixed name.
#[derive(Clone, Debug)]
pub enum Iri<'a> {
    /// `<http://example.org/>`
    IriRef(IriRef<'a>),
    /// `ex:thing`
    PrefixedName(PrefixedName<'a>),
}

impl<'a> Iri<'a> {
    pub fn span(&self) -> Span {
        match self {
            Iri::IriRef(s) => s.span,
            Iri::PrefixedName(s) => s.span,
        }
    }
}

/// A prefixed name (`namespace:local`), carrying its own source span.
#[derive(Clone, Debug)]
pub struct PrefixedName<'a> {
    pub namespace: &'a str,
    pub local: Cow<'a, str>,
    pub span: Span,
}

/// An `IRIREF` (`<...>`), carrying its own source span.
#[derive(Clone, Debug)]
pub struct IriRef<'a> {
    pub value: Cow<'a, str>,
    pub span: Span,
}

/// A blank node label (`_:b1`) or, with `None`, an anonymous blank node (`[]`).
#[derive(Clone, Debug)]
pub struct BlankNode<'a>(pub Option<&'a str>);

/// A full SPARQL Update: a sequence of operations, each with its own prologue.
///
/// ```sparql
/// INSERT DATA { <http://example.org/s> <http://example.org/p> 1 } ;
/// DELETE DATA { <http://example.org/s> <http://example.org/p> 2 }
/// ```
#[derive(Clone)]
pub struct Update<'a> {
    pub operations: Vec<(Vec<PrologueDecl<'a>>, Update1<'a>)>,
    pub trailing_prologue: Vec<PrologueDecl<'a>>,
}

/// A single [update operation](https://www.w3.org/TR/sparql11-update/#rUpdate1).
#[derive(Clone)]
pub enum Update1<'a> {
    /// `LOAD <iri> [INTO GRAPH <iri>]`
    Load {
        silent: bool,
        from: Iri<'a>,
        to: Option<Iri<'a>>,
    },
    /// `CLEAR [SILENT] graph`
    Clear {
        silent: bool,
        graph: GraphRefAll<'a>,
    },
    /// `DROP [SILENT] graph`
    Drop {
        silent: bool,
        graph: GraphRefAll<'a>,
    },
    /// `CREATE [SILENT] GRAPH <iri>`
    Create { silent: bool, graph: Iri<'a> },
    /// `ADD [SILENT] src TO dst`
    Add {
        silent: bool,
        from: GraphOrDefault<'a>,
        to: GraphOrDefault<'a>,
    },
    /// `MOVE [SILENT] src TO dst`
    Move {
        silent: bool,
        from: GraphOrDefault<'a>,
        to: GraphOrDefault<'a>,
    },
    /// `COPY [SILENT] src TO dst`
    Copy {
        silent: bool,
        from: GraphOrDefault<'a>,
        to: GraphOrDefault<'a>,
    },
    /// `DELETE WHERE { quads }`
    DeleteWhere { pattern: QuadPatterns<'a> },
    /// `WITH graph DELETE { ... } INSERT { ... } USING ... WHERE { ... }`
    Modify {
        with: Option<Iri<'a>>,
        delete: QuadPatterns<'a>,
        insert: QuadPatterns<'a>,
        using: Vec<GraphClause<'a>>,
        r#where: GraphPattern<'a>,
    },
    /// `INSERT DATA { quads }`
    InsertData { quads: QuadPatterns<'a> },
    /// `DELETE DATA { quads }`
    DeleteData { quads: QuadPatterns<'a> },
}

/// A quad pattern: a list of `(optional graph, triples)` blocks used by the
/// update forms, where `None` means the default graph.
#[derive(Clone, Default)]
pub struct QuadPatterns<'a> {
    pub patterns: Vec<(Option<VarOrIri<'a>>, TriplePatterns<'a>)>,
}

/// A basic graph pattern: a list of `(subject, property list)` triples.
///
/// ```sparql
/// ?s <http://example.org/name> ?name ; <http://example.org/age> ?age
/// ```
#[derive(Clone, Default)]
pub struct TriplePatterns<'a> {
    pub patterns: Vec<(GraphNode<'a>, PropertyList<'a>)>,
}

impl<'a> TriplePatterns<'a> {
    pub fn iter(&self) -> std::slice::Iter<'_, (GraphNode<'a>, PropertyList<'a>)> {
        self.patterns.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    pub fn len(&self) -> usize {
        self.patterns.len()
    }

    pub fn push(&mut self, graph_node: GraphNode<'a>, property_list: PropertyList<'a>) {
        self.patterns.push((graph_node, property_list));
    }
}

impl<'a> From<Vec<(GraphNode<'a>, PropertyList<'a>)>> for TriplePatterns<'a> {
    fn from(patterns: Vec<(GraphNode<'a>, PropertyList<'a>)>) -> Self {
        Self { patterns }
    }
}

impl<'a> QuadPatterns<'a> {
    pub fn iter(
        &self,
    ) -> std::slice::Iter<'_, (Option<VarOrIri<'a>>, TriplePatterns<'a>)> {
        self.patterns.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    pub fn len(&self) -> usize {
        self.patterns.len()
    }

    pub fn push(
        &mut self,
        graph: Option<VarOrIri<'a>>,
        triples: impl Into<TriplePatterns<'a>>,
    ) {
        self.patterns.push((graph, triples.into()));
    }
}

/// The `GRAPH`, `DEFAULT`, `NAMED`, or `ALL` selector for `DROP` / `CLEAR`.
#[derive(Clone)]
pub enum GraphRefAll<'a> {
    Graph(Iri<'a>),
    Default,
    Named,
    All,
}

/// The source/target of `ADD` / `MOVE` / `COPY`: a graph or the default graph.
#[derive(Clone)]
pub enum GraphOrDefault<'a> {
    Graph(Iri<'a>),
    Default,
}
