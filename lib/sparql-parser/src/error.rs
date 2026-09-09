use crate::span::Span;
use codespan_reporting::diagnostic::{
    Diagnostic as CsDiagnostic, Label as CsLabel, Severity,
};
use codespan_reporting::files::SimpleFile;
use codespan_reporting::term;
use codespan_reporting::term::termcolor::Buffer;
use datafusion_common::DataFusionError;
use rdf_fusion_common::IriParseError;

/// An error returned while parsing and planning a SPARQL query or update.
#[derive(Debug, thiserror::Error)]
pub enum SparqlParseError {
    /// A syntax error returned by the underlying parser.
    #[error(transparent)]
    Syntax(#[from] SparqlSyntaxError),
    /// A validation or semantic error discovered during AST validation or rewriting.
    #[error(transparent)]
    Validation(#[from] SparqlValidationError),
    /// An error while resolving the base IRI.
    #[error(transparent)]
    BaseIri(#[from] IriParseError),
    /// An error while creating the LogicalPlan from the parsed algebra.
    #[error(transparent)]
    PlanCreation(#[from] DataFusionError),
    /// A general error while rewriting the AST without an associated span.
    #[error("{0}")]
    Rewriting(String),
}

impl SparqlParseError {
    /// Creates a validation error carrying a single error diagnostic at `span`.
    pub fn new(span: Span, message: impl Into<String>) -> Self {
        Self::new_validation(span, message)
    }

    /// Creates a validation error carrying a single error diagnostic at `span`.
    pub fn new_validation(span: Span, message: impl Into<String>) -> Self {
        let msg = message.into();
        Self::Validation(SparqlValidationError::single(
            DiagnosticsGroup::error(msg.clone()).with_error(span, msg),
        ))
    }

    pub fn new_plan_creation(error: DataFusionError) -> Self {
        Self::PlanCreation(error)
    }

    /// Creates a rewriting error without a source span.
    pub fn new_without_span(message: impl Into<String>) -> Self {
        Self::Rewriting(message.into())
    }

    /// Creates a validation error carrying an error label at `span` and an info label at `info_span`.
    pub fn new_with_info(
        span: Span,
        label_message: impl Into<String>,
        main_message: impl Into<String>,
        info_span: Span,
        info_message: impl Into<String>,
    ) -> Self {
        Self::Validation(SparqlValidationError::single(
            DiagnosticsGroup::error(main_message)
                .with_error(span, label_message)
                .with_info(info_span, info_message),
        ))
    }

    /// Creates a validation error carrying an error label at `span` and an info label at `info_span`.
    pub fn new_validation_with_info(
        span: Span,
        label_message: impl Into<String>,
        main_message: impl Into<String>,
        info_span: Span,
        info_message: impl Into<String>,
    ) -> Self {
        Self::new_with_info(span, label_message, main_message, info_span, info_message)
    }

    /// Renders the error against `source` if it contains diagnostics,
    /// or falls back to its `Display` representation.
    pub fn render(&self, source: &str) -> String {
        match self {
            Self::Syntax(err) => err.render(source),
            Self::Validation(err) => err.render(source),
            _ => self.to_string(),
        }
    }
}

/// The kind / severity of a single [`Diagnostic`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticKind {
    /// A hard error, meaning the input does not conform to the grammar.
    Error,
    /// A suggestion for how to fix the input.
    Advice,
    /// Informational context, e.g. pointing to where a symbol was previously used.
    Info,
}

impl std::fmt::Display for DiagnosticKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiagnosticKind::Error => write!(f, "error"),
            DiagnosticKind::Advice => write!(f, "suggestion"),
            DiagnosticKind::Info => write!(f, "info"),
        }
    }
}

pub type LabelKind = DiagnosticKind;

/// A single diagnostic message attached to a source [`Span`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    /// The severity of this diagnostic.
    pub kind: DiagnosticKind,
    /// The byte range in the source this diagnostic refers to.
    pub span: Span,
    /// A human readable message.
    pub message: String,
}

impl Diagnostic {
    /// Creates a new [`Diagnostic`].
    pub fn new(kind: DiagnosticKind, span: Span, message: impl Into<String>) -> Self {
        Self {
            kind,
            span,
            message: message.into(),
        }
    }

    /// An error diagnostic.
    pub fn error(span: Span, message: impl Into<String>) -> Self {
        Self::new(DiagnosticKind::Error, span, message)
    }

    /// A suggestion diagnostic.
    pub fn advice(span: Span, message: impl Into<String>) -> Self {
        Self::new(DiagnosticKind::Advice, span, message)
    }

    /// An informational diagnostic.
    pub fn info(span: Span, message: impl Into<String>) -> Self {
        Self::new(DiagnosticKind::Info, span, message)
    }

    /// Renders the diagnostic as a single-element [`DiagnosticsGroup`] against `source`.
    pub fn render(&self, source: &str) -> String {
        DiagnosticsGroup::from(self.clone()).render(source)
    }
}

impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} at {}..{}: {}",
            self.kind, self.span.start, self.span.end, self.message
        )
    }
}

pub type Label = Diagnostic;

/// A group of diagnostics representing a single error or diagnostic event.
///
/// Contains a main `message` rendered at the top of the report, and a
/// (possibly empty) list of [`Diagnostic`]s pointing to source spans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticsGroup {
    /// The severity of this group.
    pub severity: Severity,
    /// The main error message shown at the top of the report.
    pub message: String,
    /// The list of diagnostics (with source spans) to render.
    pub diagnostics: Vec<Diagnostic>,
}

impl DiagnosticsGroup {
    /// Creates a new [`DiagnosticsGroup`] with a main message and no diagnostics.
    pub fn new(severity: Severity, message: impl Into<String>) -> Self {
        Self {
            severity,
            message: message.into(),
            diagnostics: Vec::new(),
        }
    }

    /// Creates an error [`DiagnosticsGroup`] with the given main message.
    pub fn error(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, message)
    }

    /// Appends a [`Diagnostic`].
    pub fn with_diagnostic(mut self, diagnostic: Diagnostic) -> Self {
        self.diagnostics.push(diagnostic);
        self
    }

    /// Appends an error diagnostic label.
    pub fn with_error(mut self, span: Span, message: impl Into<String>) -> Self {
        self.diagnostics.push(Diagnostic::error(span, message));
        self
    }

    /// Appends an advice diagnostic label.
    pub fn with_advice(mut self, span: Span, message: impl Into<String>) -> Self {
        self.diagnostics.push(Diagnostic::advice(span, message));
        self
    }

    /// Appends an informational diagnostic label.
    pub fn with_info(mut self, span: Span, message: impl Into<String>) -> Self {
        self.diagnostics.push(Diagnostic::info(span, message));
        self
    }

    /// Appends a [`Diagnostic`].
    pub fn push(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    /// Returns `true` if this group or any of its diagnostics is an error.
    pub fn is_error(&self) -> bool {
        self.severity == Severity::Error
            || self
                .diagnostics
                .iter()
                .any(|d| d.kind == DiagnosticKind::Error)
    }

    /// Returns the first error diagnostic, if any.
    pub fn first_error(&self) -> Option<&Diagnostic> {
        self.diagnostics
            .iter()
            .find(|d| d.kind == DiagnosticKind::Error)
    }

    /// Renders this diagnostic group against `source`.
    pub fn render(&self, source: &str) -> String {
        render_diagnostics_group(self, source)
    }
}

impl From<Diagnostic> for DiagnosticsGroup {
    fn from(diagnostic: Diagnostic) -> Self {
        let message = diagnostic.message.clone();
        let severity = match diagnostic.kind {
            DiagnosticKind::Error => Severity::Error,
            DiagnosticKind::Advice => Severity::Help,
            DiagnosticKind::Info => Severity::Note,
        };
        Self {
            severity,
            message,
            diagnostics: vec![diagnostic],
        }
    }
}

impl std::fmt::Display for DiagnosticsGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.diagnostics.is_empty() {
            write!(
                f,
                "{}: {}",
                format!("{:?}", self.severity).to_lowercase(),
                self.message
            )
        } else {
            for (i, diag) in self.diagnostics.iter().enumerate() {
                if i > 0 {
                    writeln!(f)?;
                }
                write!(f, "{diag}")?;
            }
            Ok(())
        }
    }
}

fn render_diagnostics_group(group: &DiagnosticsGroup, source: &str) -> String {
    let file = SimpleFile::new("", source);
    let mut labels = Vec::new();

    for (i, diag) in group.diagnostics.iter().enumerate() {
        let span = diag.span.start.min(source.len())..diag.span.end.min(source.len());
        let cs_label = if i == 0 && diag.kind == DiagnosticKind::Error {
            CsLabel::primary((), span)
        } else {
            CsLabel::secondary((), span)
        };
        let cs_label = if diag.message.is_empty() {
            cs_label
        } else {
            cs_label.with_message(&diag.message)
        };
        labels.push(cs_label);
    }

    let cs_diag = CsDiagnostic::new(group.severity)
        .with_message(&group.message)
        .with_labels(labels);

    let mut writer = Buffer::no_color();
    let config = term::Config::default();
    term::emit_to_io_write(&mut writer, &config, &file, &cs_diag)
        .expect("rendering diagnostic cannot fail");
    String::from_utf8_lossy(writer.as_slice()).into_owned()
}

/// The error type threaded through the parser.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SparqlSyntaxError(pub Vec<DiagnosticsGroup>);

impl SparqlSyntaxError {
    /// Creates an error carrying a single [`DiagnosticsGroup`].
    pub fn single(group: impl Into<DiagnosticsGroup>) -> Self {
        Self(vec![group.into()])
    }

    /// Appends a [`DiagnosticsGroup`].
    pub fn push(&mut self, group: impl Into<DiagnosticsGroup>) {
        self.0.push(group.into());
    }

    /// Appends all diagnostics from another parse error.
    pub fn extend(&mut self, other: SparqlSyntaxError) {
        self.0.extend(other.0);
    }

    /// Returns `true` if any [`DiagnosticsGroup`] contains an error.
    pub fn has_errors(&self) -> bool {
        self.0.iter().any(DiagnosticsGroup::is_error)
    }

    /// Returns the first error diagnostic, if any.
    pub fn first_error(&self) -> Option<&Diagnostic> {
        self.0.iter().find_map(DiagnosticsGroup::first_error)
    }

    /// Renders every [`DiagnosticsGroup`] in `self` against `source`.
    pub fn render(&self, source: &str) -> String {
        let mut out = String::new();
        for group in &self.0 {
            out.push_str(&group.render(source));
        }
        out
    }
}

impl<T: Into<DiagnosticsGroup>> From<T> for SparqlSyntaxError {
    fn from(group: T) -> Self {
        Self::single(group)
    }
}

impl std::fmt::Display for SparqlSyntaxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, group) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{group}")?;
        }
        Ok(())
    }
}

impl std::error::Error for SparqlSyntaxError {}

/// An error discovered while validating the semantics or structure of a SPARQL AST.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SparqlValidationError(pub Vec<DiagnosticsGroup>);

impl SparqlValidationError {
    /// Creates an error carrying a single [`DiagnosticsGroup`].
    pub fn single(group: impl Into<DiagnosticsGroup>) -> Self {
        Self(vec![group.into()])
    }

    /// Appends a [`DiagnosticsGroup`].
    pub fn push(&mut self, group: impl Into<DiagnosticsGroup>) {
        self.0.push(group.into());
    }

    /// Appends all diagnostics from another validation error.
    pub fn extend(&mut self, other: SparqlValidationError) {
        self.0.extend(other.0);
    }

    /// Returns `true` if any [`DiagnosticsGroup`] contains an error.
    pub fn has_errors(&self) -> bool {
        self.0.iter().any(DiagnosticsGroup::is_error)
    }

    /// Returns the first error diagnostic, if any.
    pub fn first_error(&self) -> Option<&Diagnostic> {
        self.0.iter().find_map(DiagnosticsGroup::first_error)
    }

    /// Renders every [`DiagnosticsGroup`] in `self` against `source`.
    pub fn render(&self, source: &str) -> String {
        let mut out = String::new();
        for group in &self.0 {
            out.push_str(&group.render(source));
        }
        out
    }
}

impl<T: Into<DiagnosticsGroup>> From<T> for SparqlValidationError {
    fn from(group: T) -> Self {
        Self::single(group)
    }
}

impl std::fmt::Display for SparqlValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, group) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{group}")?;
        }
        Ok(())
    }
}

impl std::error::Error for SparqlValidationError {}

/// Renders every [`DiagnosticsGroup`] in `error` against `source`.
pub fn render_internal(error: &SparqlSyntaxError, source: &str) -> String {
    error.render(source)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_single_error() {
        let group = DiagnosticsGroup::error("expected a term or variable, found `.`")
            .with_error(Span::new(8, 9), "expected a term or variable, found `.`");
        insta::assert_snapshot!(
            group.render("<s> <p> ."),
            @"
        error: expected a term or variable, found `.`
          ┌─ :1:9
          │
        1 │ <s> <p> .
          │         ^ expected a term or variable, found `.`
        "
        );
    }

    #[test]
    fn render_with_suggestion() {
        let group = DiagnosticsGroup::error("expected a term or variable, found `.`")
            .with_error(Span::new(8, 9), "expected a term or variable, found `.`")
            .with_advice(Span::new(8, 9), "consider adding an object here");
        insta::assert_snapshot!(group.render("<s> <p> ."), @"
        error: expected a term or variable, found `.`
          ┌─ :1:9
          │
        1 │ <s> <p> .
          │         ^
          │         │
          │         expected a term or variable, found `.`
          │         consider adding an object here
        ");
    }

    #[test]
    fn render_with_info() {
        let group = DiagnosticsGroup::error("variable `?x` is already in use")
            .with_error(Span::new(22, 24), "variable `?x` used here")
            .with_info(Span::new(0, 2), "variable `?x` was first introduced here");
        let input = "?x <p> ?y . BIND(1 AS ?x)";
        insta::assert_snapshot!(group.render(input), @"
        error: variable `?x` is already in use
          ┌─ :1:23
          │
        1 │ ?x <p> ?y . BIND(1 AS ?x)
          │ --                    ^^ variable `?x` used here
          │ │                      
          │ variable `?x` was first introduced here
        ");
    }

    #[test]
    fn validation_error_render() {
        let group = DiagnosticsGroup::error("variable `?x` is already in use")
            .with_error(Span::new(22, 24), "variable `?x` used here")
            .with_info(Span::new(0, 2), "variable `?x` was first introduced here");
        let err = SparqlValidationError::single(group);
        let parse_err = SparqlParseError::from(err);
        let input = "?x <p> ?y . BIND(1 AS ?x)";
        insta::assert_snapshot!(parse_err.render(input), @"
        error: variable `?x` is already in use
          ┌─ :1:23
          │
        1 │ ?x <p> ?y . BIND(1 AS ?x)
          │ --                    ^^ variable `?x` used here
          │ │                      
          │ variable `?x` was first introduced here
        ");
    }
}
