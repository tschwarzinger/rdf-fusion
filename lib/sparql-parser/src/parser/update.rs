use crate::ast::{GraphOrDefault, GraphRefAll, Iri, QuadPatterns, Update, Update1};
use crate::error::SparqlSyntaxError;
use crate::parser::SparqlParser;

impl<'a> SparqlParser<'a> {
    /// Parses an `Update`, the top-level entry point for SPARQL updates.
    ///
    /// ```text
    /// Update ::= Prologue (Update1 (';' Update)?)?
    /// ```
    pub fn parse_update(&mut self) -> Result<Update<'a>, SparqlSyntaxError> {
        let mut prologue = self.parse_prologue()?;
        let mut operations = Vec::new();

        if self.at_end() {
            return Ok(Update {
                operations,
                trailing_prologue: prologue,
            });
        }

        loop {
            let operation = self.parse_update1()?;
            operations.push((std::mem::take(&mut prologue), operation));

            if !self.consume_operator(";") {
                break;
            }
            if self.at_end() {
                break;
            }
            prologue = self.parse_prologue()?;
        }

        if !self.at_end() {
            return self.expected("`;` or end of input");
        }

        Ok(Update {
            operations,
            trailing_prologue: prologue,
        })
    }

    /// Parses an `Update1`.
    ///
    /// ```text
    /// Update1 ::= Load | Clear | Drop | Add | Move | Copy | Create
    ///             | InsertData | DeleteData | DeleteWhere | Modify
    /// ```
    fn parse_update1(&mut self) -> Result<Update1<'a>, SparqlSyntaxError> {
        if self.peek_keyword("LOAD") {
            self.parse_load()
        } else if self.peek_keyword("CLEAR") {
            self.parse_clear()
        } else if self.peek_keyword("DROP") {
            self.parse_drop()
        } else if self.peek_keyword("CREATE") {
            self.parse_create()
        } else if self.peek_keyword("ADD") {
            self.parse_add()
        } else if self.peek_keyword("MOVE") {
            self.parse_move()
        } else if self.peek_keyword("COPY") {
            self.parse_copy()
        } else if self.peek_keyword("INSERT") && self.peek_keyword_at(1, "DATA") {
            self.bump();
            self.expect_keyword("DATA")?;
            let quads = self.parse_quad_pattern()?;
            Ok(Update1::InsertData { quads })
        } else if self.peek_keyword("DELETE") && self.peek_keyword_at(1, "DATA") {
            self.bump();
            self.expect_keyword("DATA")?;
            let quads = self.parse_quad_pattern()?;
            Ok(Update1::DeleteData { quads })
        } else if self.peek_keyword("DELETE") && self.peek_keyword_at(1, "WHERE") {
            self.parse_delete_where()
        } else if self.peek_keyword("WITH")
            || self.peek_keyword("DELETE")
            || self.peek_keyword("INSERT")
        {
            // A `Modify` can only start with `WITH`, `DELETE`, or `INSERT` (the
            // `INSERT DATA`/`DELETE DATA`/`DELETE WHERE` forms are caught above),
            // so only dispatch to it when one of those is present. This keeps
            // the error for a misspelled operation (e.g. `LOADD <g>`) from being
            // misleadingly reported as a missing `DELETE`/`INSERT` clause.
            self.parse_modify()
        } else {
            self.expected(
                "an update operation \
                 (LOAD, CLEAR, DROP, CREATE, ADD, MOVE, COPY, INSERT, DELETE, WITH)",
            )
        }
    }

    /// Parses a `Load`.
    ///
    /// ```text
    /// Load ::= 'LOAD' 'SILENT'? iri ('INTO' GraphRef)?
    /// ```
    fn parse_load(&mut self) -> Result<Update1<'a>, SparqlSyntaxError> {
        self.bump();
        let silent = self.parse_keyword("SILENT");
        let from = self.parse_iri()?;
        let to = if self.parse_keyword("INTO") {
            Some(self.parse_graph_ref()?)
        } else {
            None
        };
        Ok(Update1::Load { silent, from, to })
    }

    /// Parses a `Clear`.
    ///
    /// ```text
    /// Clear ::= 'CLEAR' 'SILENT'? GraphRefAll
    /// ```
    fn parse_clear(&mut self) -> Result<Update1<'a>, SparqlSyntaxError> {
        self.bump();
        let silent = self.parse_keyword("SILENT");
        let graph = self.parse_graph_ref_all()?;
        Ok(Update1::Clear { silent, graph })
    }

    /// Parses a `Drop`.
    ///
    /// ```text
    /// Drop ::= 'DROP' 'SILENT'? GraphRefAll
    /// ```
    fn parse_drop(&mut self) -> Result<Update1<'a>, SparqlSyntaxError> {
        self.bump();
        let silent = self.parse_keyword("SILENT");
        let graph = self.parse_graph_ref_all()?;
        Ok(Update1::Drop { silent, graph })
    }

    /// Parses a `Create`.
    ///
    /// ```text
    /// Create ::= 'CREATE' 'SILENT'? GraphRef
    /// ```
    fn parse_create(&mut self) -> Result<Update1<'a>, SparqlSyntaxError> {
        self.bump();
        let silent = self.parse_keyword("SILENT");
        let graph = self.parse_graph_ref()?;
        Ok(Update1::Create { silent, graph })
    }

    /// Parses a graph management operation (`Add`, `Move` or `Copy`), which all
    /// share the shape `'<KEY>' 'SILENT'? GraphOrDefault 'TO' GraphOrDefault`.
    ///
    /// ```text
    /// Add  ::= 'ADD'  'SILENT'? GraphOrDefault 'TO' GraphOrDefault
    /// ```
    fn parse_add(&mut self) -> Result<Update1<'a>, SparqlSyntaxError> {
        self.bump();
        let silent = self.parse_keyword("SILENT");
        let from = self.parse_graph_or_default()?;
        self.expect_keyword("TO")?;
        let to = self.parse_graph_or_default()?;
        Ok(Update1::Add { silent, from, to })
    }

    fn parse_move(&mut self) -> Result<Update1<'a>, SparqlSyntaxError> {
        self.bump();
        let silent = self.parse_keyword("SILENT");
        let from = self.parse_graph_or_default()?;
        self.expect_keyword("TO")?;
        let to = self.parse_graph_or_default()?;
        Ok(Update1::Move { silent, from, to })
    }

    fn parse_copy(&mut self) -> Result<Update1<'a>, SparqlSyntaxError> {
        self.bump();
        let silent = self.parse_keyword("SILENT");
        let from = self.parse_graph_or_default()?;
        self.expect_keyword("TO")?;
        let to = self.parse_graph_or_default()?;
        Ok(Update1::Copy { silent, from, to })
    }

    /// Parses a `DELETE WHERE`.
    ///
    /// ```text
    /// DeleteWhere ::= 'DELETE WHERE' QuadPattern
    /// ```
    fn parse_delete_where(&mut self) -> Result<Update1<'a>, SparqlSyntaxError> {
        self.bump();
        self.expect_keyword("WHERE")?;
        let pattern = self.parse_quad_pattern()?;
        Ok(Update1::DeleteWhere { pattern })
    }

    /// Parses a `Modify`.
    ///
    /// ```text
    /// Modify ::= ('WITH' iri)? (DeleteClause InsertClause? | InsertClause)
    ///            UsingClause* 'WHERE' GroupGraphPattern
    /// ```
    fn parse_modify(&mut self) -> Result<Update1<'a>, SparqlSyntaxError> {
        let with = if self.parse_keyword("WITH") {
            Some(self.parse_iri()?)
        } else {
            None
        };

        let mut delete = QuadPatterns::default();
        let mut insert = QuadPatterns::default();
        if self.parse_keyword("DELETE") {
            delete = self.parse_quad_pattern()?;
        }
        if self.parse_keyword("INSERT") {
            insert = self.parse_quad_pattern()?;
        }
        if delete.is_empty() && insert.is_empty() {
            return self.expected("a DELETE or INSERT clause");
        }

        let using = self.parse_graph_clauses("USING")?;

        self.expect_keyword("WHERE")?;
        let r#where = self.parse_group_graph_pattern()?;

        Ok(Update1::Modify {
            with,
            delete,
            insert,
            using,
            r#where,
        })
    }

    /// Parses a `GraphRef`, returning the named graph's IRI.
    ///
    /// ```text
    /// GraphRef ::= 'GRAPH' iri
    /// ```
    fn parse_graph_ref(&mut self) -> Result<Iri<'a>, SparqlSyntaxError> {
        self.expect_keyword("GRAPH")?;
        self.parse_iri()
    }

    /// Parses a `GraphRefAll`.
    ///
    /// ```text
    /// GraphRefAll ::= GraphRef | 'DEFAULT' | 'NAMED' | 'ALL'
    /// ```
    fn parse_graph_ref_all(&mut self) -> Result<GraphRefAll<'a>, SparqlSyntaxError> {
        if self.parse_keyword("DEFAULT") {
            return Ok(GraphRefAll::Default);
        }
        if self.parse_keyword("NAMED") {
            return Ok(GraphRefAll::Named);
        }
        if self.parse_keyword("ALL") {
            return Ok(GraphRefAll::All);
        }
        if self.parse_keyword("GRAPH") {
            let iri = self.parse_iri()?;
            return Ok(GraphRefAll::Graph(iri));
        }
        self.expected("GRAPH, DEFAULT, NAMED, or ALL")
    }

    /// Parses a `GraphOrDefault` (used by `ADD`, `MOVE` and `COPY`).
    ///
    /// ```text
    /// GraphOrDefault ::= 'DEFAULT' | 'GRAPH'? iri
    /// ```
    fn parse_graph_or_default(
        &mut self,
    ) -> Result<GraphOrDefault<'a>, SparqlSyntaxError> {
        if self.parse_keyword("DEFAULT") {
            return Ok(GraphOrDefault::Default);
        }
        self.parse_keyword("GRAPH");
        let iri = self.parse_iri()?;
        Ok(GraphOrDefault::Graph(iri))
    }

    /// Parses a `QuadPattern`/`QuadData`: a braced `Quads` block. The result is
    /// a list of quads groups, each an optional graph name plus a (possibly
    /// empty) triples block.
    ///
    /// ```text
    /// QuadPattern ::= '{' Quads '}'
    /// QuadData    ::= '{' Quads '}'
    /// Quads       ::= TriplesTemplate? (QuadsNotTriples '.'? TriplesTemplate?)*
    /// QuadsNotTriples ::= 'GRAPH' VarOrIri '{' TriplesTemplate? '}'
    /// ```
    fn parse_quad_pattern(&mut self) -> Result<QuadPatterns<'a>, SparqlSyntaxError> {
        self.expect_operator("{")?;
        let mut quads: QuadPatterns = QuadPatterns::default();

        loop {
            if self.peek_keyword("GRAPH") {
                self.bump();
                let name = self.parse_var_or_iri()?;
                self.expect_operator("{")?;
                let triples = if self.peek_triples_start() {
                    self.parse_triples_template()?.value
                } else {
                    Vec::new()
                };
                self.expect_operator("}")?;
                quads.push(Some(name), triples);
            } else if self.peek_triples_start() {
                let triples = self.parse_triples_template()?.value;
                quads.push(None, triples);
            } else {
                break;
            }
            self.consume_operator(".");
        }

        self.expect_operator("}")?;
        Ok(quads)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::GraphClause;
    use crate::ast::GraphNode;
    use crate::ast::PrologueDecl;
    use crate::ast::PropertyList;
    use crate::ast::pretty_printable;
    use crate::parser::test_helpers::*;
    use std::fmt::Write;

    /// Parses `input` as a full `Update`.
    fn parse_update(input: &str) -> Update<'_> {
        parse(input, |parser| parser.parse_update())
    }

    /// Renders an `Iri` compactly.
    fn render_iri(iri: &Iri<'_>) -> String {
        format!("{}", pretty_printable(&iri))
    }

    /// Renders a `QuadPatterns` compactly for assertions.
    fn render_quads(quads: &QuadPatterns<'_>) -> String {
        let mut out = String::new();
        for (graph, triples) in quads.iter() {
            match graph {
                Some(name) => {
                    write!(&mut out, "GRAPH {} ", pretty_printable(&name)).unwrap()
                }
                None => out.push_str("DEFAULT "),
            }
            render_block(&triples.patterns, &mut out);
            out.push_str("; ");
        }
        out
    }

    /// Renders a triples block without span noise.
    fn render_block(triples: &[(GraphNode<'_>, PropertyList<'_>)], out: &mut String) {
        for (subject, props) in triples {
            write!(out, "{}", pretty_printable(&subject)).unwrap();
            for (verb, objects) in props.iter() {
                write!(out, " {}", pretty_printable(&verb)).unwrap();
                for object in objects {
                    write!(out, " {}", pretty_printable(&object)).unwrap();
                }
            }
            out.push_str(" . ");
        }
    }

    #[test]
    fn empty_update() {
        let update = parse_update("");
        assert!(update.operations.is_empty());
        assert!(update.trailing_prologue.is_empty());
    }

    #[test]
    fn load() {
        let update = parse_update("LOAD <http://example.org/data.ttl>");
        match &update.operations[0].1 {
            Update1::Load { silent, from, to } => {
                assert!(!silent);
                assert_eq!(render_iri(from), "<http://example.org/data.ttl>");
                assert!(to.is_none());
            }
            _ => panic!("expected Load"),
        }
    }

    #[test]
    fn load_silent_into_graph() {
        let update =
            parse_update("LOAD SILENT <http://example.org/data.ttl> INTO GRAPH <g>");
        match &update.operations[0].1 {
            Update1::Load { silent, to, .. } => {
                assert!(silent);
                assert_eq!(render_iri(to.as_ref().unwrap()), "<g>");
            }
            _ => panic!("expected Load"),
        }
    }

    #[test]
    fn clear_and_drop() {
        let update = parse_update("CLEAR DEFAULT; DROP NAMED");
        match &update.operations[0].1 {
            Update1::Clear { silent, graph } => {
                assert!(!silent);
                assert!(matches!(graph, GraphRefAll::Default));
            }
            _ => panic!("expected Clear"),
        }
        match &update.operations[1].1 {
            Update1::Drop { graph, .. } => assert!(matches!(graph, GraphRefAll::Named)),
            _ => panic!("expected Drop"),
        }
    }

    #[test]
    fn clear_graph_iri() {
        let update = parse_update("CLEAR GRAPH <g>");
        match &update.operations[0].1 {
            Update1::Clear { graph, .. } => {
                assert!(matches!(graph, GraphRefAll::Graph(_)));
            }
            _ => panic!("expected Clear"),
        }
    }

    #[test]
    fn create() {
        let update = parse_update("CREATE SILENT GRAPH <g>");
        match &update.operations[0].1 {
            Update1::Create { silent, graph } => {
                assert!(silent);
                assert_eq!(render_iri(graph), "<g>");
            }
            _ => panic!("expected Create"),
        }
    }

    #[test]
    fn add_move_copy() {
        let update = parse_update(
            "ADD DEFAULT TO GRAPH <g>; MOVE GRAPH <a> TO <b>; COPY SILENT <a> TO DEFAULT",
        );
        match &update.operations[0].1 {
            Update1::Add { from, to, .. } => {
                assert!(matches!(from, GraphOrDefault::Default));
                assert!(matches!(to, GraphOrDefault::Graph(_)));
            }
            _ => panic!("expected Add"),
        }
        match &update.operations[1].1 {
            Update1::Move { from, to, .. } => {
                assert!(matches!(from, GraphOrDefault::Graph(_)));
                assert!(matches!(to, GraphOrDefault::Graph(_)));
            }
            _ => panic!("expected Move"),
        }
        match &update.operations[2].1 {
            Update1::Copy { silent, from, to } => {
                assert!(silent);
                assert!(matches!(from, GraphOrDefault::Graph(_)));
                assert!(matches!(to, GraphOrDefault::Default));
            }
            _ => panic!("expected Copy"),
        }
    }

    #[test]
    fn insert_data() {
        let update = parse_update("INSERT DATA { <s> <p> <o> }");
        match &update.operations[0].1 {
            Update1::InsertData { quads } => {
                assert_eq!(render_quads(quads), "DEFAULT <s> <p> <o> . ; ");
            }
            _ => panic!("expected InsertData"),
        }
    }

    #[test]
    fn insert_data_mixed_graph() {
        let update = parse_update(
            "INSERT DATA { <s1> <p1> <o1> GRAPH <G> { <s> <p1> 'o1' } <s2> <p2> <o2> }",
        );
        match &update.operations[0].1 {
            Update1::InsertData { quads } => {
                assert_eq!(
                    render_quads(quads),
                    "DEFAULT <s1> <p1> <o1> . ; GRAPH <G> <s> <p1> \"o1\" . ; DEFAULT <s2> <p2> <o2> . ; "
                );
            }
            _ => panic!("expected InsertData"),
        }
    }

    #[test]
    fn delete_data_named_graph() {
        let update = parse_update("DELETE DATA { GRAPH <g> { <s> <p> <o> } }");
        match &update.operations[0].1 {
            Update1::DeleteData { quads } => {
                assert_eq!(render_quads(quads), "GRAPH <g> <s> <p> <o> . ; ")
            }
            _ => panic!("expected DeleteData"),
        }
    }

    #[test]
    fn delete_where() {
        let update = parse_update("DELETE WHERE { ?s ?p ?o }");
        match &update.operations[0].1 {
            Update1::DeleteWhere { pattern } => {
                assert_eq!(render_quads(pattern), "DEFAULT ?s ?p ?o . ; ");
            }
            _ => panic!("expected DeleteWhere"),
        }
    }

    #[test]
    fn modify_delete_insert() {
        let update = parse_update(
            "DELETE { <s> <p> <o> } INSERT { <s> <p> <o2> } WHERE { <s> <p> ?o }",
        );
        match &update.operations[0].1 {
            Update1::Modify {
                with,
                delete,
                insert,
                using,
                r#where,
            } => {
                assert!(with.is_none());
                assert!(using.is_empty());
                assert_eq!(render_quads(delete), "DEFAULT <s> <p> <o> . ; ");
                assert_eq!(render_quads(insert), "DEFAULT <s> <p> <o2> . ; ");
                assert!(matches!(r#where, crate::ast::GraphPattern::Group(_)));
            }
            _ => panic!("expected Modify"),
        }
    }

    #[test]
    fn modify_insert_only_with_using() {
        let update = parse_update(
            "WITH <g> INSERT { <s> <p> <o> } USING NAMED <u> USING <d> WHERE { ?s ?p ?o }",
        );
        match &update.operations[0].1 {
            Update1::Modify {
                with,
                delete,
                insert,
                using,
                ..
            } => {
                assert_eq!(render_iri(with.as_ref().unwrap()), "<g>");
                assert!(delete.is_empty());
                assert_eq!(render_quads(insert), "DEFAULT <s> <p> <o> . ; ");
                assert!(matches!(using[0], GraphClause::Named(_)));
                assert!(matches!(using[1], GraphClause::Default(_)));
            }
            _ => panic!("expected Modify"),
        }
    }

    #[test]
    fn prologue_before_operation() {
        let update = parse_update(
            "PREFIX ex: <http://example.org/> INSERT DATA { ex:s ex:p ex:o }",
        );
        let (prologue, op) = &update.operations[0];
        assert_eq!(prologue.len(), 1);
        assert!(matches!(prologue[0], PrologueDecl::Prefix("ex", _)));
        assert!(matches!(op, Update1::InsertData { .. }));
    }

    #[test]
    fn multiple_operations_with_prologue() {
        let update = parse_update(
            "CLEAR ALL; PREFIX ex: <http://example.org/> INSERT DATA { ex:s ex:p ex:o }",
        );
        assert_eq!(update.operations.len(), 2);
        assert!(update.operations[0].0.is_empty());
        assert_eq!(update.operations[1].0.len(), 1);
    }

    fn render_update_err(input: &str) -> String {
        crate::render_internal(
            &match SparqlParser::new(input, default_registry()).parse_update() {
                Ok(_) => panic!("expected parse error for `{input}`"),
                Err(error) => error,
            },
            input,
        )
    }

    /// Parses `input` and re-renders it with the pretty-printer.
    fn render_update_pretty(input: &str) -> String {
        render(&parse_update(input))
    }

    /// Asserts that pretty-printing is stable under a parse → print →
    /// re-parse → re-print round-trip (i.e. the printed form re-parses into the
    /// same canonical representation).
    fn assert_round_trip(input: &str) {
        let once = render_update_pretty(input);
        let twice = render_update_pretty(&once);
        assert_eq!(
            once, twice,
            "\npretty-printed output is not a stable round-trip for `{input}`:\n  once:  {once}\n  twice: {twice}"
        );
    }

    #[test]
    fn round_trip_load() {
        assert_round_trip("LOAD <http://example.org/data.ttl>");
        assert_round_trip("LOAD SILENT <http://example.org/data.ttl> INTO GRAPH <g>");
    }

    #[test]
    fn round_trip_clear_drop_create() {
        assert_round_trip("CLEAR GRAPH <g>");
        assert_round_trip("CLEAR DEFAULT");
        assert_round_trip("CLEAR ALL");
        assert_round_trip("DROP NAMED");
        assert_round_trip("CREATE SILENT GRAPH <g>");
    }

    #[test]
    fn round_trip_add_move_copy() {
        assert_round_trip("ADD DEFAULT TO <g>");
        assert_round_trip("MOVE <a> TO DEFAULT");
        assert_round_trip("COPY SILENT <a> TO <b>");
    }

    #[test]
    fn round_trip_data_operations() {
        assert_round_trip("INSERT DATA { <s> <p> <o> }");
        assert_round_trip("INSERT DATA { <s> <p> <o>, <o2>; <q> \"v\" }");
        assert_round_trip("INSERT DATA { <s> <p> <o> . <a> <b> <c> }");
        assert_round_trip("DELETE DATA { GRAPH <g> { <s> <p> <o> } }");
        assert_round_trip("INSERT DATA { GRAPH <g> { <s> <p> <o> } <a> <b> <c> }");
        assert_round_trip("DELETE WHERE { ?s ?p ?o }");
    }

    #[test]
    fn round_trip_modify() {
        assert_round_trip(
            "DELETE { <s> <p> <o> } INSERT { <s> <p> <o2> } WHERE { ?s ?p ?o }",
        );
        assert_round_trip(
            "DELETE { GRAPH ?g { ?s ?p ?o } } WHERE { GRAPH ?g { ?s ?p ?o } }",
        );
        assert_round_trip(
            "WITH <g> DELETE { <s> <p> <o> } INSERT { <s> <p> <o2> } USING NAMED <u> USING <d> WHERE { ?s ?p ?o }",
        );
        assert_round_trip("INSERT { <s> <p> <o> } WHERE { ?s ?p ?o }");
    }

    #[test]
    fn round_trip_prologue_and_multiple() {
        assert_round_trip(
            "PREFIX ex: <http://example.org/> INSERT DATA { ex:s ex:p ex:o }",
        );
        assert_round_trip(
            "CLEAR ALL; PREFIX ex: <http://example.org/> INSERT DATA { ex:s ex:p ex:o }",
        );
        assert_round_trip("BASE <http://example.org/>");
    }

    #[test]
    fn error_unknown_operation() {
        insta::assert_snapshot!(render_update_err("FROBNICATE <g>"), @"
        error: expected an update operation (LOAD, CLEAR, DROP, CREATE, ADD, MOVE, COPY, INSERT, DELETE, WITH), found `FROBNICATE`
          ┌─ :1:1
          │
        1 │ FROBNICATE <g>
          │ ^^^^^^^^^^ expected an update operation (LOAD, CLEAR, DROP, CREATE, ADD, MOVE, COPY, INSERT, DELETE, WITH), found `FROBNICATE`
        ");
    }

    #[test]
    fn error_missing_where_in_modify() {
        insta::assert_snapshot!(render_update_err("INSERT { <s> <p> <o> }"), @"
        error: expected WHERE, found `end of input`
          ┌─ :1:23
          │
        1 │ INSERT { <s> <p> <o> }
          │                       ^ expected WHERE, found `end of input`
        ");
    }
}
