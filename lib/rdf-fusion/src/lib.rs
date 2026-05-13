#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! RDF Fusion is an experimental columnar [SPARQL](https://www.w3.org/TR/sparql11-overview/) engine.
//! It is built on [Apache DataFusion](https://datafusion.apache.org/), an extensible query engine that
//! uses [Apache Arrow](https://arrow.apache.org/) as its in-memory data format.
//!
//! # Using RDF Fusion
//!
//! RDF Fusion can currently be used in two ways: via the convenient `Store` API or as a library for DataFusion.
//!
//! ## Store API
//!
//! The `Store` API provides high-level methods for interacting with the database, such as inserting data and running
//! queries.
//! Users who primarily want to *use* RDF Fusion are encouraged to use this API.
//!
//! While the `Store` API is based on [Oxigraph](https://github.com/oxigraph/oxigraph)'s `Store`, full compatibility is
//! not a goal.
//! Some aspects of RDF Fusion differ fundamentally, for example, its use of `async` methods.
//!
//! The `Store` API also supports extending SPARQL for domain-specific purposes.
//! For instance, users can register custom SPARQL functions, similar to those found in other SPARQL engines.
//! Additional extension points are planned for future releases.
//!
//! See the [examples](https://github.com/tobixdev/rdf-fusion/tree/main/examples) directory for
//! demonstrations of the `Store` API in action.
//!
//! ## Library Use
//!
//! RDF Fusion can also be used as a library for DataFusion.
//! In this mode, users interact directly with DataFusion's query engine and leverage RDF Fusion's operators and rewriting
//! rules used to implement SPARQL.
//!
//! This approach allows combining operators from DataFusion, RDF Fusion, and even other systems built on DataFusion within
//! a single query.
//! Users who want to *build new systems* using RDF Fusion's SPARQL implementation are encouraged to use this API.
//!
//! See the [examples](https://github.com/tobixdev/rdf-fusion/tree/main/examples) directory for more
//! details.
//!
//! # Background
//!
//! This documentation is intended to be somewhat self-sufficient for both users coming from the
//! Semantic Web and those coming from DataFusion.
//! To that end, the following sections introduce the basic concepts.
//! If you are already familiar with them, feel free to skip ahead.
//!
//! ## A Brief Introduction to DataFusion
//!
//! For readers who want an in-depth understanding of DataFusion, we recommend the official
//! [architecture documentation](https://docs.rs/datafusion/latest/datafusion/index.html#architecture).
//! For those who prefer to learn by doing, the following brief overview should be enough to get started.
//!
//! DataFusion is an extensible relational query engine built on top of Apache Arrow.
//! Let’s break this down:
//!
//! **Extensible** means that DataFusion can be customized in many ways, including adding new
//! operators, optimizations, and data sources. Much of this introduction will focus on these
//! aspects as otherwise RDF Fusion could not be built on top of DataFusion.
//!
//! **Relational** means that DataFusion is based on a variant of the
//! [relational model](https://en.wikipedia.org/wiki/Relational_model).
//! We won’t dive into the theory here: if you know the ideas of relations (tables), attributes
//! (columns), tuples (rows), and attribute domains (column data types), you’re in good shape.
//! Otherwise, you may want to skim through the linked resource first.
//!
//! [**Apache Arrow**](https://arrow.apache.org/) is an in-memory data format for high-performance analytics.
//! DataFusion stores intermediate results in Arrow arrays. An array is a contiguous sequence of
//! values with a certain type and known length. Arrow defines how arrays of various
//! (including complex) data types are represented in memory. This standardization:
//! 1. enables sharing data between different libraries and systems,
//! 2. provides a memory layout well-suited for efficient vectorized operations, and
//! 3. includes efficient compute kernels that extensions (such as RDF Fusion) can directly use.
//!
//! The high-level process of executing a query in DataFusion is shown below:
//!
//! ```text
//!                  rewrite               rewrite
//!                  ┌───◄──┐              ┌───◄──┐
//!                  ▼      │              ▼      │
//! ┌───────┐    ┌───────────────┐    ┌────────────────┐    ┌─────────┐
//! │ Query ├───►│ Logical PLan  ├───►│ Execution Plan ├───►│ Streams │   <--- Computes the results
//! └───────┘    └───────────────┘    └────────────────┘    └─────────┘
//! ```
//!
//! 1. A query is parsed by a front-end (most often SQL) and transformed into a **logical plan**.
//! 2. The logical plan is transformed using rewriting rules (e.g., optimizations).
//! 3. The optimized logical plan is translated into an **execution plan**.
//! 4. The execution plan is further transformed by rewriting rules (e.g., optimizations).
//! 5. Finally, the execution plan is executed, producing streams of results that can be lazily
//!    iterated.
//!
//! Usually, users do not interact with these APIs directly.
//! Instead, queries are typically executed via the DataFrame API or the SQL interface.
//! Internally, however, these APIs rely on the same mechanisms.
//! For example, when you use the `collect` method of the DataFrame API, it gathers all results
//! from the top-level stream to populate a DataFrame.
//!
//! ### Logical Plans vs. Execution Plans
//!
//! If you have never worked with query engines before, you might wonder why there are two different
//! types of plans.
//! The distinction is:
//! - The **logical plan** specifies *what* operation to perform (e.g., `Join`).
//! - The **execution plan** specifies *how* the operation should be performed
//!   (e.g., `HashJoin`, number of partitions to use, etc.).
//!
//! ### Rewriting Rules
//!
//! Rewriting rules serve multiple purposes. Their role is to take a plan (e.g., a logical plan)
//! and transform it into another plan.
//!
//! Often, the goal is to produce an equivalent plan that is more efficient (i.e., an optimization).
//! However, rewriting rules can also be used for tasks such as mapping custom operators onto
//! built-in operators.
//!
//! ### Extending DataFusion
//!
//! DataFusion provides extension points at all stages of query processing.
//! You can create custom front-ends, define new logical and execution plan nodes, and implement new streams.
//!
//! To implement a SPARQL operator entirely on our own, the process generally involves:
//! 1. Transforming it into a **custom logical plan**, possibly with custom optimization rules.
//! 2. Transforming that logical plan into a **custom execution plan**, again potentially with optimizations.
//! 3. Implementing a **custom stream** capable of executing the SPARQL operator.
//!
//! While this may sound complex, most SPARQL features can already be mapped to built-in DataFusion
//! operators, allowing us to replace steps 2. and 3. with an often simple rewriting rule.
//!
//! ## A Brief Introduction to RDF and SPARQL
//!
//! If you're familiar with relational databases, you might wonder how SPARQL queries can be implemented on a relational
//! query engine.
//! At first glance, these query languages appear quite different.
//!
//! However, despite surface-level differences, SPARQL engines share many similarities with relational query engines.
//! This common ground allows RDF Fusion to provide SPARQL support on top of DataFusion without re-implementing large
//! portions of the query engine.
//!
//! For readers familiar with relational databases who want to start using RDF Fusion without diving deep into the SPARQL
//! standard, this section provides a brief introduction to RDF and SPARQL.
//! If you are already familiar with these technologies, you can safely skip this section.
//!
//! Some details are simplified to make the introduction more accessible.
//! For example, we will completely ignore [blank nodes](https://www.w3.org/TR/rdf11-concepts/#dfn-blank-node).
//! For a detailed specification, please refer to the official RDF and SPARQL standards.
//!
//! ### The Resource Description Framework
//!
//! The [Resource Description Framework (RDF)](https://www.w3.org/TR/rdf11-concepts/) is the data model that underpins
//! SPARQL.
//! Data in RDF are represented as **triples**, where each triple consists of a **subject**, a **predicate**, and an
//! **object**.
//!
//! - The **subject** and **predicate** are
//!   typically [IRIs](https://en.wikipedia.org/wiki/Internationalized_Resource_Identifier).
//! - The **object** can be either an IRI or a **literal**.
//!
//! Think of IRIs as global identifiers that look similar to web links, while literals are standard values like strings,
//! numbers, or dates.
//! Lastly, an **RDF term** is either an IRI or a literal.
//!
//! For example, the following triple states that Spiderman (an IRI) has the name "Spiderman" (a literal):
//!
//! ```text
//! (<http://example.org/spiderman>, <http://xmlns.com/foaf/0.1/name>, "Spiderman")
//! ```
//!
//! An **RDF graph** is simply a set of triples.
//! The following example, taken from the [Turtle Specification](https://www.w3.org/TR/turtle/), shows a small graph
//! containing information about Spiderman, the Green Goblin, and their relationship.
//!
//! ```turtle
//! # Base Address to resolve relative IRIs
//! BASE <http://example.org/>
//!
//! # Some prefixes to make it easier to spell out other IRIs we are using
//! PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
//! PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
//! PREFIX foaf: <http://xmlns.com/foaf/0.1/> .
//! PREFIX rel: <http://www.perceive.net/schemas/relationship/> .
//!
//! <#spiderman>
//!     rel:enemyOf <#green-goblin> ;               # The Green Goblin is an enemy of Spiderman.
//!     a foaf:Person ;                             # Spiderman is a Person.
//!     foaf:name "Spiderman", "Человек-паук"@ru .  # You can even add language tags to your literals
//! ```
//!
//! At first, it may not be obvious how a set of triples represents a graph.
//! In an RDF graph, subjects and objects correspond to **nodes**, while predicates label the **edges** connecting them.
//! It is important that the same IRI always corresponds to the same node, even across multiple triples.
//!
//! ### Graph Patterns
//!
//! Given an RDF graph, we can ask questions about the data.
//! For example: *"Who are the enemies of Spiderman?"*
//!
//! These questions can be expressed as **graph patterns**, a core concept in the SPARQL standard.
//! A graph pattern is essentially a triple in which one or more components may be **variables**.
//!
//! For example, the following graph pattern expresses the question above (assuming the prefixes defined previously):
//!
//! ```text
//! <#spiderman> rel:enemyOf ?enemy
//! ```
//!
//! Evaluating graph patterns against an RDF graph is often referred to as **graph pattern matching**.
//! In this process, we look for triples in the graph that match the components of the graph pattern.
//! For example, the graph pattern above matches the following triple from the RDF graph introduced earlier:
//!
//! ```text
//! <#spiderman> rel:enemyOf <#green-goblin>
//! ```
//!
//! However, the result of graph pattern matching is not the triples themselves, but a **solution**.
//! A solution is a set of bindings for the variables in the graph pattern.
//! Here, we will depict solutions as a table, reflecting how SPARQL query execution can be mapped onto a relational query
//! engine (more on this later).
//! The result of the above graph pattern matching is the following table:
//!
//! | **?enemy**      |
//! |-----------------|
//! | <#green-goblin> |
//!
//! ### SPARQL
//!
//! Some relevant fundamental concepts of SPARQL were already covered in the previous section.
//! Here, we will dive a bit deeper to try to understand the connection between SPARQL and relational query engines.
//!
//! First, let's look at a simple SPARQL query.
//! The query below searches for all persons whose enemies contain the Green Goblin.
//! Note that the variable `?superhero` is used multiple times in the query.
//!
//! ```text
//! SELECT ?superhero
//! {
//!     ?superhero a foaf:Person .
//!     ?superhero rel:enemyOf <#green-goblin> .
//! }
//! ```
//!
//! The SPARQL standard specifies that the query above must find all solutions (i.e., bindings of `?superhero`) that
//! satisfy **both** graph patterns.
//!
//! In our approach, multiple graph patterns can be combined by **joining** them on the variables they share.
//! In the example above, the two graph patterns can be joined on the variable `?superhero`.
//! This produces the following (simplified) query plan:
//!
//! ```text
//! Inner Join: lhs.superhero = rhs.superhero
//!   SubqueryAlias: lhs
//!     TriplePattern: ?superhero a foaf:Person
//!   SubqueryAlias: rhs
//!     TriplePattern ?superhero rel:enemyOf <#green-goblin>
//! ```
//!
//! Users familiar with DataFusion should feel right at home.
//! We have effectively transformed the core operator of SPARQL into relational query operators!
//! Fortunately, DataFusion allows us to extend its set of operators with custom ones, such as `TriplePattern`.
//!
//! Next, let's consider an important challenge within this approach:
//! What is the result of the following query that retrieves all information about Spiderman?
//!
//! ```text
//! SELECT ?object
//! {
//!     <#spiderman> ?predicate ?object
//! }
//! ```
//!
//! Here is the result of the query:
//!
//! | **?object**       |
//! |-------------------|
//! | <#green-goblin>   |
//! | foaf:Person       |
//! | "Spiderman"       |
//! | "Человек-паук"@ru |
//!
//! Those familiar with relational databases might naturally wonder about the data type of this column.
//! After all, a single column can contain IRIs, plain strings, and language-tagged strings.
//! It may also include other literal types such as booleans, numbers, and dates.
//!
//! Such variability is not typically possible in standard relational models.
//! While the mathematical domain of the column is simple (the set of RDF terms), representing it efficiently in a
//! relational query engine is not trivial.
//!
//! This challenge motivates **RDF Fusion's RDF term encodings**, which bridge the gap between the dynamic nature of SPARQL
//! solutions and the expressive type system of Apache Arrow.
//! We will cover this topic in more detail in the next section.
//!
//! # SPARQL on top of DataFusion
//!
//! As RDF Fusion is built on top of DataFusion, it shares the same architecture of the query engine
//! which we have quickly covered in the previous section.
//! Nevertheless, there are interesting aspects of how we extend DataFusion to support SPARQL.
//! Here, we will briefly discuss various aspects of RDF Fusion and then link to the more detailed documentation.
//!
//! ## Encoding RDF Terms in Arrow
//!
//! Recall that within a solution set, one solution may map a variable to a string value, while another may map the same
//! variable to an integer value.
//! If this does not make sense to you, the previous section should help.
//!
//! If we were theoretical mathematicians, we could simply state that the domain of column within a SPARQL solution is the
//! set of RDF terms, and we would be done.
//! Easy enough, right?
//! Unfortunately, in practice, this is not so easy as Arrow would need a native Data Type for RDF terms for this to work.
//! As this is not the case, we must somehow encode the domain of RDF terms in the data types supported by Arrow.
//!
//! Furthermore, this encoding must support efficiently evaluating multiple types of operations.
//! For example, one operation is joining solution sets, while another one is allowing is evaluating arithmetic expressions.
//! As it turns out, doing this according to the SPARQL standard is not trivial.
//!
//! One of the major challenges lies in the "two worlds" that are associated with RDF literals.
//! To recap, on the one hand, RDF literals have a lexical value and an optional datatype (ignoring language tags for now).
//! On the other hand, the very same literal has a typed value that is part in a different domain.
//! The domain is determined by the datatype.
//! For example, the RDF term `"1"^^xsd:integer` has a typed value of `1` in the set of integers.
//! In addition, another RDF term `"01"^^xsd:integer` also has a typed value of `1` in the same domain.
//! Note that the necessary mapping functions (RDF term → Typed Value and vice versa) are not bijective.
//! In other words, there is no one-to-one mapping between RDF terms and typed values.
//!
//! Let us stay a bit longer on this example because this is a very important aspect of RDF Fusion.
//! Some SPARQL operations now would like to use the lexical value of a literal, while others would like to use the typed
//! value.
//! For example, the SPARQL join operation would like to use the lexical value of a literal, as the join operation is
//! defined
//! on RDF term equality, which in turn requires comparing the lexical values of the literals.
//! As a result, RDF Fusion cannot simply encode the typed value of a literal because it would lose information about the
//! lexical value.
//!
//! On the other hand, the SPARQL arithmetic operations would like to use the typed value of a literal, as the arithmetic
//! operations are defined on typed values.
//! For example, the SPARQL `+` operation does not care whether the lexical value of an integer literal is `"1"` or `"01"`.
//! It cares about the typed value of the literal, which is `1` in both cases.
//! Furthermore, while it is possible to extract the typed value of a literal (i.e., parsing), it is additional overhead
//! that must be accounted for in evaluating each sub-expression, as DataFusion uses Arrow arrays to pass data between
//! operators.
//! So evaluating a complex expression would be scattered with parsing and stringification operations if only the lexical
//! value of the literal was materialized.
//! As a result, also encoding just the lexical value of a literal would create problems.
//!
//! To address these challenges, RDF Fusion uses multiple encodings for the same domain of RDF terms.
//! One of the encodings retains the lexical value of the literal, while the other one retains the typed value.
//! Then there are additional encodings that we use to improve the performance of certain operations.
//! For further details, please refer to
//! the [rdf-fusion-encoding](https://docs.rs/rdf-fusion-encoding/) crate.
//!
//! ## Using DataFusion's Extension Points
//!
//! As mentioned earlier, RDF Fusion leverages many of DataFusion’s extension points to implement SPARQL.
//!
//! First, we define **custom logical operators** for various graph patterns (e.g., pattern matching, filters).
//! These custom logical operators and their rewriting rules are detailed in the [rdf-fusion-logical](https://docs.rs/rdf-fusion-logical/) crate.
//!
//! Next, we provide **custom execution operators** for operations that cannot be mapped to built-in operators
//! or for those where we want to preserve SPARQL semantics during the planning step.
//! The custom execution operators and their rewriting rules are detailed in the [rdf-fusion-physical](https://docs.rs/rdf-fusion-physical/) crate,
//! which also contains the implementations of the streams.
//!
//! One of the most important operators that integrates these components is the `QuadPattern`.
//! Its execution plan is defined in the [rdf-fusion-storage](https://docs.rs/rdf-fusion-storage/) crate.
//! This operator is special because the storage layer implementation is responsible for planning it.
//!
//! Additionally, RDF Fusion uses the `ScalarFunction` and `AggregateFunction` traits to implement SPARQL functions.
//! Implementations of these functions are detailed in the [rdf-fusion-functions](https://docs.rs/rdf-fusion-functions/) crate.
//!
//! # Crates
//!
//! To conclude, here is a list of the creates that constitute RDF Fusion with a quick description of each one.
//! You can find more details in their respective documentation.
//!
//! - [rdf-fusion-encoding](https://docs.rs/rdf-fusion-encoding/): The RDF term encodings used by
//!   RDF Fusion.
//! - [rdf-fusion-extensions](https://docs.rs/rdf-fusion-extensions/): Contains a set of traits and
//!   core data types used to extend RDF Fusion (e.g., custom storage layer).
//! - [rdf-fusion-functions](https://docs.rs/rdf-fusion-functions/): Scalar and aggregate functions
//!   for RDF Fusion.
//! - [rdf-fusion-logical](https://docs.rs/rdf-fusion-logical/): The logical plan operators and
//!   rewriting rules used by RDF Fusion.
//! - [rdf-fusion-common](https://docs.rs/rdf-fusion-common/): Provides a model for RDF and SPARQL.
//! - [rdf-fusion-physical](https://docs.rs/rdf-fusion-physical/): The physical plan operators and
//!   rewriting rules used by RDF Fusion.
//! - [rdf-fusion](https://docs.rs/rdf-fusion/): This crate. The primary entry point for RDF Fusion.
//! - [rdf-fusion-storage](https://docs.rs/rdf-fusion-storage/): The storage layer implementations
//!   for RDF Fusion.
//! - [rdf-fusion-web](https://docs.rs/rdf-fusion-web/): The web server for RDF Fusion.

pub mod error;
pub mod store;

pub mod api {
    pub use rdf_fusion_extensions::*;
}

pub mod encoding {
    pub use rdf_fusion_encoding::*;
}

pub mod functions {
    pub use rdf_fusion_functions::*;
}

pub mod common {
    pub use rdf_fusion_common::*;
}

pub mod logical {
    pub use rdf_fusion_logical::*;
}

pub mod execution {
    pub use rdf_fusion_execution::*;
}

pub mod storage {
    pub use rdf_fusion_storage::*;
}
