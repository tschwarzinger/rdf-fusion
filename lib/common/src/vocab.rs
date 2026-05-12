pub use oxrdf::vocab::*;

/// Well-known IRIs related to RDF Fusion.
pub mod rdf_fusion_vocab {
    use oxrdf::NamedNodeRef;

    /// Uses ZOrder to interleave the bits of multiple
    pub const ZORDER: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://rdf-fusion.org/sparql/functions#ZOrder");
}
