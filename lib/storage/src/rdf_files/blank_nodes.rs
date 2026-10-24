use md5::{Digest, Md5};
use rdf_fusion_common::{BlankNode, GraphName, NamedOrBlankNode, Quad, Term};

/// Renames blank nodes based on a random seed.
///
/// The new identifier only depends on the seed and the original label. Therefore, multiple
/// parsers that share a renamer (e.g., when parsing chunks of a file in parallel) rename the same
/// blank node consistently, without having to keep a mapping of all encountered blank nodes.
#[derive(Debug, Clone)]
pub struct BlankNodeRenamer {
    seed: String,
}

impl BlankNodeRenamer {
    /// Creates a new [`BlankNodeRenamer`] with a random seed.
    pub fn new_random() -> Self {
        Self {
            seed: BlankNode::default().into_string(),
        }
    }

    /// Returns the renamed blank node.
    pub fn rename(&self, blank_node: &BlankNode) -> BlankNode {
        let mut hasher = Md5::new();
        hasher.update(self.seed.as_bytes());
        hasher.update(blank_node.as_str().as_bytes());
        BlankNode::new_from_unique_id(u128::from_le_bytes(hasher.finalize().into()))
    }

    /// Renames all blank nodes in the given quad.
    pub fn rename_quad(&self, quad: &mut Quad) {
        if let NamedOrBlankNode::BlankNode(bnode) = &quad.subject {
            quad.subject = self.rename(bnode).into();
        }
        if let Term::BlankNode(bnode) = &quad.object {
            quad.object = self.rename(bnode).into();
        }
        if let GraphName::BlankNode(bnode) = &quad.graph_name {
            quad.graph_name = self.rename(bnode).into();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renaming_is_consistent() {
        let renamer = BlankNodeRenamer::new_random();
        let a = BlankNode::new_unchecked("a");
        let b = BlankNode::new_unchecked("b");

        assert_eq!(renamer.rename(&a), renamer.clone().rename(&a));
        assert_ne!(renamer.rename(&a), renamer.rename(&b));
        assert_ne!(renamer.rename(&a), a);
        assert_ne!(
            renamer.rename(&a),
            BlankNodeRenamer::new_random().rename(&a)
        );
    }
}
