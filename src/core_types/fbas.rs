use super::*;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::mem;

/// Representation of an FBAS.
///
/// ## Example
/// ```
/// use fbas_analyzer::{Fbas, QuorumSet};
///
/// let fbas = Fbas::from_json_str(
///     r#"[
///     {
///         "publicKey": "n0",
///         "quorumSet": { "threshold": 1, "validators": ["n1"] }
///     },
///     {
///         "publicKey": "n1",
///         "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
///     },
///     {
///         "publicKey": "n2",
///         "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
///     }
/// ]"#,
/// );
/// assert_eq!(3, fbas.number_of_nodes());
/// assert_eq!(Some(0), fbas.get_node_id("n0"));
/// assert_eq!(
///     QuorumSet {
///         validators: vec![1],
///         inner_quorum_sets: vec![],
///         threshold: 1
///     },
///     fbas.get_quorum_set(0).unwrap()
/// );
///
/// let quorum_set = QuorumSet {
///     validators: vec![1, 2],
///     inner_quorum_sets: vec![],
///     threshold: 2,
/// };
/// let mut fbas = fbas;
/// fbas.swap_quorum_set(0, quorum_set.clone());
/// assert_eq!(Some(quorum_set), fbas.get_quorum_set(0));
/// ```
#[derive(Clone, Debug, Default)]
pub struct Fbas {
    pub(crate) nodes: Vec<Node>,
    pub(crate) pk_to_id: HashMap<PublicKey, NodeId>,
}
impl Fbas {
    /// FBAS of 0 nodes.
    pub fn new() -> Self {
        Fbas {
            nodes: vec![],
            pk_to_id: HashMap::new(),
        }
    }
    /// FBAS of `n` nodes with empty quorum sets
    pub fn new_generic_unconfigured(n: usize) -> Self {
        let mut fbas = Fbas::new();
        for _ in 0..n {
            fbas.add_generic_node(QuorumSet::new_empty());
        }
        fbas
    }
    pub fn add_node(&mut self, node: Node) -> NodeId {
        let node_id = self.nodes.len();
        // use expect_none here once it becomes stable
        if let Some(duplicate_id) = self.pk_to_id.insert(node.public_key.clone(), node_id) {
            panic!(
                "Duplicate public key {}",
                self.nodes[duplicate_id].public_key
            );
        }
        self.nodes.push(node);
        node_id
    }
    /// Add a node with generic `public_key`
    pub fn add_generic_node(&mut self, quorum_set: QuorumSet) -> NodeId {
        let node_id = self.nodes.len();
        self.add_node(Node {
            public_key: generate_generic_node_name(node_id),
            quorum_set,
        });
        node_id
    }
    pub fn get_node_id(&self, public_key: &str) -> Option<NodeId> {
        self.pk_to_id.get(&PublicKey::from(public_key)).copied()
    }
    pub fn get_quorum_set(&self, node_id: NodeId) -> Option<QuorumSet> {
        self.nodes.get(node_id).map(|node| node.quorum_set.clone())
    }
    pub fn swap_quorum_set(&mut self, node_id: NodeId, mut quorum_set: QuorumSet) -> QuorumSet {
        mem::swap(&mut self.nodes[node_id].quorum_set, &mut quorum_set);
        quorum_set
    }
    pub fn number_of_nodes(&self) -> usize {
        self.nodes.len()
    }
    pub fn all_nodes(&self) -> NodeIdSet {
        (0..self.nodes.len()).collect()
    }
    pub fn is_quorum(&self, node_set: &NodeIdSet) -> bool {
        !node_set.is_empty()
            && node_set
                .iter()
                .all(|x| self.nodes[x].is_quorum_slice(node_set))
    }
}
impl Hash for Fbas {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.nodes.hash(state);
    }
}
impl Ord for Fbas {
    fn cmp(&self, other: &Self) -> Ordering {
        self.nodes.cmp(&other.nodes)
    }
}
impl PartialOrd for Fbas {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for Fbas {}
impl PartialEq for Fbas {
    fn eq(&self, other: &Self) -> bool {
        self.nodes == other.nodes
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Node {
    pub(crate) public_key: PublicKey,
    pub(crate) quorum_set: QuorumSet,
}
impl Node {
    /// Returns a node with an empty quorum set that induces one-node quorums!
    pub fn new_unconfigured() -> Self {
        Node {
            public_key: PublicKey::default(),
            quorum_set: QuorumSet::new_empty(),
        }
    }
    pub fn is_quorum_slice(&self, node_set: &NodeIdSet) -> bool {
        self.quorum_set.is_quorum_slice(node_set)
    }
}

fn generate_generic_node_name(node_id: NodeId) -> String {
    format!("n{}", node_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    #[should_panic]
    fn add_node_panics_on_duplicate_public_key() {
        let mut fbas = Fbas::new();
        let node = Node {
            public_key: "test".to_string(),
            quorum_set: QuorumSet::new_empty(),
        };
        fbas.add_node(node.clone());
        fbas.add_node(node);
    }

    #[test]
    fn is_quorum_for_fbas() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        assert!(fbas.is_quorum(&bitset![0, 1]));
        assert!(!fbas.is_quorum(&bitset![0]));
    }

    #[test]
    fn empty_set_is_not_quorum() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));
        assert!(!fbas.is_quorum(&bitset![]));
    }
}
