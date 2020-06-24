pub use bit_set::BitSet;
use itertools::Itertools;
pub use std::collections::BTreeSet;
pub use std::collections::HashMap;
pub use std::collections::HashSet;
pub use std::collections::VecDeque;
use std::mem;

use serde::Serialize;

pub type NodeId = usize; // internal and possibly different between runs
pub type PublicKey = String;

pub type NodeIdSet = BitSet;
pub type NodeIdDeque = VecDeque<NodeId>;

/// Create a `BitSet` from a list of elements.
///
/// ## Example
/// ```
/// #[macro_use] extern crate fbas_analyzer;
///
/// let set = bitset!{23, 42};
/// assert!(set.contains(23));
/// assert!(set.contains(42));
/// assert!(!set.contains(100));
/// ```
#[macro_export]
macro_rules! bitset {
    (@single $($x:tt)*) => (());
    (@count $($rest:expr),*) => (<[()]>::len(&[$(bitset!(@single $rest)),*]));

    () => { ::bit_set::BitSet::new(); };
    ($($key:expr,)+) => { bitset!($($key),+) };
    ($($key:expr),*) => {
        {
            let _cap = bitset!(@count $($key),*);
            let mut _set = ::bit_set::BitSet::with_capacity(_cap);
            $(
                let _ = _set.insert($key);
            )*
            _set
        }
    };
}

/// Create a `Vec<BitSet>` from a list of sets.
///
/// ## Example
/// ```
/// #[macro_use] extern crate fbas_analyzer;
///
/// let actual = bitsetvec![[0, 1], [23, 42]];
/// let expected = vec![bitset![0, 1], bitset![23, 42]];
/// assert_eq!(expected, actual);
/// ```
#[macro_export]
macro_rules! bitsetvec {
    ($($setcontent:tt),*) => {
        {
            vec![
            $(
                bitset!$setcontent
            ),*
            ]
        }
    };
}

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
#[derive(Clone, Eq, PartialEq, Debug, Default)]
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
            fbas.add_generic_node(QuorumSet::new());
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
                .all(|x| self.nodes[x].is_quorum_slice(&node_set))
    }
}
fn generate_generic_node_name(node_id: NodeId) -> String {
    format!("n{}", node_id)
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Node {
    pub(crate) public_key: PublicKey,
    pub(crate) quorum_set: QuorumSet,
}
impl Node {
    pub fn new(public_key: PublicKey) -> Self {
        let quorum_set = QuorumSet::new();
        Node {
            public_key,
            quorum_set,
        }
    }
    pub fn is_quorum_slice(&self, node_set: &NodeIdSet) -> bool {
        self.quorum_set.is_quorum_slice(node_set)
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuorumSet {
    pub threshold: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub validators: Vec<NodeId>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inner_quorum_sets: Vec<QuorumSet>,
}
impl QuorumSet {
    pub fn new() -> Self {
        QuorumSet {
            threshold: 0,
            validators: vec![],
            inner_quorum_sets: vec![],
        }
    }
    pub fn contained_nodes(&self) -> NodeIdSet {
        let mut nodes: NodeIdSet = self.validators.iter().cloned().collect();
        for inner_quorum_set in self.inner_quorum_sets.iter() {
            nodes.union_with(&inner_quorum_set.contained_nodes());
        }
        nodes
    }
    pub fn is_quorum_slice(&self, node_set: &NodeIdSet) -> bool {
        if self.threshold == 0 {
            false // badly configured quorum set
        } else {
            let found_validator_matches = self
                .validators
                .iter()
                .filter(|x| node_set.contains(**x))
                .take(self.threshold)
                .count();
            let found_inner_quorum_set_matches = self
                .inner_quorum_sets
                .iter()
                .filter(|x| x.is_quorum_slice(node_set))
                .take(self.threshold - found_validator_matches)
                .count();

            found_validator_matches + found_inner_quorum_set_matches == self.threshold
        }
    }
    /// Each valid quorum slice for this quorum set is a superset (i.e., equal to or a proper superset of)
    /// of at least one of the sets returned by this function.
    pub fn to_quorum_slices(&self) -> Vec<NodeIdSet> {
        let mut subslice_groups: Vec<Vec<NodeIdSet>> = vec![];
        subslice_groups.extend(
            self.validators
                .iter()
                .map(|&node_id| vec![bitset![node_id]]),
        );
        subslice_groups.extend(
            self.inner_quorum_sets
                .iter()
                .map(|qset| qset.to_quorum_slices()),
        );
        subslice_groups
            .into_iter()
            .combinations(self.threshold)
            .map(|group_combination| {
                group_combination
                    .into_iter()
                    .map(|subslice_group| subslice_group.into_iter())
                    .multi_cartesian_product()
                    .map(|subslice_combination| {
                        let mut slice = bitset![];
                        for node_set in subslice_combination.into_iter() {
                            slice.union_with(&node_set);
                        }
                        slice
                    })
                    .collect()
            })
            .concat()
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Organizations<'fbas> {
    pub(crate) organizations: Vec<Organization>,
    pub(crate) merged_ids: Vec<NodeId>,
    node_id_to_org_idx: HashMap<NodeId, usize>,
    // for ensuring fbas remains stable + serializeability via Serialize trait
    pub(crate) fbas: &'fbas Fbas,
}
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Organization {
    pub(crate) name: String,
    pub(crate) validators: Vec<NodeId>,
}
impl<'fbas> Organizations<'fbas> {
    pub fn new(organizations: Vec<Organization>, fbas: &'fbas Fbas) -> Self {
        let mut merged_ids: Vec<NodeId> = (0..fbas.nodes.len()).collect();
        let mut node_id_to_org_idx: HashMap<NodeId, usize> = HashMap::new();

        for (org_idx, org) in organizations.iter().enumerate() {
            let mut validator_it = org.validators.iter().copied();
            if let Some(merged_id) = validator_it.next() {
                node_id_to_org_idx.insert(merged_id, org_idx);
                for validator in validator_it {
                    merged_ids[validator] = merged_id;
                    node_id_to_org_idx.insert(validator, org_idx);
                }
            }
        }
        Organizations {
            organizations,
            merged_ids,
            node_id_to_org_idx,
            fbas,
        }
    }
    pub fn get_by_member(self: &Self, node_id: NodeId) -> Option<&Organization> {
        if let Some(&org_idx) = self.node_id_to_org_idx.get(&node_id) {
            Some(&self.organizations[org_idx])
        } else {
            None
        }
    }
    pub fn number_of_organizations(&self) -> usize {
        self.organizations.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn test_node(validators: &[NodeId], threshold: usize) -> Node {
        Node {
            public_key: Default::default(),
            quorum_set: QuorumSet {
                threshold,
                validators: validators.iter().copied().collect(),
                inner_quorum_sets: vec![],
            },
        }
    }

    #[test]
    fn new_node() {
        let node = Node::new("test".to_string());
        assert_eq!(node.public_key, "test");
        assert_eq!(
            node.quorum_set,
            QuorumSet {
                threshold: 0,
                validators: vec![],
                inner_quorum_sets: vec![]
            }
        );
    }

    #[test]
    #[should_panic]
    fn add_node_panics_on_duplicate_public_key() {
        let mut fbas = Fbas::new();
        let node = Node::new("test".to_string());
        fbas.add_node(node.clone());
        fbas.add_node(node);
    }

    #[test]
    fn is_quorum_slice_if_not_quorum_slice() {
        let node = test_node(&[0, 1, 2], 3);
        let node_set = bitset![1, 2, 3];
        assert!(!node.is_quorum_slice(&node_set));
    }

    #[test]
    fn is_quorum_if_quorum() {
        let node = test_node(&[0, 1, 2], 2);
        let node_set = bitset![1, 2, 3];
        assert!(node.is_quorum_slice(&node_set));
    }

    #[test]
    fn is_quorum_slice_with_inner_quorum_sets() {
        let mut node = test_node(&[0, 1], 3);
        node.quorum_set.inner_quorum_sets = vec![
            QuorumSet {
                threshold: 2,
                validators: vec![2, 3, 4],
                inner_quorum_sets: vec![],
            },
            QuorumSet {
                threshold: 2,
                validators: vec![4, 5, 6],
                inner_quorum_sets: vec![],
            },
        ];
        let not_quorum = bitset![1, 2, 3];
        let quorum = bitset![0, 3, 4, 5];
        assert!(!node.is_quorum_slice(&not_quorum));
        assert!(node.is_quorum_slice(&quorum));
    }

    #[test]
    fn is_quorum_for_fbas() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        assert!(fbas.is_quorum(&bitset![0, 1]));
        assert!(!fbas.is_quorum(&bitset![0]));
    }

    #[test]
    fn empty_set_is_not_quorum_slice() {
        let node = test_node(&[0, 1, 2], 2);
        assert!(!node.is_quorum_slice(&bitset![]));

        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));
        assert!(!fbas.is_quorum(&bitset![]));
    }

    #[test]
    fn quorum_set_with_threshold_0_trusts_no_one() {
        let node = test_node(&[0, 1, 2], 0);
        assert!(!node.is_quorum_slice(&bitset![]));
        assert!(!node.is_quorum_slice(&bitset![0]));
        assert!(!node.is_quorum_slice(&bitset![0, 1]));
        assert!(!node.is_quorum_slice(&bitset![0, 1, 2]));
    }

    #[test]
    fn quorum_set_to_quorum_slices_simple_no_intersect() {
        let quorum_set = QuorumSet {
            threshold: 1,
            validators: vec![0, 1, 2],
            inner_quorum_sets: vec![],
        };
        let expected = bitsetvec![[0], [1], [2]];
        let actual = quorum_set.to_quorum_slices();
        assert_eq!(expected, actual);
    }

    #[test]
    fn quorum_set_to_quorum_slices() {
        let quorum_set = QuorumSet {
            threshold: 3,
            validators: vec![0, 1],
            inner_quorum_sets: vec![
                QuorumSet {
                    threshold: 1,
                    validators: vec![2, 3],
                    inner_quorum_sets: vec![],
                },
                QuorumSet {
                    threshold: 3,
                    validators: vec![3, 4],
                    inner_quorum_sets: vec![QuorumSet {
                        threshold: 1,
                        validators: vec![5],
                        inner_quorum_sets: vec![],
                    }],
                },
            ],
        };
        let expected = bitsetvec![
            [0, 1, 2],
            [0, 1, 3],
            [0, 1, 3, 4, 5],
            [0, 2, 3, 4, 5],
            [0, 3, 4, 5],
            [1, 2, 3, 4, 5],
            [1, 3, 4, 5]
        ];
        let actual = quorum_set.to_quorum_slices();
        assert_eq!(expected, actual);
    }
}
