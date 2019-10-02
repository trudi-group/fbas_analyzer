use super::*;
use log::info;

mod find_blocking_sets;
mod find_intersections;
mod find_quorums;

pub use find_blocking_sets::find_minimal_blocking_sets;
pub use find_intersections::find_minimal_intersections;
pub use find_quorums::find_minimal_quorums;

impl Fbas {
    fn is_quorum(&self, node_set: &NodeIdSet) -> bool {
        !node_set.is_empty() && node_set.iter().all(|x| self.nodes[x].is_quorum(&node_set))
    }
    #[allow(dead_code)]
    fn has_quorum_intersection(&self) -> bool {
        all_interesect(&find_minimal_quorums(&self))
    }
}
impl Node {
    fn is_quorum(&self, node_set: &NodeIdSet) -> bool {
        self.quorum_set.is_quorum(node_set)
    }
}
impl QuorumSet {
    fn is_quorum(&self, node_set: &NodeIdSet) -> bool {
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
                .filter(|x| x.is_quorum(node_set))
                .take(self.threshold - found_validator_matches)
                .count();

            found_validator_matches + found_inner_quorum_set_matches == self.threshold
        }
    }
    fn contained_nodes(&self) -> NodeIdSet {
        let mut nodes: NodeIdSet = self.validators.iter().cloned().collect();
        for inner_quorum_set in self.inner_quorum_sets.iter() {
            nodes.union_with(&inner_quorum_set.contained_nodes());
        }
        nodes
    }
}

pub fn all_interesect(node_sets: &[NodeIdSet]) -> bool {
    node_sets
        .iter()
        .enumerate()
        .all(|(i, x)| node_sets.iter().skip(i + 1).all(|y| !x.is_disjoint(y)))
}

pub fn involved_nodes(node_sets: &[NodeIdSet]) -> NodeIdSet {
    let mut all_nodes: NodeIdSet = bitset![];
    for node_set in node_sets {
        all_nodes.union_with(node_set);
    }
    all_nodes
}

/// Reduce to minimal node sets, i.e. to a set of node sets so that no member set is a superset of another.
pub fn remove_non_minimal_node_sets(node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
    let mut node_sets = node_sets;
    let mut minimal_node_sets: Vec<NodeIdSet> = vec![];

    node_sets.sort_by(|x, y| x.len().cmp(&y.len()));

    for node_set in node_sets.into_iter() {
        if minimal_node_sets.iter().all(|x| !x.is_subset(&node_set)) {
            minimal_node_sets.push(node_set);
        }
    }
    minimal_node_sets
}

impl Organizations {
    /// Collapse a node ID so that all nodes by the same organization get the same ID.
    pub fn collapse_node(self: &Self, node_id: NodeId) -> NodeId {
        self.collapsed_ids[node_id]
    }
    /// Collapse a node ID set so that all nodes by the same organization get the same ID.
    pub fn collapse_node_set(self: &Self, node_set: NodeIdSet) -> NodeIdSet {
        node_set
            .into_iter()
            .map(|x| self.collapse_node(x))
            .collect()
    }
    /// Collapse a list of node ID sets so that all nodes by the same organization get the same ID.
    pub fn collapse_node_sets(self: &Self, node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
        node_sets
            .into_iter()
            .map(|x| self.collapse_node_set(x))
            .collect()
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
    fn is_quorum_if_not_quorum() {
        let node = test_node(&[0, 1, 2], 3);
        let node_set = bitset![1, 2, 3];
        assert!(!node.is_quorum(&node_set));
    }

    #[test]
    fn is_quorum_if_quorum() {
        let node = test_node(&[0, 1, 2], 2);
        let node_set = bitset![1, 2, 3];
        assert!(node.is_quorum(&node_set));
    }

    #[test]
    fn is_quorum_with_inner_quorum_sets() {
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
        assert!(!node.is_quorum(&not_quorum));
        assert!(node.is_quorum(&quorum));
    }

    #[test]
    fn is_quorum_for_fbas() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        assert!(fbas.is_quorum(&bitset![0, 1]));
        assert!(!fbas.is_quorum(&bitset![0]));
    }

    #[test]
    fn empty_set_is_not_quorum() {
        let node = test_node(&[0, 1, 2], 2);
        assert!(!node.is_quorum(&bitset![]));

        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));
        assert!(!fbas.is_quorum(&bitset![]));
    }

    #[test]
    fn quorum_set_with_threshold_0_trusts_no_one() {
        let node = test_node(&[0, 1, 2], 0);
        assert!(!node.is_quorum(&bitset![]));
        assert!(!node.is_quorum(&bitset![0]));
        assert!(!node.is_quorum(&bitset![0, 1]));
        assert!(!node.is_quorum(&bitset![0, 1, 2]));
    }

    #[test]
    fn node_set_interesections() {
        assert!(all_interesect(&vec![
            bitset![0, 1],
            bitset![0, 2],
            bitset![1, 2]
        ]));
        assert!(!all_interesect(&vec![bitset![0], bitset![1, 2]]));
    }

    #[test]
    fn has_quorum_intersection_trivial() {
        let correct = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));
        let broken = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));

        assert!(correct.has_quorum_intersection());
        assert!(!broken.has_quorum_intersection());
    }

    #[test]
    fn has_quorum_intersection_nontrivial() {
        let correct = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let broken = Fbas::from_json_file(Path::new("test_data/broken.json"));

        assert!(correct.has_quorum_intersection());
        assert!(!broken.has_quorum_intersection());
    }

    #[test]
    fn minimal_node_sets() {
        let non_minimal = vec![bitset![0, 1, 2], bitset![0, 1], bitset![0, 2]];

        let expected = vec![bitset![0, 1], bitset![0, 2]];
        let actual = remove_non_minimal_node_sets(non_minimal);
        assert_eq!(expected, actual);
    }

    #[test]
    fn collapse_node_sets_organization() {
        let fbas_input = r#"[
            {
                "publicKey": "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH"
            },
            {
                "publicKey": "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ"
            },
            {
                "publicKey": "GCWJKM4EGTGJUVSWUJDPCQEOEP5LHSOFKSA4HALBTOO4T4H3HCHOM6UX"
            }]"#;
        let organizations_input = r#"[
            {
                "id": "266107f8966d45eedce41fee2581326d",
                "name": "Stellar Development Foundation",
                "validators": [
                    "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK",
                    "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                    "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ"
                ]
            }]"#;
        let fbas = Fbas::from_json_str(&fbas_input);
        let organizations = Organizations::from_json_str(&organizations_input, &fbas);

        let node_sets = vec![bitset![0], bitset![1, 2]];

        let expected = vec![bitset![0], bitset![0, 2]];
        let actual = organizations.collapse_node_sets(node_sets);

        assert_eq!(expected, actual);
    }
}
