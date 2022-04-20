use super::*;

extern crate pathfinding;
use pathfinding::directed::strongly_connected_components::strongly_connected_components;

pub type RankScore = f64;

impl Fbas {
    pub fn satisfiable_nodes(&self) -> NodeIdSet {
        find_satisfiable_nodes(&self.all_nodes(), self).0
    }
    pub fn unsatisfiable_nodes(&self) -> NodeIdSet {
        find_satisfiable_nodes(&self.all_nodes(), self).1
    }
    pub fn strongly_connected_components(&self) -> Vec<NodeIdSet> {
        partition_into_strongly_connected_components(&self.all_nodes(), self)
    }
    /// Rank all nodes in the FBAS using an algorithm vaguely resembling PageRank.
    pub fn rank_nodes(&self) -> Vec<RankScore> {
        let all_nodes: Vec<NodeId> = (0..self.nodes.len()).collect();
        rank_nodes(&all_nodes, self)
    }
    /// Returns all nodes part of a quorum-containing strongly connected component.
    pub fn core_nodes(&self) -> NodeIdSet {
        let sccs = partition_into_strongly_connected_components(&self.satisfiable_nodes(), self);
        let mut relevant_nodes = bitset![];
        for scc in sccs {
            if contains_quorum(&scc, self) {
                relevant_nodes.union_with(&scc);
            }
        }
        relevant_nodes
    }
    /// Returns all nodes v for which {v} is a quorum slice (and hence a quorum).
    /// These are often nodes that are somehow broken.
    pub fn one_node_quorums(&self) -> Vec<NodeId> {
        let mut nodes = vec![];
        for (node_id, node) in self.nodes.iter().enumerate() {
            if node.quorum_set.is_quorum_slice(&bitset![node_id]) {
                nodes.push(node_id);
            }
        }
        nodes
    }
    /// Removes all nodes that are not part of a quorum-containing strongly connected component
    /// (the FBAS "core"). Changes node IDs and causes splitting sets analyses to return only such
    /// splitting sets that can cause core nodes to diverge from each other!
    pub fn to_core(&self) -> Self {
        self.shrunken(self.core_nodes()).0
    }
    /// Removes all unsatisfiable nodes and reorders node IDs so that nodes are sorted by public
    /// key.
    pub fn to_standard_form(&self) -> Self {
        let shrunken_self = self.shrunken(self.satisfiable_nodes()).0;
        let mut raw_shrunken_self = shrunken_self.to_raw();
        raw_shrunken_self
            .0
            .sort_by_cached_key(|n| n.public_key.clone());
        Fbas::from_raw(raw_shrunken_self)
    }
    /// Remove `nodes` (referred to by their public keys) from the FBAS and all quorum sets,
    /// basically assuming they have irrevocably crashed. Changes the node IDs of remaining nodes!
    /// For a similar method hat keeps node IDs unchanged see [`Fbas::assume_crash_faulty`].
    pub fn without_nodes_pretty(&self, nodes: &[PublicKey]) -> Self {
        let nodes: Vec<usize> = nodes.iter().filter_map(|p| self.get_node_id(p)).collect();
        self.without_nodes(&nodes)
    }
    /// Remove `nodes` (referred to by their node IDs) from the FBAS and all quorum sets, basically
    /// assuming they have irrevocably crashed. Changes the node IDs of remaining nodes! For a
    /// similar method hat keeps node IDs unchanged see [`Fbas::assume_crash_faulty`].
    pub fn without_nodes(&self, nodes: &[NodeId]) -> Self {
        let mut remaining_nodes = self.all_nodes();
        for &node in nodes.iter() {
            remaining_nodes.remove(node);
        }
        self.shrunken(remaining_nodes).0
    }
}

/// Partitions `node_set` into the sets of `(satisfiable, unsatisfiable)` nodes.
pub fn find_satisfiable_nodes(node_set: &NodeIdSet, fbas: &Fbas) -> (NodeIdSet, NodeIdSet) {
    let (mut satisfiable, mut unsatisfiable): (NodeIdSet, NodeIdSet) = node_set
        .iter()
        .partition(|&x| fbas.nodes[x].quorum_set.is_quorum_slice(node_set));

    while let Some(unsatisfiable_node) = satisfiable
        .iter()
        .find(|&x| !fbas.nodes[x].quorum_set.is_quorum_slice(&satisfiable))
    {
        satisfiable.remove(unsatisfiable_node);
        unsatisfiable.insert(unsatisfiable_node);
    }
    (satisfiable, unsatisfiable)
}

/// Using implementation from `pathfinding` crate.
pub fn partition_into_strongly_connected_components(
    nodes: &NodeIdSet,
    fbas: &Fbas,
) -> Vec<NodeIdSet> {
    let sucessors = |&node_id: &NodeId| -> Vec<NodeId> {
        fbas.nodes[node_id]
            .quorum_set
            .contained_nodes()
            .into_iter()
            .collect()
    };
    let nodes: Vec<NodeId> = nodes.iter().collect();

    let sccs = strongly_connected_components(&nodes, sucessors);
    sccs.into_iter()
        .map(|x| x.into_iter().filter(|node| nodes.contains(node)).collect())
        .collect()
}

/// Rank nodes using an adaptation of the page rank algorithm (no dampening, fixed number of runs,
/// no distinction between validators and inner quorum set validators). Links from nodes not in
/// `nodes` are ignored.
// TODO dedup / harmonize this with Graph::get_rank_scores
pub fn rank_nodes(nodes: &[NodeId], fbas: &Fbas) -> Vec<RankScore> {
    let nodes_set: NodeIdSet = nodes.iter().cloned().collect();
    assert_eq!(nodes.len(), nodes_set.len());

    let runs = 100;
    let starting_score = 1. / nodes.len() as RankScore;

    let mut scores: Vec<RankScore> = vec![starting_score; fbas.nodes.len()];
    let mut last_scores: Vec<RankScore>;

    for _ in 0..runs {
        last_scores = scores;
        scores = vec![0.; fbas.nodes.len()];

        for node_id in nodes.iter().copied() {
            let node = &fbas.nodes[node_id];
            let trusted_nodes = node.quorum_set.contained_nodes();
            let l = trusted_nodes.len() as RankScore;

            for trusted_node_id in trusted_nodes
                .into_iter()
                .filter(|&id| nodes_set.contains(id))
            {
                scores[trusted_node_id] += last_scores[node_id] / l;
            }
        }
    }
    debug!(
        "Non-zero ranking scores: {:?}",
        scores
            .iter()
            .copied()
            .enumerate()
            .filter(|&(_, s)| s > 0.)
            .collect::<Vec<(usize, RankScore)>>()
    );
    scores
}

/// Rank nodes and sort them by "highest rank score first". Nodes with equal score are sorted by
/// node ID (lowest ID first).
pub fn sort_by_rank(nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeId> {
    let scores = rank_nodes(&nodes, fbas);

    sort_by_score(nodes, &scores)
}

/// Sort nodes by "highest score" first. Nodes with equal score are sorted by node ID (lowest ID
/// first).
pub fn sort_by_score(mut nodes: Vec<NodeId>, scores: &[RankScore]) -> Vec<NodeId> {
    use std::cmp::Ordering::*;
    nodes.sort_by(|x, y| match scores[*y].partial_cmp(&scores[*x]).unwrap() {
        Less => Less,
        Greater => Greater,
        Equal => x.partial_cmp(y).unwrap(),
    });
    nodes
}

/// Find nodes that are affected by each node v, i.e., that point to v after a few steps; nodes
/// also affect themselves.
pub fn find_affected_nodes_per_node(fbas: &Fbas) -> Vec<NodeIdSet> {
    let mut result: Vec<NodeIdSet> = (0..fbas.number_of_nodes())
        .map(|node_id| bitset! {node_id})
        .collect();
    let mut this_visit = NodeIdSet::new();
    let mut next_visit = fbas.all_nodes();
    let mut tmp = NodeIdSet::new(); // for fewer memory allocations
    while !next_visit.is_empty() {
        this_visit.clear();
        this_visit.union_with(&next_visit);
        next_visit.clear();
        for affected_node in this_visit.iter() {
            for affecting_node in fbas.nodes[affected_node]
                .quorum_set
                .contained_nodes()
                .iter()
            {
                tmp.clear();
                tmp.union_with(&result[affected_node]);
                if !result[affecting_node].is_superset(&tmp) {
                    result[affecting_node].union_with(&tmp);
                    next_visit.insert(affecting_node);
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn unsatisfiable_nodes_not_returned_as_relevant() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1", "n2"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1", "n3"] }
            }
        ]"#,
        );
        let actual = fbas.core_nodes();
        let expected = bitset![0, 1];
        assert_eq!(expected, actual);
    }

    #[test]
    fn one_node_quorums_are_relevant() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 1, "validators": ["n2", "n2"] }
            }
        ]"#,
        );
        let actual = fbas.core_nodes();
        let expected = bitset![0, 1, 2];
        assert_eq!(expected, actual);
    }

    #[test]
    fn one_node_quorums_returns_only_one_node_quorums() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 1, "validators": ["n1"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 1, "validators": ["n0"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 0, "validators": [] }
            }
        ]"#,
        );
        let actual = fbas.one_node_quorums();
        let expected = vec![1, 3];
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_core_removes_non_core_nodes() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n2"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n2"] }
            }
        ]"#,
        );
        let actual = fbas.to_core();
        let expected = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n2"] }
            }
        ]"#,
        );
        assert_eq!(expected, actual);
    }

    fn toy_standard_form_fbas() -> Fbas {
        Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            }
        ]"#,
        )
    }

    #[test]
    fn to_standard_form_no_change() {
        let fbas = toy_standard_form_fbas();
        assert_eq!(fbas, fbas.to_standard_form());
    }

    #[test]
    fn to_standard_form_reorders() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            }
        ]"#,
        );
        let expected = toy_standard_form_fbas();
        let actual = fbas.to_standard_form();
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_standard_form_filters_unsatisfiable() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n2", "n3"] }
            }
        ]"#,
        );
        let expected = toy_standard_form_fbas();
        let actual = fbas.to_standard_form();
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_standard_form_keeps_satisfiable_edge_nodes() {
        // because they can be relevant for determining splitting sets!
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            }
        ]"#,
        );
        let expected = fbas.clone();
        let actual = fbas.to_standard_form();
        assert_eq!(expected, actual);
    }

    #[test]
    fn standard_form_is_stable() {
        use hex;
        use sha3::{Digest, Sha3_256};

        let original_fbas =
            Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let original_fbas_actual_hash = hex::encode(Sha3_256::digest(
            &original_fbas.to_json_string().into_bytes(),
        ));
        let original_fbas_expected_hash =
            "fd16dab62d4d075def4ea787a44516b784bc9e9368f572faa7729af6f90f8e2c";

        assert_eq!(
            original_fbas_expected_hash, original_fbas_actual_hash,
            "The hash of the original FBAS changed - the test might not make much sense..."
        );

        let standard_form_fbas = original_fbas.to_standard_form();
        let standard_form_fbas_actual_hash = hex::encode(Sha3_256::digest(
            &standard_form_fbas.to_json_string().into_bytes(),
        ));
        let standard_form_fbas_expected_hash =
            "d7ffa370c12ea97a2c51c87b752ab89914081704b824caef660896eb68adb75d";

        assert_eq!(
            standard_form_fbas_expected_hash,
            standard_form_fbas_actual_hash
        );
    }

    #[test]
    fn remove_nodes_from_fbas() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1", "n2"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1", "n2"] }
            }
        ]"#,
        );
        let fbas = fbas.without_nodes_pretty(&[String::from("n2")]);

        assert_eq!(2, fbas.number_of_nodes());
        assert_eq!(None, fbas.get_node_id("n2"));
        assert_eq!(
            QuorumSet {
                validators: vec![0, 1],
                inner_quorum_sets: vec![],
                threshold: 2
            },
            fbas.get_quorum_set(0).unwrap()
        );
        assert_eq!(
            fbas.get_quorum_set(0).unwrap(),
            fbas.get_quorum_set(1).unwrap()
        );
    }

    #[test]
    fn affected_nodes_outside_symmetric_cluster() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1", "n2", "n3"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1", "n2", "n3"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1", "n2", "n3"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1", "n2", "n3"] }
            },
            {
                "publicKey": "n4",
                "quorumSet": { "threshold": 1, "validators": ["n2"] }
            },
            {
                "publicKey": "n5",
                "quorumSet": { "threshold": 2, "validators": ["n3", "n4"] }
            },
            {
                "publicKey": "n6",
                "quorumSet": { "threshold": 1, "validators": ["n5"] }
            }
        ]"#,
        );
        let expected: Vec<NodeIdSet> = bitsetvec![
            {0, 1, 2, 3, 4, 5, 6},
            {0, 1, 2, 3, 4, 5, 6},
            {0, 1, 2, 3, 4, 5, 6},
            {0, 1, 2, 3, 4, 5, 6},
            {4, 5, 6},
            {5, 6},
            {6}
        ];
        let actual = find_affected_nodes_per_node(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn find_transitively_unsatisfiable_nodes() {
        let mut fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        let directly_unsatisfiable = fbas.add_generic_node(QuorumSet {
            threshold: 1,
            validators: vec![],
            inner_quorum_sets: vec![],
        });
        let transitively_unsatisfiable = fbas.add_generic_node(QuorumSet {
            threshold: 1,
            validators: vec![directly_unsatisfiable],
            inner_quorum_sets: vec![],
        });

        fbas.nodes[0]
            .quorum_set
            .validators
            .push(directly_unsatisfiable);
        fbas.nodes[1]
            .quorum_set
            .validators
            .push(transitively_unsatisfiable);

        let all_nodes: NodeIdSet = (0..fbas.nodes.len()).collect();
        let (_, unsatisfiable) = find_satisfiable_nodes(&all_nodes, &fbas);

        assert!(unsatisfiable.contains(directly_unsatisfiable));
        assert!(unsatisfiable.contains(transitively_unsatisfiable));
    }

    #[test]
    fn sort_by_score_sorts_equivalent_nodes_by_node_id() {
        let nodes = vec![0, 5, 1, 2];
        let scores = vec![2., 0., 0.5, 0.3, 0.3, 0.5];

        let expected = vec![0, 2, 5, 1];
        let actual = sort_by_score(nodes, &scores);

        assert_eq!(expected, actual);
    }
}
