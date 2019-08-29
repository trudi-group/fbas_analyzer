use super::*;
use log::info;
use std::collections::BTreeMap;

impl Fbas {
    fn is_quorum(&self, node_set: &NodeIdSet) -> bool {
        !node_set.is_empty() && node_set.iter().all(|x| self.nodes[x].is_quorum(&node_set))
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

pub fn has_quorum_intersection(fbas: &Fbas) -> bool {
    all_node_sets_interesect(&find_minimal_quorums(fbas))
}

pub fn find_minimal_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    let n = fbas.nodes.len();
    let mut unprocessed: Vec<NodeId> = (0..n).collect();

    info!("Reducing to strongly connected components...");
    unprocessed = reduce_to_strongly_connected_components(unprocessed, fbas);
    info!(
        "Reducing removed {} of {} nodes...",
        n - unprocessed.len(),
        n
    );

    info!("Sorting nodes by rank...");
    unprocessed = sort_nodes_by_rank(unprocessed, fbas);
    info!("Sorted.");

    let mut selection = NodeIdSet::with_capacity(n);
    let mut available = unprocessed.iter().cloned().collect();

    fn step(
        unprocessed: &mut NodeIdDeque,
        selection: &mut NodeIdSet,
        available: &mut NodeIdSet,
        fbas: &Fbas,
    ) -> Vec<NodeIdSet> {
        let mut result: Vec<NodeIdSet> = vec![];

        if fbas.is_quorum(selection) {
            result.push(selection.clone());
        } else if let Some(current_candidate) = unprocessed.pop_front() {
            selection.insert(current_candidate);

            result.extend(step(unprocessed, selection, available, fbas));

            selection.remove(current_candidate);
            available.remove(current_candidate);

            if quorums_possible(selection, available, fbas) {
                result.extend(step(unprocessed, selection, available, fbas));
            }

            unprocessed.push_front(current_candidate);
            available.insert(current_candidate);
        }
        result
    }
    fn quorums_possible(selection: &NodeIdSet, available: &NodeIdSet, fbas: &Fbas) -> bool {
        selection.iter().all(|x| fbas.nodes[x].is_quorum(available))
    }

    let quorums = step(
        &mut unprocessed.into(),
        &mut selection,
        &mut available,
        fbas,
    );
    info!("Found {} quorums.", quorums.len());

    let minimal_quorums = remove_non_minimal_node_sets(quorums);
    info!("Reduced to {} minimal quorums.", minimal_quorums.len());
    minimal_quorums
}

pub fn all_node_sets_interesect(node_sets: &[NodeIdSet]) -> bool {
    node_sets
        .iter()
        .enumerate()
        .all(|(i, x)| node_sets.iter().skip(i + 1).all(|y| !x.is_disjoint(y)))
}

pub fn sort_nodes_by_rank(nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeId> {
    // a quick and dirty something resembling page rank
    // TODO not protected against overflows ...
    let mut scores: Vec<u64> = vec![1; fbas.nodes.len()];

    let runs = 10;

    for _ in 0..runs {
        let scores_snapshot = scores.clone();

        for node_id in nodes.iter().copied() {
            let node = &fbas.nodes[node_id];

            for trusted_node_id in node.quorum_set.contained_nodes().into_iter() {
                scores[trusted_node_id] += scores_snapshot[node_id];
            }
        }
    }

    let mut nodes = nodes;
    // sort by "highest score first"
    nodes.sort_by(|x, y| scores[*y].cmp(&scores[*x]));
    nodes
}

pub fn find_minimal_blocking_sets(quorums: &[NodeIdSet]) -> Vec<NodeIdSet> {
    // TODO has refactoring and performance tuning potential

    let mut quorum_memberships: BTreeMap<NodeId, NodeIdSet> = BTreeMap::new();
    let mut quorum_members: Vec<u32> = vec![0; quorums.len()];

    for (quorum_id, quorum) in quorums.iter().enumerate() {
        for node_id in quorum.iter() {
            (*quorum_memberships
                .entry(node_id)
                .or_insert_with(NodeIdSet::new))
            .insert(quorum_id);
            quorum_members[quorum_id] += 1;
        }
    }

    let mut unprocessed: Vec<NodeId> = quorum_memberships.keys().cloned().collect();
    // sort so that nodes included in many quorums are first
    unprocessed.sort_by(|x, y| {
        quorum_memberships[y]
            .len()
            .cmp(&quorum_memberships[x].len())
    });

    let mut unprocessed = NodeIdDeque::from(unprocessed);
    let mut selection = Vec::new();
    let missing_quorums: NodeIdSet = (0..quorums.len()).collect();

    fn step(
        unprocessed: &mut NodeIdDeque,
        selection: &mut Vec<NodeId>,
        remaining_quorum_members: &mut Vec<u32>,
        missing_quorums: NodeIdSet,
        quorum_memberships: &BTreeMap<NodeId, NodeIdSet>,
    ) -> Vec<NodeIdSet> {
        let mut result: Vec<NodeIdSet> = vec![];

        if missing_quorums.is_empty() {
            result.push(selection.iter().cloned().collect());
        } else if missing_quorums
            .iter()
            .all(|x| remaining_quorum_members[x] == 0)
        {
        } else if let Some(current_candidate) = unprocessed.pop_front() {
            for quorum_id in quorum_memberships[&current_candidate].iter() {
                remaining_quorum_members[quorum_id] -= 1;
            }

            selection.push(current_candidate);
            let mut updated_missing_quorums = missing_quorums.clone();
            for quorum in quorum_memberships[&current_candidate].iter() {
                updated_missing_quorums.remove(quorum);
            }
            result.extend(step(
                unprocessed,
                selection,
                remaining_quorum_members,
                updated_missing_quorums,
                quorum_memberships,
            ));

            selection.pop();
            result.extend(step(
                unprocessed,
                selection,
                remaining_quorum_members,
                missing_quorums,
                quorum_memberships,
            ));

            unprocessed.push_front(current_candidate);
            for quorum_id in quorum_memberships[&current_candidate].iter() {
                remaining_quorum_members[quorum_id] += 1;
            }
        }
        result
    }

    info!("Getting all blocking sets...");
    let blocking_sets = step(
        &mut unprocessed,
        &mut selection,
        &mut quorum_members,
        missing_quorums,
        &quorum_memberships,
    );
    info!("Found {} blocking sets.", blocking_sets.len());
    let minimal_blocking_sets = remove_non_minimal_node_sets(blocking_sets);
    info!(
        "Reduced to {} minimal blocking sets.",
        minimal_blocking_sets.len()
    );
    minimal_blocking_sets
}

fn remove_non_minimal_node_sets(node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
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

fn reduce_to_strongly_connected_components(nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeId> {
    // can probably be done faster
    let k = nodes.len();
    let reduced_once = remove_nodes_not_included_in_quorum_slices(nodes, fbas);

    if reduced_once.len() < k {
        reduce_to_strongly_connected_components(reduced_once, fbas)
    } else {
        reduced_once
    }
}

fn remove_nodes_not_included_in_quorum_slices(nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeId> {
    let mut included_nodes = NodeIdSet::with_capacity(fbas.nodes.len());

    for node_id in nodes {
        let node = &fbas.nodes[node_id];
        for included_node in node.quorum_set.contained_nodes().into_iter() {
            included_nodes.insert(included_node);
        }
    }
    included_nodes.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let fbas = Fbas::from_json_file("test_data/correct_trivial.json");

        assert!(fbas.is_quorum(&bitset![0, 1]));
        assert!(!fbas.is_quorum(&bitset![0]));
    }

    #[test]
    fn empty_set_is_not_quorum() {
        let node = test_node(&[0, 1, 2], 2);
        assert!(!node.is_quorum(&bitset![]));

        let fbas = Fbas::from_json_file("test_data/correct_trivial.json");
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
    fn find_minimal_quorums_correct_trivial() {
        let fbas = Fbas::from_json_file("test_data/correct_trivial.json");

        let expected = vec![bitset![0, 1], bitset![0, 2], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_quorums_broken_trivial() {
        let fbas = Fbas::from_json_file("test_data/broken_trivial.json");

        let expected = vec![bitset![0], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_quorums_broken_trivial_reversed_node_ids() {
        let mut fbas = Fbas::from_json_file("test_data/broken_trivial.json");
        fbas.nodes.reverse();

        let expected = vec![bitset![2], bitset![0, 1]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn node_set_interesections() {
        assert!(all_node_sets_interesect(&vec![
            bitset![0, 1],
            bitset![0, 2],
            bitset![1, 2]
        ]));
        assert!(!all_node_sets_interesect(&vec![bitset![0], bitset![1, 2]]));
    }

    #[test]
    fn has_quorum_intersection_trivial() {
        let correct = Fbas::from_json_file("test_data/correct_trivial.json");
        let broken = Fbas::from_json_file("test_data/broken_trivial.json");

        assert!(has_quorum_intersection(&correct));
        assert!(!has_quorum_intersection(&broken));
    }

    #[test]
    fn has_quorum_intersection_nontrivial() {
        let correct = Fbas::from_json_file("test_data/correct.json");
        let broken = Fbas::from_json_file("test_data/broken.json");

        assert!(has_quorum_intersection(&correct));
        assert!(!has_quorum_intersection(&broken));
    }

    #[test]
    fn find_minimal_blocking_sets_simple() {
        let minimal_quorums = vec![bitset![0, 1], bitset![0, 2]];

        let expected = vec![bitset![0], bitset![1, 2]];
        let actual = find_minimal_blocking_sets(&minimal_quorums);

        assert_eq!(expected, actual);
    }

    #[test]
    #[ignore]
    fn minimal_blocking_sets_more_minimal_than_minimal_quorums() {
        let fbas = Fbas::from_json_file("test_data/stellarbeat_2019-08-02.json");
        let minimal_quorums = find_minimal_quorums(&fbas);
        let minimal_blocking_sets = find_minimal_blocking_sets(&minimal_quorums);

        let minimal_all = remove_non_minimal_node_sets(
            minimal_blocking_sets
                .iter()
                .chain(minimal_quorums.iter())
                .cloned()
                .collect(),
        );

        assert_eq!(minimal_blocking_sets, minimal_all);
    }
}
