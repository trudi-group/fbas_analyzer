use super::*;
use bit_set::BitSet;

use std::collections::VecDeque;

/// Create a **BitSet** from a list of elements.
///
/// ## Example
///
/// ```
/// #[macro_use] extern crate fba_quorum_analyzer;
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

impl Network {
    fn is_quorum(&self, node_set: &BitSet) -> bool {
        !node_set.is_empty() && node_set.iter().all(|x| self.nodes[x].is_quorum(&node_set))
    }
}
impl Node {
    fn is_quorum(&self, node_set: &BitSet) -> bool {
        self.quorum_set.is_quorum(node_set)
    }
}
impl QuorumSet {
    fn is_quorum(&self, node_set: &BitSet) -> bool {
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
    fn get_all_nodes(&self) -> Vec<NodeID> {
        let mut result = self.validators.clone();
        for inner_quorum_set in self.inner_quorum_sets.iter() {
            result.extend(inner_quorum_set.get_all_nodes());
        }
        result
    }
}

pub fn has_quorum_intersection(network: &Network) -> bool {
    all_node_sets_interesect(&get_minimal_quorums(network))
}

pub fn get_minimal_quorums(network: &Network) -> Vec<BitSet> {
    let n = network.nodes.len();
    let mut unprocessed: Vec<NodeID> = (0..n).collect();

    unprocessed = reduce_to_strongly_connected_components(unprocessed, network);
    println!(
        "Reducing removed {} of {} nodes...",
        n - unprocessed.len(),
        n
    );

    println!("Sorting...");
    unprocessed = sort_nodes_by_rank(unprocessed, network);
    println!("Sorted.");

    let mut selection = BitSet::with_capacity(n);
    let mut available = unprocessed.iter().cloned().collect();

    fn step(
        unprocessed: &mut VecDeque<NodeID>,
        selection: &mut BitSet,
        available: &mut BitSet,
        network: &Network,
    ) -> Vec<BitSet> {
        let mut result: Vec<BitSet> = vec![];

        if network.is_quorum(selection) {
            result.push(selection.clone());
        } else if let Some(current_candidate) = unprocessed.pop_front() {
            selection.insert(current_candidate);

            result.extend(step(unprocessed, selection, available, network));

            selection.remove(current_candidate);
            available.remove(current_candidate);

            if quorums_possible(selection, available, network) {
                result.extend(step(unprocessed, selection, available, network));
            }

            unprocessed.push_front(current_candidate);
            available.insert(current_candidate);
        }
        result
    }
    fn quorums_possible(selection: &BitSet, available: &BitSet, network: &Network) -> bool {
        selection
            .iter()
            .all(|x| network.nodes[x].is_quorum(available))
    }

    let quorums = step(
        &mut unprocessed.into(),
        &mut selection,
        &mut available,
        network,
    );
    println!("Found {} quorums...", quorums.len());

    let minimal_quorums = remove_non_minimal_node_sets(quorums);
    println!("Reduced to {} minimal quorums.", minimal_quorums.len());
    minimal_quorums
}

pub fn all_node_sets_interesect(node_sets: &[BitSet]) -> bool {
    node_sets
        .iter()
        .enumerate()
        .all(|(i, x)| node_sets.iter().skip(i + 1).all(|y| !x.is_disjoint(y)))
}

pub fn sort_nodes_by_rank(nodes: Vec<NodeID>, network: &Network) -> Vec<NodeID> {
    // a quick and dirty something resembling page rank
    // TODO not protected against overflows ...
    let mut scores: Vec<u64> = vec![1; network.nodes.len()];

    let runs = 10;

    for _ in 0..runs {
        let scores_snapshot = scores.clone();

        for node_id in nodes.iter().copied() {
            let node = &network.nodes[node_id];

            for trusted_node_id in node.quorum_set.get_all_nodes() {
                scores[trusted_node_id] += scores_snapshot[node_id];
            }
        }
    }

    let mut nodes = nodes;
    // sort by "highest score first"
    nodes.sort_by(|x, y| scores[*y].cmp(&scores[*x]));
    nodes
}

fn remove_non_minimal_node_sets(node_sets: Vec<BitSet>) -> Vec<BitSet> {
    let mut node_sets = node_sets;
    let mut minimal_node_sets: Vec<BitSet> = vec![];

    node_sets.sort_by(|x, y| x.len().cmp(&y.len()));

    for node_set in node_sets.into_iter() {
        if minimal_node_sets.iter().all(|x| !x.is_subset(&node_set)) {
            minimal_node_sets.push(node_set);
        }
    }
    minimal_node_sets
}

fn reduce_to_strongly_connected_components(nodes: Vec<NodeID>, network: &Network) -> Vec<NodeID> {
    // can probably be done faster
    let k = nodes.len();
    let reduced_once = remove_nodes_not_included_in_quorum_slices(nodes, network);

    if reduced_once.len() < k {
        reduce_to_strongly_connected_components(reduced_once, network)
    } else {
        reduced_once
    }
}

fn remove_nodes_not_included_in_quorum_slices(
    nodes: Vec<NodeID>,
    network: &Network,
) -> Vec<NodeID> {
    let mut included_nodes = BitSet::with_capacity(network.nodes.len());

    for node_id in nodes {
        let node = &network.nodes[node_id];
        for included_node in node.quorum_set.get_all_nodes() {
            included_nodes.insert(included_node);
        }
    }
    included_nodes.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node(validators: &[NodeID], threshold: usize) -> Node {
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
        let node_set = [1, 2, 3].iter().copied().collect();
        assert!(!node.is_quorum(&node_set));
    }

    #[test]
    fn is_quorum_if_quorum() {
        let node = test_node(&[0, 1, 2], 2);
        let node_set = [1, 2, 3].iter().copied().collect();
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
        let not_quorum = [1, 2, 3].iter().copied().collect();
        let quorum = [0, 3, 4, 5].iter().copied().collect();
        assert!(!node.is_quorum(&not_quorum));
        assert!(node.is_quorum(&quorum));
    }

    #[test]
    fn is_quorum_for_network() {
        let network = Network::from_json_file("test_data/correct_trivial.json");

        assert!(network.is_quorum(&vec![0, 1].into_iter().collect()));
        assert!(!network.is_quorum(&vec![0].into_iter().collect()));
    }

    #[test]
    fn empty_set_is_not_quorum() {
        let node = test_node(&[0, 1, 2], 2);
        assert!(!node.is_quorum(&BitSet::new()));

        let network = Network::from_json_file("test_data/correct_trivial.json");
        assert!(!network.is_quorum(&BitSet::new()));
    }

    #[test]
    fn get_minimal_quorums_correct_trivial() {
        let network = Network::from_json_file("test_data/correct_trivial.json");

        let expected = vec![bitset! {0, 1}, bitset! {0, 2}, bitset! {1, 2}];
        let actual = get_minimal_quorums(&network);

        assert_eq!(expected, actual);
    }

    #[test]
    fn get_minimal_quorums_broken_trivial() {
        let network = Network::from_json_file("test_data/broken_trivial.json");

        let expected = vec![bitset! {0}, bitset! {1, 2}];
        let actual = get_minimal_quorums(&network);

        assert_eq!(expected, actual);
    }

    #[test]
    fn get_minimal_quorums_broken_trivial_reversed_node_ids() {
        let mut network = Network::from_json_file("test_data/broken_trivial.json");
        network.nodes.reverse();

        let expected = vec![bitset! {2}, bitset! {0, 1}];
        let actual = get_minimal_quorums(&network);

        assert_eq!(expected, actual);
    }

    #[test]
    fn node_set_interesections() {
        assert!(all_node_sets_interesect(&vec![
            bitset! {0,1},
            bitset! {0,2},
            bitset! {1,2}
        ]));
        assert!(!all_node_sets_interesect(&vec![bitset! {0}, bitset! {1,2}]));
    }

    #[test]
    fn has_quorum_intersection_trivial() {
        let correct = Network::from_json_file("test_data/correct_trivial.json");
        let broken = Network::from_json_file("test_data/broken_trivial.json");

        assert!(has_quorum_intersection(&correct));
        assert!(!has_quorum_intersection(&broken));
    }

    #[test]
    fn has_quorum_intersection_nontrivial() {
        let correct = Network::from_json_file("test_data/correct.json");
        let broken = Network::from_json_file("test_data/broken.json");

        assert!(has_quorum_intersection(&correct));
        assert!(!has_quorum_intersection(&broken));
    }
}
