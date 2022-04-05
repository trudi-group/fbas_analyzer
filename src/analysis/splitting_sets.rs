use super::*;
use itertools::Itertools;
use std::iter::FromIterator;

/// If the FBAS *doesn't* enjoy quorum intersection, this will just return `bitsetvec![{}]`...
pub fn find_minimal_splitting_sets(fbas: &Fbas) -> Vec<NodeIdSet> {
    info!("Starting to look for minimal splitting sets...");
    let minimal_splitting_sets = find_minimal_sets(fbas, |clusters, fbas| {
        minimal_splitting_sets_finder(clusters, fbas)
    });
    info!(
        "Found {} minimal splitting sets.",
        minimal_splitting_sets.len()
    );
    minimal_splitting_sets
}

/// Finds all nodes that can potentially make quorums smaller by more than one node (i.e., more
/// than by just themselves) by changing their quorum sets or lying about them.
pub fn find_quorum_expanders(fbas: &Fbas) -> NodeIdSet {
    info!("Starting to look for quorum expanders...");
    let mut result = NodeIdSet::new();
    for quorum_set in fbas.nodes.iter().map(|node| &node.quorum_set).unique() {
        result.union_with(&quorum_set.quorum_expanders(fbas));
    }
    info!("Found {} quorum expanders.", result.len());
    result
}

fn minimal_splitting_sets_finder(
    consensus_clusters: Vec<NodeIdSet>,
    fbas: &Fbas,
) -> Vec<NodeIdSet> {
    // We'll be using `is_symmetric_cluster` multiple times, and it needs quorum sets to be in
    // "standard form".
    let fbas = fbas.with_standard_form_quorum_sets();

    if consensus_clusters.len() > 1 {
        debug!("It's clear that we lack quorum intersection; the empty set is a splitting set.");
        bitsetvec![{}]
    } else if consensus_clusters.is_empty() {
        debug!("There aren't any quorums, and hence there are no splitting sets.");
        bitsetvec![]
    } else {
        debug!("Finding minimal splitting sets...");
        let cluster_nodes = consensus_clusters.into_iter().next().unwrap();

        debug!("Finding quorum expanders...");
        let quorum_expanders = find_quorum_expanders(&fbas);
        debug!("Done.");

        // If there are quorum expanders then there might be smaller (and different) splitting sets
        // than what is suggested by the cluster's defining quorum set.
        let usable_symmetric_cluster = quorum_expanders
            .is_empty()
            .then(|| is_symmetric_cluster(&cluster_nodes, &fbas))
            .flatten();

        if let Some(symmetric_cluster) = usable_symmetric_cluster {
            debug!("Cluster contains a usable symmetric cluster! Extracting splitting sets...");
            symmetric_cluster.to_minimal_splitting_sets()
        } else {
            let relevant_nodes: Vec<NodeId> = cluster_nodes.union(&quorum_expanders).collect();

            debug!("Determining the set of affected nodes by each node...");
            let affected_per_node = find_affected_nodes_per_node(&fbas);
            debug!("Done.");

            debug!("Determining (page) rank scores");
            // non-cluster nodes will have 0 scores anyway
            let rank_scores = rank_nodes(&cluster_nodes.iter().collect::<Vec<NodeId>>(), &fbas);
            debug!("Done.");

            let combined_scores: Vec<RankScore> = rank_scores
                .into_iter()
                .enumerate()
                .map(|(i, score)| {
                    score + affected_per_node[i].len() as f64 / fbas.number_of_nodes() as f64
                })
                .collect();

            debug!("Sorting nodes by combined rank...");
            let sorted_nodes = sort_by_score(relevant_nodes, &combined_scores);
            debug!("Sorted.");

            debug!("Looking for symmetric nodes...");
            let symmetric_nodes = find_symmetric_nodes_in_node_set(&fbas.all_nodes(), &fbas);
            debug!("Done.");

            let mut found_splitting_sets = vec![];

            debug!("Collecting splitting sets...");
            splitting_sets_finder_step(
                &mut CandidateValues::new(sorted_nodes),
                &mut found_splitting_sets,
                FbasValues::new(fbas),
                &PrecomputedValues::new(combined_scores, symmetric_nodes.clone()),
            );
            debug!(
                "Found {} splitting sets. Reducing to minimal splitting sets...",
                found_splitting_sets.len()
            );
            let minimal_unexpanded_node_sets = remove_non_minimal_node_sets(found_splitting_sets);
            symmetric_nodes.expand_sets(minimal_unexpanded_node_sets)
        }
    }
}
fn splitting_sets_finder_step(
    candidates: &mut CandidateValues,
    found_splitting_sets: &mut Vec<NodeIdSet>,
    mut fbas: FbasValues,
    precomputed: &PrecomputedValues,
) {
    if fbas.consensus_clusters.is_empty() && !has_potential(candidates, &fbas) {
        // return
    } else if fbas.consensus_clusters_changed && !fbas.has_quorum_intersection(precomputed) {
        found_splitting_sets.push(candidates.selection.clone());
        if found_splitting_sets.len() % 100_000 == 0 {
            debug!("...{} splitting sets found", found_splitting_sets.len());
        }
    } else if let Some(current_candidate) = candidates.unprocessed.pop_front() {
        // Resetting this as we just checked for quorum intersection and the clusters didn't change
        // since then.
        fbas.consensus_clusters_changed = false;

        // We require that symmetric nodes are used in a fixed order; this way we can omit
        // redundant branches (we expand all combinations of symmetric nodes in the final result
        // sets).
        if precomputed
            .symmetric_nodes
            .is_non_redundant_next(current_candidate, &candidates.selection)
        {
            candidates.selection.insert(current_candidate);

            let modified_fbas = fbas.clone_assuming_faulty(&bitset![current_candidate]);

            splitting_sets_finder_step(
                candidates,
                found_splitting_sets,
                modified_fbas,
                precomputed,
            );
            candidates.selection.remove(current_candidate);
        }
        if has_potential(candidates, &fbas) {
            splitting_sets_finder_step(candidates, found_splitting_sets, fbas, precomputed);
        }
        candidates.unprocessed.push_front(current_candidate);
    }
}

#[derive(Debug, Clone)]
struct CandidateValues {
    selection: NodeIdSet,
    unprocessed: NodeIdDequeSet,
}
impl CandidateValues {
    fn new(sorted_nodes_to_process: Vec<NodeId>) -> Self {
        let selection = bitset![];
        let unprocessed = sorted_nodes_to_process.into_iter().collect();
        Self {
            selection,
            unprocessed,
        }
    }
}

#[derive(Debug, Clone)]
struct FbasValues {
    fbas: Fbas,
    consensus_clusters_changed: bool,
    sccs: Vec<NodeIdSet>,
    consensus_clusters: Vec<NodeIdSet>,
    faulty_nodes: NodeIdSet,
}
impl FbasValues {
    fn new(fbas: Fbas) -> Self {
        let sccs = partition_into_strongly_connected_components(&fbas.satisfiable_nodes(), &fbas);
        let consensus_clusters = sccs
            .iter()
            .filter(|scc| contains_quorum(scc, &fbas))
            .take(2)
            .cloned()
            .collect();
        Self {
            fbas,
            consensus_clusters,
            sccs,
            consensus_clusters_changed: true,
            faulty_nodes: bitset![],
        }
    }
    fn has_quorum_intersection(&self, precomputed: &PrecomputedValues) -> bool {
        debug_assert!(!self.consensus_clusters.is_empty()); // when we expect to call this
        if self.consensus_clusters.len() > 1 {
            false
        } else {
            let cluster = &self.consensus_clusters[0];
            if let Some(symmetric_cluster) = is_symmetric_cluster(cluster, &self.fbas) {
                symmetric_cluster.has_nonintersecting_quorums().is_none()
            } else {
                let sorted_nodes =
                    sort_by_score(cluster.iter().collect_vec(), &precomputed.ranking_scores);
                let nonintersecting_quorums =
                    nonintersecting_quorums_finder_using_sorted_nodes(sorted_nodes, &self.fbas);
                nonintersecting_quorums.len() < 2
            }
        }
    }
    fn clone_assuming_faulty(&self, faulty_nodes: &NodeIdSet) -> Self {
        let mut new_faulty_nodes = faulty_nodes.clone();
        new_faulty_nodes.difference_with(&self.faulty_nodes);

        let mut faulty_nodes = self.faulty_nodes.clone();
        faulty_nodes.union_with(&new_faulty_nodes);

        let mut fbas = self.fbas.clone();
        fbas.assume_split_faulty(&faulty_nodes);

        // sccs can't become bigger by adding faulty nodes
        let sccs = self
            .sccs
            .iter()
            .cloned()
            .flat_map(|mut scc| {
                if scc.is_disjoint(&new_faulty_nodes) {
                    vec![scc]
                } else {
                    scc.difference_with(&new_faulty_nodes);
                    partition_into_strongly_connected_components(&scc, &fbas)
                }
            })
            .collect_vec();

        let consensus_clusters = sccs
            .iter()
            .filter(|scc| contains_quorum(scc, &fbas))
            .take(2)
            .cloned()
            .collect_vec();

        let consensus_clusters_changed = !consensus_clusters.eq(&self.consensus_clusters);

        Self {
            fbas,
            consensus_clusters_changed,
            sccs,
            consensus_clusters,
            faulty_nodes,
        }
    }
}

struct PrecomputedValues {
    ranking_scores: Vec<RankScore>,
    symmetric_nodes: SymmetricNodesMap, // maintained for relevance to splitting sets
}
impl PrecomputedValues {
    fn new(ranking_scores: Vec<RankScore>, symmetric_nodes: SymmetricNodesMap) -> Self {
        Self {
            ranking_scores,
            symmetric_nodes,
        }
    }
}

#[derive(Debug, Default, Clone)]
struct NodeIdDequeSet {
    deque: NodeIdDeque,
    set: NodeIdSet,
}
impl NodeIdDequeSet {
    pub fn new() -> Self {
        Self::default()
    }
    fn pop_front(&mut self) -> Option<NodeId> {
        if let Some(popped_value) = self.deque.pop_front() {
            let existed = self.set.remove(popped_value);
            debug_assert!(existed);
            Some(popped_value)
        } else {
            None
        }
    }
    /// Notably, we don't avoid adding the same item twice here!
    fn push_front(&mut self, node_id: NodeId) {
        let is_new = self.set.insert(node_id);
        debug_assert!(is_new);
        self.deque.push_front(node_id);
    }
    /// Notably, we don't avoid adding the same item twice here!
    fn push_back(&mut self, node_id: NodeId) {
        let is_new = self.set.insert(node_id);
        debug_assert!(is_new);
        self.deque.push_back(node_id);
    }
}
impl FromIterator<NodeId> for NodeIdDequeSet {
    fn from_iter<T: IntoIterator<Item = NodeId>>(iter: T) -> Self {
        let mut new = Self::new();
        for node_id in iter.into_iter() {
            new.push_back(node_id);
        }
        new
    }
}

impl QuorumSet {
    /// Nodes that are in one of my slices but not satisfied by it, thereby potentially expanding
    /// it to a larger quorum.
    fn quorum_expanders(&self, fbas: &Fbas) -> NodeIdSet {
        let mut result = bitset![];
        if self.threshold > 0 {
            for slice in self.nonempty_slices_iter(|qset| qset.threshold) {
                for node in slice.iter() {
                    if !fbas.nodes[node].quorum_set.is_quorum_slice(&slice) {
                        result.insert(node);
                    }
                }
            }
        }
        result
    }
    /// If `self` represents a symmetric quorum cluster, this function returns all minimal splitting sets of the induced FBAS.
    fn to_minimal_splitting_sets(&self) -> Vec<NodeIdSet> {
        let splitting_sets = self.to_splitting_sets();
        if self.contains_duplicates() {
            remove_non_minimal_node_sets(splitting_sets)
        } else {
            splitting_sets
        }
    }
    /// If `self` represents a symmetric quorum cluster, this function returns all minimal splitting sets of the induced FBAS,
    /// but perhaps also a few extra...
    fn to_splitting_sets(&self) -> Vec<NodeIdSet> {
        let potential_splitting_sets = self.to_slices(|qset| qset.splitting_threshold());
        // check to see if we aren't really a 1-quorum quorum set
        if potential_splitting_sets.len() == 1 && self.is_quorum_slice(&potential_splitting_sets[0])
        {
            vec![]
        } else {
            potential_splitting_sets
        }
    }
    fn splitting_threshold(&self) -> usize {
        if 2 * self.threshold > (self.validators.len() + self.inner_quorum_sets.len()) {
            2 * self.threshold - (self.validators.len() + self.inner_quorum_sets.len())
        } else {
            0
        }
    }
}

// A heuristic for pruning
fn has_potential(candidates: &CandidateValues, fbas: &FbasValues) -> bool {
    let remaining = &candidates.unprocessed.set;
    debug_assert!(fbas.consensus_clusters.len() <= 1); // when we expect to call this

    // the remaining nodes can split off some of themselves
    remaining.iter().any(|node_id| fbas.fbas.nodes[node_id].is_quorum_slice(node_id, remaining))
        ||
    // the remaining nodes could split off some other nodes
    fbas.clone_assuming_faulty(remaining)
        .consensus_clusters_changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn quorum_expanders_in_3_ring() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
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
        let expected = bitset! {0, 1, 2};
        let actual = find_quorum_expanders(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn unsatisfiable_nodes_can_be_quorum_expanders() {
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
                "quorumSet": { "threshold": 4, "validators": ["n0", "n1", "n2"] }
            }
        ]"#,
        );
        let expected = bitset! {2};
        let actual = find_quorum_expanders(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_in_correct() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json")).to_core();

        let expected = vec![bitset![0], bitset![1], bitset![2], bitset![3]]; // One of these is Eno!
        let actual = find_minimal_splitting_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_in_different_consensus_clusters() {
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
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 2, "validators": ["n2", "n3"] }
            }
        ]"#,
        );
        let minimal_quorums = bitsetvec![{0, 1}, {2, 3}];
        assert_eq!(find_minimal_quorums(&fbas), minimal_quorums);

        // No quorum intersection => the FBAS is splitting even without faulty nodes => the empty
        // set is a splitting set.
        let actual = find_minimal_splitting_sets(&fbas);
        let expected = bitsetvec![{}];

        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_if_one_quorum() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 1, "validators": [] }
            }
        ]"#,
        );
        // no two quorums => no way to lose quorum intersection
        let expected: Vec<NodeIdSet> = vec![];
        let actual = find_minimal_splitting_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_if_one_quorum_and_symmetric() {
        let fbas = Fbas::from_json_str(
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
        );
        // no two quorums => no way to lose quorum intersection
        let expected: Vec<NodeIdSet> = vec![];
        let actual = find_minimal_splitting_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_if_two_quorums() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            }
        ]"#,
        );
        let expected = vec![bitset![1]];
        let actual = find_minimal_splitting_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_if_no_quorum() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            }
        ]"#,
        );
        // no two quorums => no way to lose quorum intersection
        let expected: Vec<NodeIdSet> = vec![];
        let actual = find_minimal_splitting_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_of_pyramid() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 1, "validators": ["n0"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 1, "validators": ["n0"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 1, "validators": ["n0"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            },
            {
                "publicKey": "n4",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n5",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n2"] }
            }
        ]"#,
        );
        let expected: Vec<NodeIdSet> = bitsetvec![{0}, {1, 2}];
        let actual = find_minimal_splitting_sets(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_of_weird_fbas() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1"], "innerQuorumSets": [
                    { "threshold": 1, "validators": ["n2", "n3"] }
                ]}
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1"], "innerQuorumSets": [
                    { "threshold": 1, "validators": ["n2", "n3"] }
                ]}
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 4, "validators": ["n2", "n3"], "innerQuorumSets": [
                    { "threshold": 1, "validators": ["n0", "n1"] },
                    { "threshold": 1, "validators": ["n4", "n5"] }
                ]}
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 4, "validators": ["n2", "n3"], "innerQuorumSets": [
                    { "threshold": 1, "validators": ["n0", "n1"] },
                    { "threshold": 1, "validators": ["n4", "n5"] }
                ]}
            },
            {
                "publicKey": "n4",
                "quorumSet": { "threshold": 2, "validators": ["n2", "n3"] }
            },
            {
                "publicKey": "n5",
                "quorumSet": { "threshold": 2, "validators": ["n2", "n3"] }
            }
        ]"#,
        );
        let expected: Vec<NodeIdSet> = bitsetvec![{0, 2}, {0, 3}, {1, 2}, {1, 3}, {2, 3}];
        let actual = find_minimal_splitting_sets(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_that_split_single_nodes_by_qset_lying() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            }
        ]"#,
        );
        let expected: Vec<NodeIdSet> = bitsetvec![{ 1 }];
        let actual = find_minimal_splitting_sets(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_that_split_multiple_nodes_by_qset_lying() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1", "n2"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 5, "validators": ["n0", "n1", "n2", "n3", "n4"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 3, "validators": ["n2", "n3", "n4"] }
            },
            {
                "publicKey": "n4",
                "quorumSet": { "threshold": 3, "validators": ["n2", "n3", "n4"] }
            }
        ]"#,
        );
        let minimal_quorums = bitsetvec![{0, 1, 2, 3, 4}];
        assert_eq!(minimal_quorums, find_minimal_quorums(&fbas));

        let expected: Vec<NodeIdSet> = bitsetvec![{ 2 }];
        let actual = find_minimal_splitting_sets(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_in_4_ring() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 1, "validators": ["n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 1, "validators": ["n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 1, "validators": ["n3"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 1, "validators": ["n0"] }
            }
        ]"#,
        );
        let expected: Vec<NodeIdSet> = bitsetvec![{0, 2}, {1, 3}];
        let actual = find_minimal_splitting_sets(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_in_six_node_cigar() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 3, "validators": ["n1", "n2", "n3"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n2", "n3"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 6, "validators": ["n0", "n1", "n2", "n3", "n4", "n5"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 6, "validators": ["n0", "n1", "n2", "n3", "n4", "n5"] }
            },
            {
                "publicKey": "n4",
                "quorumSet": { "threshold": 3, "validators": ["n2", "n3", "n5"] }
            },
            {
                "publicKey": "n5",
                "quorumSet": { "threshold": 3, "validators": ["n2", "n3", "n4"] }
            }
        ]"#,
        );
        let expected: Vec<NodeIdSet> = bitsetvec![{2, 3}];
        let actual = find_minimal_splitting_sets(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_in_almost_line() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 1, "validators": ["n0"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 2, "validators": ["n2", "n3"] }
            },
            {
                "publicKey": "n4",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n3", "n4"] }
            }
        ]"#,
        );
        let expected: Vec<NodeIdSet> = bitsetvec![{1}, {2}, {0, 3}];
        let actual = find_minimal_splitting_sets(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_splitting_sets_outside_symmetric_cluster() {
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
            },
            {
                "publicKey": "n7",
                "quorumSet": { "threshold": 1, "validators": ["n2"] }
            },
            {
                "publicKey": "n8",
                "quorumSet": { "threshold": 2, "validators": ["n3", "n7"] }
            }
        ]"#,
        );
        let expected: Vec<NodeIdSet> = bitsetvec![{2}, {5}, {0, 1}, {0, 3}, {1, 3}, {3, 4}, {3, 7}];
        let actual = find_minimal_splitting_sets(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn unsatisfiable_quorum_set_to_splitting_sets() {
        let quorum_set = QuorumSet::new_unsatisfiable();
        let expected: Vec<NodeIdSet> = bitsetvec![];
        let actual = quorum_set.to_splitting_sets();
        assert_eq!(expected, actual);
    }

    #[test]
    fn empty_quorum_set_to_splitting_sets() {
        let quorum_set = QuorumSet::new_empty();
        let expected: Vec<NodeIdSet> = bitsetvec![];
        let actual = quorum_set.to_splitting_sets();
        assert_eq!(expected, actual);
    }

    #[test]
    fn quorum_set_with_non_intersecting_slices_to_splitting_sets() {
        let quorum_set = QuorumSet::new(vec![0, 1, 2, 3], vec![], 2);
        let expected: Vec<NodeIdSet> = vec![bitset![]];
        let actual = quorum_set.to_splitting_sets();
        assert_eq!(expected, actual);
    }
}
