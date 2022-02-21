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

/// Finds all nodes that, if they want to help causing splits, can potentially benefit from lying
/// or changing their quorum sets for making quorums smaller by more than one node.
pub fn find_quorum_expanders(fbas: &Fbas) -> NodeIdSet {
    info!("Starting to look for minimal quorum reducing nodes...");
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
    if consensus_clusters.len() > 1 {
        debug!("It's clear that we lack quorum intersection; the empty set is a splitting set.");
        bitsetvec![{}]
    } else if consensus_clusters.is_empty() {
        debug!("There aren't any quorums, and hence there are no splitting sets.");
        bitsetvec![]
    } else {
        debug!("Finding minimal splitting sets...");
        let cluster_nodes = consensus_clusters.into_iter().next().unwrap();

        debug!("Finding nodes that can cause quorums to shrink significantly by changing their quorum set.");
        let quorum_expanders = find_quorum_expanders(fbas);
        debug!("Done.");

        // If a symmetric cluster contains quorum expanders there might be smaller splitting sets
        // than what is suggested by the cluster's defining quorum set.
        let usable_symmetric_cluster = quorum_expanders
            .is_disjoint(&cluster_nodes)
            .then(|| find_symmetric_cluster_in_consensus_cluster(&cluster_nodes, fbas))
            .flatten();

        if let Some(symmetric_cluster) = usable_symmetric_cluster {
            debug!("Cluster contains a usable symmetric cluster! Extracting splitting sets...");
            symmetric_cluster.to_minimal_splitting_sets()
        } else {
            let relevant_nodes: Vec<NodeId> = cluster_nodes.union(&quorum_expanders).collect();

            debug!("Determining the set of affected nodes by each node...");
            let affected_nodes_per_node = find_affected_nodes_per_node(fbas);
            debug!("Done.");

            debug!("Determining (page) rank scores");
            // non-cluster nodes will have 0 scores anyway
            let rank_scores = rank_nodes(&cluster_nodes.iter().collect::<Vec<NodeId>>(), fbas);
            debug!("Done.");

            let combined_scores: Vec<RankScore> = rank_scores
                .into_iter()
                .enumerate()
                .map(|(i, score)| {
                    score + affected_nodes_per_node[i].len() as f64 / fbas.number_of_nodes() as f64
                })
                .collect();

            debug!("Sorting nodes by combined rank...");
            let sorted_nodes = sort_by_score(relevant_nodes, &combined_scores);
            debug!("Sorted.");

            let mut selection = bitset![];
            let mut found_splitting_sets = vec![];

            let unprocessed = sorted_nodes.into_iter().collect();

            let nodes_affected_by_selection = fbas.all_nodes();

            debug!("Collecting splitting sets...");
            splitting_sets_finder_step(
                &mut selection,
                &mut found_splitting_sets,
                unprocessed,
                nodes_affected_by_selection,
                FbasValues::new(fbas),
                &PrecomputedValues {
                    quorum_expanders,
                    affected_nodes_per_node,
                    ranking_scores: combined_scores,
                },
            );
            debug!(
                "Found {} splitting sets. Reducing to minimal splitting sets...",
                found_splitting_sets.len()
            );
            remove_non_minimal_node_sets(found_splitting_sets)
        }
    }
}
fn splitting_sets_finder_step(
    selection: &mut NodeIdSet,
    found_splitting_sets: &mut Vec<NodeIdSet>,
    mut unprocessed: NodeIdDequeSet,
    nodes_affected_by_selection: NodeIdSet,
    fbas: FbasValues,
    precomputed: &PrecomputedValues,
) {
    if fbas.consensus_clusters.is_empty()
        && unprocessed.set.is_disjoint(&precomputed.quorum_expanders)
    {
        // return
    } else if fbas.consensus_clusters_changed && !fbas.has_quorum_intersection(precomputed) {
        found_splitting_sets.push(selection.clone());
        if found_splitting_sets.len() % 100_000 == 0 {
            debug!("...{} splitting sets found", found_splitting_sets.len());
        }
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        debug_assert!(precomputed.affected_nodes_per_node[current_candidate]
            .is_subset(&nodes_affected_by_selection));

        selection.insert(current_candidate);

        let mut nodes_affected_by_changed_selection = nodes_affected_by_selection.clone();
        nodes_affected_by_changed_selection
            .intersect_with(&precomputed.affected_nodes_per_node[current_candidate]);

        // We filter out all unprocessed nodes that aren't affecting a subset (⊆) of the currently
        // affected nodes. Not doing so will just result in non-minimal finds. We don't lose any
        // minimal finds because we expect that the nodes are sorted in a topology-preserving way.
        let relevant_unprocessed = unprocessed
            .deque
            .iter()
            .copied()
            .filter(|&node| {
                precomputed.affected_nodes_per_node[node]
                    .is_subset(&nodes_affected_by_changed_selection)
            })
            .collect();

        let modified_fbas = fbas.clone_assuming_faulty(&bitset![current_candidate]);

        splitting_sets_finder_step(
            selection,
            found_splitting_sets,
            relevant_unprocessed,
            nodes_affected_by_changed_selection,
            modified_fbas,
            precomputed,
        );
        selection.remove(current_candidate);

        if has_potential(&unprocessed, &fbas) {
            splitting_sets_finder_step(
                selection,
                found_splitting_sets,
                unprocessed,
                nodes_affected_by_selection,
                fbas,
                precomputed,
            );
        }
    }
}

#[derive(Debug, Default)]
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

#[derive(Debug, Clone)]
struct FbasValues {
    fbas: Fbas,
    consensus_clusters_changed: bool,
    sccs: Vec<NodeIdSet>,
    consensus_clusters: Vec<NodeIdSet>,
    faulty_nodes: NodeIdSet,
}
impl FbasValues {
    fn new(fbas: &Fbas) -> Self {
        let fbas = fbas.clone();
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
            if let Some(symmetric_cluster) =
                find_symmetric_cluster_in_consensus_cluster(cluster, &self.fbas)
            {
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
            .map(|mut scc| {
                if scc.is_disjoint(&new_faulty_nodes) {
                    vec![scc]
                } else {
                    scc.difference_with(&new_faulty_nodes);
                    partition_into_strongly_connected_components(&scc, &fbas)
                }
            })
            .flatten()
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
    affected_nodes_per_node: Vec<NodeIdSet>,
    quorum_expanders: NodeIdSet,
    ranking_scores: Vec<RankScore>,
}

impl QuorumSet {
    /// Nodes that are in one of my slices but not satisfied by it, thereby potentially expanding
    /// it to a larger quorum.
    fn quorum_expanders(&self, fbas: &Fbas) -> NodeIdSet {
        self.to_quorum_slices()
            .into_iter()
            .map(|slice| {
                slice
                    .iter()
                    .filter(|&node| !fbas.nodes[node].quorum_set.is_quorum_slice(&slice))
                    .collect::<Vec<NodeId>>()
            })
            .flatten()
            .collect()
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

// For pruning
fn has_potential(unprocessed: &NodeIdDequeSet, fbas: &FbasValues) -> bool {
    // each node in available has either been a quorum expander or a core node;
    // so, `remaining` either can never be "all nodes", or all nodes have been in SCCs,
    // i.e., all nodes are core nodes
    let remaining = &unprocessed.set;
    debug_assert!(fbas.consensus_clusters.len() <= 1);
    if fbas.consensus_clusters.is_empty() || remaining.is_disjoint(&fbas.consensus_clusters[0]) {
        fbas.clone_assuming_faulty(remaining)
            .consensus_clusters_changed
    } else {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn find_quorum_expanders_in_3_ring() {
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
    fn find_minimal_splitting_sets_in_correct() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));

        let expected = vec![bitset![0], bitset![1], bitset![4], bitset![10]]; // 4 is Eno!
        let actual = find_minimal_splitting_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_splitting_sets_in_different_consensus_clusters() {
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
    fn find_minimal_splitting_sets_if_one_quorum() {
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
    fn find_minimal_splitting_sets_if_one_quorum_and_symmetric() {
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
    fn find_minimal_splitting_sets_if_two_quorums() {
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
    fn find_minimal_splitting_sets_if_no_quorum() {
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
    fn find_minimal_splitting_sets_of_weird_fbas() {
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
    fn find_minimal_splitting_sets_that_split_single_nodes_by_qset_lying() {
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
    fn find_minimal_splitting_sets_that_split_multiple_nodes_by_qset_lying() {
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
    fn find_minimal_splitting_sets_in_4_ring() {
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
    fn find_minimal_splitting_sets_in_six_node_cigar() {
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
    fn find_minimal_splitting_sets_outside_symmetric_cluster() {
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
