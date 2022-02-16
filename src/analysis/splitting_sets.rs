use super::*;
use itertools::Itertools;

/// If the FBAS *doesn't* enjoy quorum intersection, this will just return `bitsetvec![{}]`...
pub fn find_minimal_splitting_sets(fbas: &Fbas) -> Vec<NodeIdSet> {
    find_minimal_splitting_sets_with_minimal_quorums(fbas, &find_minimal_quorums(fbas))
}

/// Finds all nodes that, if they want to help causing splits, can potentially benefit from lying
/// or changing their quorum sets for making quorums smaller by more than one node.
pub fn find_quorum_expanders(fbas: &Fbas) -> NodeIdSet {
    info!("Starting to look for minimal quorum reducing nodes...");
    let mut result = NodeIdSet::new();
    let mut processed = HashSet::new();
    for node in fbas.nodes.iter() {
        if !processed.contains(&node.quorum_set) {
            result.union_with(&node.quorum_set.quorum_expanders(fbas));
            processed.insert(node.quorum_set.clone());
        }
    }
    info!("Found {} quorum expanders.", result.len());
    result
}

/// If the FBAS *doesn't* enjoy quorum intersection, this will just return `bitsetvec![{}]`...
pub fn find_minimal_splitting_sets_with_minimal_quorums(
    fbas: &Fbas,
    minimal_quorums: &[NodeIdSet],
) -> Vec<NodeIdSet> {
    info!("Starting to look for minimal splitting sets...");
    let minimal_splitting_sets = find_minimal_sets(fbas, |clusters, fbas| {
        minimal_splitting_sets_finder(clusters, fbas, minimal_quorums)
    });
    info!(
        "Found {} minimal splitting sets.",
        minimal_splitting_sets.len()
    );
    minimal_splitting_sets
}

fn minimal_splitting_sets_finder(
    consensus_clusters: Vec<NodeIdSet>,
    fbas: &Fbas,
    minimal_quorums: &[NodeIdSet],
) -> Vec<NodeIdSet> {
    if consensus_clusters.len() > 1 {
        debug!("It's clear that we lack quorum intersection; the empty set is a splitting set.");
        bitsetvec![{}]
    } else if consensus_clusters.is_empty() {
        debug!("There aren't any quorums, and hence there are no splitting sets.");
        assert!(minimal_quorums.is_empty());
        bitsetvec![]
    } else {
        debug!("Finding minimal splitting sets...");
        let cluster_nodes = consensus_clusters.into_iter().next().unwrap();

        debug!("Finding nodes that can cause quorums to shrink significantly by changing their quorum set.");
        let quorum_expanders = find_quorum_expanders(fbas);
        debug!("Done.");

        let usable_symmetric_cluster = quorum_expanders
            .is_empty()
            .then(|| find_symmetric_cluster_in_consensus_cluster(&cluster_nodes, fbas))
            .flatten();

        if let Some(symmetric_cluster) = usable_symmetric_cluster {
            // TODO let's better do this in _step, whenever a selection consists of non-expanders?
            debug!("Cluster contains a symmetric quorum cluster! Extracting splitting sets...");
            symmetric_cluster.to_minimal_splitting_sets()
        } else {
            let mut relevant_nodes: Vec<NodeId> = cluster_nodes.union(&quorum_expanders).collect();

            debug!("Counting the number of affected nodes by each node...");
            let affected_nodes_per_node = find_affected_nodes_per_node(&fbas);
            debug!("Done.");

            debug!("Sorting nodes by the number of nodes they affect...");
            relevant_nodes.sort_by(|x, y| {
                affected_nodes_per_node[*y]
                    .len()
                    .partial_cmp(&affected_nodes_per_node[*x].len())
                    .unwrap()
            });
            debug!("Sorted.");

            println!(
                "{:?}",
                fbas.all_nodes()
                    .iter()
                    .map(|node| format!("{}: {}", node, fbas.nodes[node].public_key))
                    .collect::<Vec<String>>()
            );

            let unprocessed = relevant_nodes;
            let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());
            let mut available = unprocessed.iter().copied().collect();

            let affected_nodes_by_selection = fbas.all_nodes();
            let relevant_quorum_parts = minimal_quorums.to_vec();

            let mut found_splitting_sets: Vec<NodeIdSet> = vec![];

            debug!("Collecting splitting sets...");
            splitting_sets_finder_step(
                &mut unprocessed.into(),
                &mut selection,
                &mut available,
                &mut found_splitting_sets,
                fbas,
                &fbas.core_nodes(),
                &quorum_expanders,
                &affected_nodes_per_node,
                affected_nodes_by_selection,
                relevant_quorum_parts,
                true,
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
    unprocessed: &mut NodeIdDeque,
    selection: &mut NodeIdSet,
    available: &mut NodeIdSet,
    found_splitting_sets: &mut Vec<NodeIdSet>,
    fbas: &Fbas,
    core_nodes: &NodeIdSet,
    quorum_expanders: &NodeIdSet,
    affected_nodes_per_node: &[NodeIdSet],
    affected_nodes_by_selection: NodeIdSet,
    relevant_quorum_parts: Vec<NodeIdSet>,
    minimal_quorums_changed: bool,
) {
    if relevant_quorum_parts.len() < 2 && !unprocessed.iter().any(|&n| quorum_expanders.contains(n))
    {
        // return
    } else if minimal_quorums_changed && is_quorum_intersection(selection, &relevant_quorum_parts) {
        found_splitting_sets.push(selection.clone());
        if found_splitting_sets.len() % 1_000 == 0 {
            println!(
                "...{} splitting sets found; {:?}, {:?}",
                found_splitting_sets.len(),
                selection,
                affected_nodes_by_selection
            );
        }
        if found_splitting_sets.len() % 100_000 == 0 {
            debug!("...{} splitting sets found", found_splitting_sets.len());
        }
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        debug_assert!(
            affected_nodes_per_node[current_candidate].is_subset(&affected_nodes_by_selection)
        );

        selection.insert(current_candidate);

        // TODO: is "is_subset" part correct? we can assume that the ordering is topology-preserving?

        let mut affected_nodes = affected_nodes_by_selection.clone();
        affected_nodes.intersect_with(&affected_nodes_per_node[current_candidate]);

        let mut new_unprocessed = VecDeque::new();
        let mut new_available = selection.clone();
        for &node in unprocessed
            .iter()
            .filter(|&&node| affected_nodes_per_node[node].is_subset(&affected_nodes))
        {
            new_unprocessed.push_back(node);
            new_available.insert(node);
        }

        if quorum_expanders.contains(current_candidate) {
            let mut modified_fbas = fbas.clone();
            modified_fbas.assume_faulty(selection);

            let modified_fbas_core_nodes = modified_fbas.core_nodes();

            // // Not sure anymore if this is helpful...
            // if core_nodes.is_superset(&modified_fbas_core_nodes) {

            let (relevant_quorum_parts, changed) = if core_nodes.eq(&modified_fbas_core_nodes) {
                (relevant_quorum_parts.clone(), false)
            // } else if false {
            //     // TODO some optimization should be possible for if the core nodes shrank only
            //     // by the current candidate?
            } else {
                (find_minimal_quorums(&modified_fbas), true)
            };

            // probably no helpful at all
            // let modified_affected_nodes_per_node = find_affected_nodes_per_node(&fbas);

            splitting_sets_finder_step(
                &mut new_unprocessed,
                selection,
                &mut new_available,
                found_splitting_sets,
                &modified_fbas,
                &modified_fbas_core_nodes,
                quorum_expanders,
                &affected_nodes_per_node,
                affected_nodes,
                relevant_quorum_parts,
                changed,
            );
            // Not sure anymore if this is helpful...
            // } else {
            //     // We got different clusters!
            //     found_splitting_sets.extend(
            //         find_minimal_splitting_sets(&modified_fbas)
            //             .into_iter()
            //             .map(|mut ss| {
            //                 ss.union_with(&selection);
            //                 ss
            //             }),
            //     );
            // }
        } else {
            debug_assert!(core_nodes.contains(current_candidate));
            let mut updated_core_nodes = core_nodes.clone();
            updated_core_nodes.remove(current_candidate);

            let relevant_quorum_parts = relevant_quorum_parts
                .iter()
                .filter(|q| q.contains(current_candidate))
                .cloned()
                .map(|mut q| {
                    q.remove(current_candidate);
                    q
                })
                .collect();

            splitting_sets_finder_step(
                &mut new_unprocessed,
                selection,
                &mut new_available,
                found_splitting_sets,
                fbas,
                &updated_core_nodes,
                quorum_expanders,
                affected_nodes_per_node,
                affected_nodes,
                relevant_quorum_parts,
                true,
            );
        }
        selection.remove(current_candidate);
        available.remove(current_candidate);

        let relevant_quorum_parts_for_available: Vec<NodeIdSet> = relevant_quorum_parts
            .into_iter()
            .filter(|q| !q.is_disjoint(available))
            .collect();

        if has_potential(available, fbas, core_nodes) {
            splitting_sets_finder_step(
                unprocessed,
                selection,
                available,
                found_splitting_sets,
                fbas,
                core_nodes,
                quorum_expanders,
                affected_nodes_per_node,
                affected_nodes_by_selection,
                relevant_quorum_parts_for_available,
                false,
            );
        }
        unprocessed.push_front(current_candidate);
        available.insert(current_candidate);
    }
}

impl Node {
    fn is_slice_intersection(&self, node_set: &NodeIdSet) -> bool {
        self.quorum_set.is_slice_intersection(node_set)
    }
}
impl QuorumSet {
    /// Nodes that are not satisfied by one of my slices, thereby potentially expanding it to a
    /// larger quorum.
    fn quorum_expanders(&self, fbas: &Fbas) -> NodeIdSet {
        self.to_quorum_slices()
            .into_iter()
            .map(|slice| {
                slice
                    .iter()
                    .filter(|node| !fbas.nodes[*node].is_quorum_slice(&slice))
                    .collect::<Vec<NodeId>>()
            })
            .flatten()
            .collect()
    }
    fn is_slice_intersection(&self, node_set: &NodeIdSet) -> bool {
        let splitting_threshold = self.splitting_threshold();
        if splitting_threshold == 0 {
            true // everything is splitting what is already split
        } else {
            let found_validator_matches = self
                .validators
                .iter()
                .filter(|x| node_set.contains(**x))
                .take(splitting_threshold)
                .count();
            let found_inner_quorum_set_matches = self
                .inner_quorum_sets
                .iter()
                .filter(|x| x.is_slice_intersection(node_set))
                .take(splitting_threshold - found_validator_matches)
                .count();

            found_validator_matches + found_inner_quorum_set_matches == splitting_threshold
        }
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
        let mut subslice_groups: Vec<Vec<NodeIdSet>> = vec![];
        subslice_groups.extend(
            self.validators
                .iter()
                .map(|&node_id| vec![bitset![node_id]]),
        );
        subslice_groups.extend(
            self.inner_quorum_sets
                .iter()
                .map(|qset| qset.to_splitting_sets()),
        );
        let potential_splitting_sets: Vec<NodeIdSet> = subslice_groups
            .into_iter()
            .combinations(self.splitting_threshold())
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
            .concat();
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

// TODO was für preprocessing? (incl. test)
/// Nodes that point to nodes after a few steps; nodes also affect themselves.
fn find_affected_nodes_per_node(fbas: &Fbas) -> Vec<NodeIdSet> {
    let mut result: Vec<NodeIdSet> = (0..fbas.number_of_nodes())
        .map(|node_id| bitset! {node_id})
        .collect();
    let mut this_visit = NodeIdSet::new();
    let mut next_visit = fbas.all_nodes();
    let mut tmp = NodeIdSet::new(); // for fewer clones
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

fn is_quorum_intersection(selection: &NodeIdSet, relevant_quorum_parts: &[NodeIdSet]) -> bool {
    !selection.is_empty() && !all_intersect(relevant_quorum_parts)
}

// For pruning
fn has_potential(available: &NodeIdSet, fbas: &Fbas, core_nodes: &NodeIdSet) -> bool {
    // each node in available has either been a quorum expander or a core node;
    // so, available either can never be "all nodes", or all nodes have been in SCCs
    if available.is_disjoint(core_nodes) {
        let mut modified_fbas = fbas.clone();
        modified_fbas.assume_faulty(available);
        !modified_fbas.core_nodes().eq(core_nodes)
    } else {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn is_slice_intersection_2_of_3() {
        let qset = QuorumSet {
            threshold: 2,
            validators: vec![0, 1, 2],
            inner_quorum_sets: vec![],
        };
        assert!(qset.is_slice_intersection(&bitset![0]));
    }

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
        // We need to shrink the FBAS a bit as the analysis is too hard otherwise.
        let fbas = fbas.shrunken(fbas.core_nodes()).0;
        let minimal_quorums = bitsetvec![{0, 1}, {0, 3}, {1, 3}];
        assert_eq!(find_minimal_quorums(&fbas), minimal_quorums);

        let expected = vec![bitset![0], bitset![1], bitset![2], bitset![3]]; // 2 is Eno!
        let actual = find_minimal_splitting_sets_with_minimal_quorums(&fbas, &minimal_quorums);

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
        let actual = find_minimal_splitting_sets_with_minimal_quorums(&fbas, &minimal_quorums);
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
        let minimal_quorums = bitsetvec![{0, 1}];

        // no two quorums => no way to lose quorum intersection
        let expected: Vec<NodeIdSet> = vec![];
        let actual = find_minimal_splitting_sets_with_minimal_quorums(&fbas, &minimal_quorums);

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
        let minimal_quorums = bitsetvec![{0, 1}];

        // no two quorums => no way to lose quorum intersection
        let expected: Vec<NodeIdSet> = vec![];
        let actual = find_minimal_splitting_sets_with_minimal_quorums(&fbas, &minimal_quorums);

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
        let minimal_quorums = bitsetvec![{0, 1}, {1, 2}];
        let expected = vec![bitset![1]];
        let actual = find_minimal_splitting_sets_with_minimal_quorums(&fbas, &minimal_quorums);

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
        let minimal_quorums = bitsetvec![];

        // no two quorums => no way to lose quorum intersection
        let expected: Vec<NodeIdSet> = vec![];
        let actual = find_minimal_splitting_sets_with_minimal_quorums(&fbas, &minimal_quorums);

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
        let minimal_quorums = bitsetvec![{0, 1, 2, 3, 4}, {0, 1, 2, 3, 5}];
        assert_eq!(minimal_quorums, find_minimal_quorums(&fbas));

        let expected: Vec<NodeIdSet> = bitsetvec![{0, 2}, {0, 3}, {1, 2}, {1, 3}, {2, 3}];
        let actual = find_minimal_splitting_sets_with_minimal_quorums(&fbas, &minimal_quorums);
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
        let minimal_quorums = bitsetvec![{0, 1, 2}];
        assert_eq!(minimal_quorums, find_minimal_quorums(&fbas));

        let expected: Vec<NodeIdSet> = bitsetvec![{ 1 }];
        let actual = find_minimal_splitting_sets_with_minimal_quorums(&fbas, &minimal_quorums);
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
        let actual = find_minimal_splitting_sets_with_minimal_quorums(&fbas, &minimal_quorums);
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
        // let expected: Vec<NodeIdSet> = bitsetvec![
        //     {1, 2, 3, 4, 5, 6},
        //     {0, 2, 3, 4, 5, 6},
        //     {0, 1, 3, 4, 5, 6},
        //     {0, 1, 2, 4, 5, 6},
        //     {5, 6},
        //     {6},
        //     {}
        // ];
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
}
