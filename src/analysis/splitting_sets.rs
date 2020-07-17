use super::*;
use itertools::Itertools;

/// If the FBAS *doesn't* enjoy quorum intersection, this returns the minimal splitting sets of all
/// sub-FBASs. As this is probably not what you want then, you should check for quorum
/// intersection before using this function.
pub fn find_minimal_splitting_sets(fbas: &Fbas, minimal_quorums: &[NodeIdSet]) -> Vec<NodeIdSet> {
    info!("Starting to look for minimal splitting_sets...");
    let minimal_splitting_sets = find_minimal_sets(fbas, |clusters, fbas| {
        minimal_splitting_sets_finder(clusters, fbas, minimal_quorums)
    });
    info!(
        "Found {} minimal splitting_sets.",
        minimal_splitting_sets.len()
    );
    minimal_splitting_sets
}

fn minimal_splitting_sets_finder(
    consensus_clusters: Vec<NodeIdSet>,
    fbas: &Fbas,
    minimal_quorums: &[NodeIdSet],
) -> Vec<NodeIdSet> {
    let mut found_splitting_sets: Vec<NodeIdSet> = vec![];
    for (i, nodes) in consensus_clusters.into_iter().enumerate() {
        debug!("Finding minimal splitting sets in cluster {}...", i);

        if let Some(symmetric_cluster) = find_symmetric_cluster_in_consensus_cluster(&nodes, fbas) {
            debug!("Cluster contains a symmetric quorum cluster! Extracting splitting sets...");
            found_splitting_sets
                .append(&mut symmetric_cluster.to_minimal_splitting_sets(fbas, minimal_quorums));
        } else {
            debug!("Sorting nodes by rank...");
            let sorted_nodes = sort_by_rank(nodes.into_iter().collect(), fbas);
            debug!("Sorted.");

            let unprocessed = sorted_nodes;
            let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());
            let mut available = unprocessed.iter().cloned().collect();

            debug!("Collecting splitting_sets...");
            minimal_splitting_sets_finder_step(
                &mut unprocessed.into(),
                &mut selection,
                &mut available,
                &mut found_splitting_sets,
                fbas,
                minimal_quorums,
                true,
            );
        }
    }
    found_splitting_sets
}
fn minimal_splitting_sets_finder_step(
    unprocessed: &mut NodeIdDeque,
    selection: &mut NodeIdSet,
    available: &mut NodeIdSet,
    found_splitting_sets: &mut Vec<NodeIdSet>,
    fbas: &Fbas,
    minimal_quorums: &[NodeIdSet],
    selection_changed: bool,
) {
    if selection_changed && is_splitting_set(selection, fbas, minimal_quorums) {
        if is_minimal_for_splitting_set(selection, fbas, minimal_quorums) {
            found_splitting_sets.push(selection.clone());
            if found_splitting_sets.len() % 100_000 == 0 {
                debug!("...{} splitting_sets found", found_splitting_sets.len());
            }
        }
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        selection.insert(current_candidate);

        minimal_splitting_sets_finder_step(
            unprocessed,
            selection,
            available,
            found_splitting_sets,
            fbas,
            minimal_quorums,
            true,
        );

        selection.remove(current_candidate);
        available.remove(current_candidate);

        if selection_likely_splittable(selection, available, fbas) {
            minimal_splitting_sets_finder_step(
                unprocessed,
                selection,
                available,
                found_splitting_sets,
                fbas,
                minimal_quorums,
                false,
            );
        }
        unprocessed.push_front(current_candidate);
        available.insert(current_candidate);
    }
}

impl Node {
    fn is_slice_intersection(&self, node_set: &NodeIdSet) -> bool {
        self.quorum_set.is_slice_intersection(&node_set)
    }
}
impl QuorumSet {
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
    fn to_minimal_splitting_sets(
        &self,
        fbas: &Fbas,
        minimal_quorums: &[NodeIdSet],
    ) -> Vec<NodeIdSet> {
        let splitting_sets = self.to_splitting_sets();
        if self.contains_duplicates() {
            remove_non_minimal_x(
                splitting_sets,
                |ss, fbas| is_minimal_for_splitting_set(ss, fbas, minimal_quorums),
                fbas,
            )
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

fn is_splitting_set(selection: &NodeIdSet, fbas: &Fbas, minimal_quorums: &[NodeIdSet]) -> bool {
    if is_likely_splitting_set(selection, fbas) {
        is_definetely_splitting_set(selection, minimal_quorums)
    } else {
        false
    }
}

fn is_minimal_for_splitting_set(
    splitting_set: &NodeIdSet,
    fbas: &Fbas,
    minimal_quorums: &[NodeIdSet],
) -> bool {
    let mut tester = splitting_set.clone();

    for node_id in splitting_set.iter() {
        tester.remove(node_id);
        if is_splitting_set(&tester, fbas, minimal_quorums) {
            return false;
        }
        tester.insert(node_id);
    }
    true
}

/// Heuristic check that filters out many hopeless branches
fn selection_likely_splittable(selection: &NodeIdSet, available: &NodeIdSet, fbas: &Fbas) -> bool {
    selection.is_empty()
        || selection
            .iter()
            .all(|x| fbas.nodes[x].is_slice_intersection(available))
}

/// Heuristic check that filters out *almost all* false candidates
fn is_likely_splitting_set(selection: &NodeIdSet, fbas: &Fbas) -> bool {
    !selection.is_empty()
        && selection
            .iter()
            .all(|x| fbas.nodes[x].is_slice_intersection(selection))
}

/// Expensive check that filters out *all* false candidates
fn is_definetely_splitting_set(selection: &NodeIdSet, minimal_quorums: &[NodeIdSet]) -> bool {
    let mut tester = NodeIdSet::new();
    let mut relevant_quorums_it = minimal_quorums.iter().filter(|q| q.is_superset(selection));
    relevant_quorums_it.clone().any(|q1| {
        tester = q1.clone();
        tester.difference_with(selection);
        relevant_quorums_it.next();
        relevant_quorums_it.clone().any(|q2| tester.is_disjoint(q2))
    })
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
    fn find_minimal_splitting_sets_in_correct() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let minimal_quorums = bitsetvec![{0, 1}, {0, 10}, {1, 10}];

        let expected = vec![bitset![0], bitset![1], bitset![10]];
        let actual = find_minimal_splitting_sets(&fbas, &minimal_quorums);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_splitting_sets_in_different_consensus_clusters() {
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
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 2, "validators": ["n3", "n4", "n5"] }
            },
            {
                "publicKey": "n4",
                "quorumSet": { "threshold": 2, "validators": ["n3", "n4", "n5"] }
            },
            {
                "publicKey": "n5",
                "quorumSet": { "threshold": 2, "validators": ["n3", "n4", "n5"] }
            }
        ]"#,
        );
        let minimal_quorums = bitsetvec![{0, 1}, {0, 2}, {1, 2}, {3, 4}, {3, 5}, {4, 5}];
        assert_eq!(find_minimal_quorums(&fbas), minimal_quorums);

        // No quorum intersection => the FBAS is splitting even without faulty nodes. In these
        // cases `find_minimal_splitting_sets`, assumes that we have sub-FBASs and finds the
        // splitting sets in all of them.
        let actual = find_minimal_splitting_sets(&fbas, &minimal_quorums);
        let expected = bitsetvec![{ 0 }, { 1 }, { 2 }, { 3 }, { 4 }, { 5 }];

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

        assert!(is_likely_splitting_set(&bitset! {0, 1}, &fbas));
        assert!(!is_definetely_splitting_set(
            &bitset! {0, 1},
            &minimal_quorums
        ));

        // no two quorums => no way to lose quorum intersection
        let expected: Vec<NodeIdSet> = vec![];
        let actual = find_minimal_splitting_sets(&fbas, &minimal_quorums);

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
        let actual = find_minimal_splitting_sets(&fbas, &minimal_quorums);

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
        let actual = find_minimal_splitting_sets(&fbas, &minimal_quorums);

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
        let actual = find_minimal_splitting_sets(&fbas, &minimal_quorums);

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

        assert!(is_likely_splitting_set(&bitset! {0, 1, 2, 3}, &fbas));
        assert!(is_definetely_splitting_set(
            &bitset! {0, 1, 2, 3},
            &minimal_quorums
        ));

        let expected: Vec<NodeIdSet> = bitsetvec![{0, 1, 2, 3}];
        let actual = find_minimal_splitting_sets(&fbas, &minimal_quorums);
        assert_eq!(expected, actual);
    }
}
