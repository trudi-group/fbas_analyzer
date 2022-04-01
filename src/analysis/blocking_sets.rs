use super::*;

use itertools::Itertools;

/// Find all minimal blocking sets in the FBAS.
pub fn find_minimal_blocking_sets(fbas: &Fbas) -> Vec<NodeIdSet> {
    info!("Starting to look for minimal blocking_sets...");
    let minimal_blocking_sets = find_minimal_sets(fbas, minimal_blocking_sets_finder);
    info!(
        "Found {} minimal blocking_sets.",
        minimal_blocking_sets.len()
    );
    minimal_blocking_sets
}

fn minimal_blocking_sets_finder(consensus_clusters: Vec<NodeIdSet>, fbas: &Fbas) -> Vec<NodeIdSet> {
    let mut found_blocking_sets_per_cluster: Vec<Vec<NodeIdSet>> = vec![];
    for (i, nodes) in consensus_clusters.into_iter().enumerate() {
        debug!("Finding minimal blocking sets in cluster {}...", i);

        if let Some(symmetric_cluster) =
            is_symmetric_cluster(&nodes, &fbas.with_standard_form_quorum_sets())
        {
            debug!("Cluster contains a symmetric quorum cluster! Extracting blocking sets...");
            found_blocking_sets_per_cluster.push(symmetric_cluster.to_minimal_blocking_sets(fbas));
        } else {
            debug!("Sorting nodes by rank...");
            let sorted_nodes = sort_by_rank(nodes.into_iter().collect(), fbas);
            debug!("Sorted.");

            let mut found_unexpanded_blocking_sets_in_this_cluster: Vec<NodeIdSet> = vec![];

            let unprocessed = sorted_nodes;
            let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());

            // what remains after we take out `selection`
            let mut remaining: NodeIdSet = unprocessed.iter().copied().collect();

            // what remains after we take out `selection` + all `unprocessed`
            let mut max_remaining = NodeIdSet::with_capacity(fbas.nodes.len());

            let symmetric_nodes = find_symmetric_nodes_in_node_set(&remaining, fbas);

            debug!("Collecting blocking_sets...");
            minimal_blocking_sets_finder_step(
                &mut unprocessed.into(),
                &mut selection,
                &mut remaining,
                &mut max_remaining,
                &mut found_unexpanded_blocking_sets_in_this_cluster,
                &FbasValues {
                    fbas,
                    symmetric_nodes: &symmetric_nodes,
                },
                true,
            );
            let found_blocking_sets =
                symmetric_nodes.expand_sets(found_unexpanded_blocking_sets_in_this_cluster);
            found_blocking_sets_per_cluster.push(found_blocking_sets);
        }
    }
    found_blocking_sets_per_cluster
        .into_iter()
        .map(|blocking_sets_group| blocking_sets_group.into_iter())
        .multi_cartesian_product()
        .map(|blocking_set_combinations| {
            let mut combined_blocking_set = bitset![];
            for blocking_set in blocking_set_combinations.into_iter() {
                combined_blocking_set.union_with(&blocking_set);
            }
            combined_blocking_set
        })
        .collect()
}
fn minimal_blocking_sets_finder_step(
    unprocessed: &mut NodeIdDeque,
    selection: &mut NodeIdSet,
    remaining: &mut NodeIdSet,
    max_remaining: &mut NodeIdSet,
    found_blocking_sets: &mut Vec<NodeIdSet>,
    fbas_values: &FbasValues,
    selection_changed: bool,
) {
    if selection_changed && is_blocked_set(remaining, fbas_values.fbas) {
        if is_minimal_for_blocking_set_with_precomputed_blocked_set(
            selection,
            remaining,
            fbas_values.fbas,
        ) {
            found_blocking_sets.push(selection.clone());
            if found_blocking_sets.len() % 100_000 == 0 {
                debug!("...{} blocking_sets found", found_blocking_sets.len());
            }
        }
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        // We require that symmetric nodes are used in a fixed order; this way we can omit
        // redundant branches (we expand all combinations of symmetric nodes in the final result
        // sets).
        if fbas_values
            .symmetric_nodes
            .is_non_redundant_next(current_candidate, selection)
        {
            selection.insert(current_candidate);
            remaining.remove(current_candidate);

            minimal_blocking_sets_finder_step(
                unprocessed,
                selection,
                remaining,
                max_remaining,
                found_blocking_sets,
                fbas_values,
                true,
            );

            selection.remove(current_candidate);
            remaining.insert(current_candidate);
        }
        max_remaining.insert(current_candidate);

        if is_blocked_set(max_remaining, fbas_values.fbas) {
            minimal_blocking_sets_finder_step(
                unprocessed,
                selection,
                remaining,
                max_remaining,
                found_blocking_sets,
                fbas_values,
                false,
            );
        }
        unprocessed.push_front(current_candidate);
        max_remaining.remove(current_candidate);
    }
}

// This exists because clippy complained that we have too many arguments.
#[derive(Debug, Clone)]
struct FbasValues<'a> {
    fbas: &'a Fbas,
    symmetric_nodes: &'a SymmetricNodesMap,
}

impl QuorumSet {
    /// If `self` represents a symmetric quorum cluster, this function returns all minimal blocking sets of the induced FBAS.
    fn to_minimal_blocking_sets(&self, fbas: &Fbas) -> Vec<NodeIdSet> {
        let blocking_sets = self.to_blocking_sets();
        if self.contains_duplicates() {
            remove_non_minimal_x(blocking_sets, is_minimal_for_blocking_set, fbas)
        } else {
            blocking_sets
        }
    }
    /// If `self` represents a symmetric quorum cluster, this function returns all minimal blocking sets of the induced FBAS,
    /// but perhaps also a few extra...
    fn to_blocking_sets(&self) -> Vec<NodeIdSet> {
        self.to_slices(|qset| qset.blocking_threshold())
    }
    fn blocking_threshold(&self) -> usize {
        (self.validators.len() + self.inner_quorum_sets.len() + 1).wrapping_sub(self.threshold)
    }
}

fn is_minimal_for_blocking_set(blocking_set: &NodeIdSet, fbas: &Fbas) -> bool {
    let mut blocked_set = fbas.all_nodes();
    blocked_set.difference_with(blocking_set);
    is_minimal_for_blocking_set_with_precomputed_blocked_set(blocking_set, blocking_set, fbas)
}
fn is_minimal_for_blocking_set_with_precomputed_blocked_set(
    blocking_set: &NodeIdSet,
    blocked_set: &NodeIdSet,
    fbas: &Fbas,
) -> bool {
    let mut tester = blocked_set.clone();

    for node_id in blocking_set.iter() {
        tester.insert(node_id);
        if is_blocked_set(&tester, fbas) {
            return false;
        }
        tester.remove(node_id);
    }
    true
}

fn is_blocked_set(nodes: &NodeIdSet, fbas: &Fbas) -> bool {
    !contains_quorum(nodes, fbas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn find_minimal_blocking_sets_in_correct() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));

        let expected = vec![bitset![0, 1], bitset![0, 10], bitset![1, 10]];
        let actual = find_minimal_blocking_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_blocking_sets_in_broken_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));

        let expected = vec![bitset![0, 1], bitset![0, 2]];
        let actual = find_minimal_blocking_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn flat_2_of_3_quorum_set_to_blocking_sets() {
        let qset = QuorumSet {
            threshold: 2,
            validators: vec![0, 1, 2],
            inner_quorum_sets: vec![],
        };
        let expected = bitsetvec![{0, 1}, {0, 2}, {1, 2}];
        let actual = qset.to_blocking_sets();

        assert_eq!(expected, actual);
    }

    #[test]
    fn unsatisfiable_quorum_set_to_blocking_sets() {
        let quorum_set = QuorumSet::new_unsatisfiable();
        let expected = bitsetvec![{}];
        let actual = quorum_set.to_blocking_sets();
        assert_eq!(expected, actual);
    }

    #[test]
    fn empty_quorum_set_to_blocking_sets() {
        let quorum_set = QuorumSet::new_empty();
        let expected: Vec<NodeIdSet> = bitsetvec![];
        let actual = quorum_set.to_blocking_sets();
        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_blocking_sets_in_symmetric_consensus_cluster() {
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
        let expected = bitsetvec![{ 0 }, { 1 }];
        let actual = find_minimal_blocking_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_blocking_sets_in_different_consensus_clusters() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 1, "validators": ["n1"] }
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
        let expected = vec![bitset![0, 2], bitset![0, 3], bitset![1, 2], bitset![1, 3]];
        let actual = find_minimal_blocking_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_blocking_sets_in_different_symmetric_consensus_clusters() {
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
        let expected = vec![bitset![0, 2], bitset![0, 3], bitset![1, 2], bitset![1, 3]];
        let actual = find_minimal_blocking_sets(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    #[ignore]
    fn minimal_blocking_sets_more_minimal_than_minimal_quorums() {
        let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let fbas = fbas.to_standard_form();
        let minimal_quorums = find_minimal_quorums(&fbas);
        let minimal_blocking_sets = find_minimal_blocking_sets(&fbas);

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
