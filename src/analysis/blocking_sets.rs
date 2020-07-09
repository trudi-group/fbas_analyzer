use super::*;
use itertools::Itertools;

pub fn find_minimal_blocking_sets(fbas: &Fbas) -> Vec<NodeIdSet> {
    info!("Starting to look for minimal blocking_sets...");
    let blocking_sets = find_sets(fbas, minimal_blocking_sets_finder);
    info!(
        "Found {} (not necessarily minimal) blocking_sets.",
        blocking_sets.len()
    );
    let minimal_blocking_sets = remove_non_minimal_blocking_sets(blocking_sets, fbas);
    info!(
        "Reduced to {} minimal blocking_sets.",
        minimal_blocking_sets.len()
    );
    minimal_blocking_sets
}

fn minimal_blocking_sets_finder(consensus_clusters: Vec<NodeIdSet>, fbas: &Fbas) -> Vec<NodeIdSet> {
    let mut found_blocking_sets_per_cluster: Vec<Vec<NodeIdSet>> = vec![];
    for (i, nodes) in consensus_clusters.into_iter().enumerate() {
        debug!("Finding minimal blocking_sets in cluster {}...", i);
        let mut found_blocking_sets: Vec<NodeIdSet> = vec![];

        debug!("Sorting nodes by rank...");
        let sorted_nodes = sort_by_rank(nodes.into_iter().collect(), fbas);
        debug!("Sorted.");

        let unprocessed = sorted_nodes;
        let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());

        // what remains after we take out `selection`
        let mut remaining: NodeIdSet = unprocessed.iter().copied().collect();

        // what remains after we take out `selection` + all `unprocessed`
        let mut max_remaining = NodeIdSet::with_capacity(fbas.nodes.len());

        debug!("Collecting blocking_sets...");
        minimal_blocking_sets_finder_step(
            &mut unprocessed.into(),
            &mut selection,
            &mut remaining,
            &mut max_remaining,
            &mut found_blocking_sets,
            fbas,
            true,
        );
        found_blocking_sets_per_cluster.push(found_blocking_sets);
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
    fbas: &Fbas,
    selection_changed: bool,
) {
    if selection_changed && !contains_quorum(remaining, fbas) {
        found_blocking_sets.push(selection.clone());
        if found_blocking_sets.len() % 100_000 == 0 {
            debug!("...{} blocking_sets found", found_blocking_sets.len());
        }
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        selection.insert(current_candidate);
        remaining.remove(current_candidate);

        minimal_blocking_sets_finder_step(
            unprocessed,
            selection,
            remaining,
            max_remaining,
            found_blocking_sets,
            fbas,
            true,
        );

        selection.remove(current_candidate);
        remaining.insert(current_candidate);
        max_remaining.insert(current_candidate);

        if !contains_quorum(max_remaining, &fbas) {
            minimal_blocking_sets_finder_step(
                unprocessed,
                selection,
                remaining,
                max_remaining,
                found_blocking_sets,
                fbas,
                false,
            );
        }
        unprocessed.push_front(current_candidate);
        max_remaining.remove(current_candidate);
    }
}

fn remove_non_minimal_blocking_sets(blocking_sets: Vec<NodeIdSet>, fbas: &Fbas) -> Vec<NodeIdSet> {
    let mut minimal_blocking_sets = vec![];
    let mut tester: NodeIdSet;
    let mut is_minimal;
    let all_nodes = fbas.all_nodes();

    debug!("Filtering non-minimal blocking_sets...");
    for (i, blocking_set) in blocking_sets.into_iter().enumerate() {
        if i % 100_000 == 0 {
            debug!(
                "...at blocking_set {}; {} minimal blocking_sets",
                i,
                minimal_blocking_sets.len()
            );
        }
        is_minimal = true;
        // whyever, using clone() here seems to be faster than clone_from()
        tester = all_nodes.clone();
        tester.difference_with(&blocking_set);

        for node_id in blocking_set.iter() {
            tester.insert(node_id);
            if !contains_quorum(&tester, fbas) {
                is_minimal = false;
                break;
            }
            tester.remove(node_id);
        }
        if is_minimal {
            minimal_blocking_sets.push(blocking_set);
        }
    }
    debug!("Filtering done.");
    debug_assert!(contains_only_minimal_node_sets(&minimal_blocking_sets));
    minimal_blocking_sets.sort();
    minimal_blocking_sets.sort_by_key(|x| x.len());
    minimal_blocking_sets
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
    fn find_minimal_blocking_sets_in_different_consensus_clusters() {
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
