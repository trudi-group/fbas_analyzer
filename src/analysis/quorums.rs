use super::*;
use std::collections::BTreeMap;

/// Find all minimal quorums in the FBAS...
pub fn find_minimal_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    info!("Starting to look for minimal quorums...");
    let minimal_quorums = find_minimal_sets(fbas, minimal_quorums_finder);
    info!("Found {} minimal quorums.", minimal_quorums.len());
    minimal_quorums
}

/// Find at least two non-intersecting quorums. Use this function if it is very likely that
/// the FBAS lacks quorum intersection and you want to stop early in such cases.
pub fn find_nonintersecting_quorums(fbas: &Fbas) -> Option<Vec<NodeIdSet>> {
    info!("Starting to look for potentially non-intersecting quorums...");
    let quorums = find_sets(fbas, nonintersecting_quorums_finder);
    if quorums.len() < 2 {
        info!("Found no non-intersecting quorums.");
        None
    } else {
        warn!(
            "Found {} non-intersecting quorums (there could more).",
            quorums.len()
        );
        Some(quorums)
    }
}

/// Finds groups of nodes (represented as quorum sets) such that all members of the same group have
/// the exact identical quorum set, and the nodes contained in this quorum set are exactly the
/// group of nodes (a symmetric cluster). Once no more such clusters are found, returns the maximum
/// quorum of the remaining nodes. (So, getting a result with more than 1 entry implies that we
/// don't have quorum intersection.)
pub fn find_symmetric_clusters(fbas: &Fbas) -> Vec<QuorumSet> {
    info!("Starting to look for symmetric quorum clusters...");
    let quorums = find_sets(fbas, symmetric_clusters_finder);
    info!("Found {} different quorum clusters.", quorums.len());
    quorums
}

fn minimal_quorums_finder(consensus_clusters: Vec<NodeIdSet>, fbas: &Fbas) -> Vec<NodeIdSet> {
    let mut found_quorums: Vec<NodeIdSet> = vec![];
    for (i, nodes) in consensus_clusters.into_iter().enumerate() {
        debug!("Finding minimal quorums in cluster {}...", i);

        let symmetric_clusters = find_symmetric_clusters_in_node_set(&nodes, fbas);
        if !symmetric_clusters.is_empty() {
            assert!(symmetric_clusters.len() == 1);
            debug!("Cluster contains a symmetric quorum cluster! Extracting quorums...");
            let symmetric_cluster = symmetric_clusters.into_iter().next().unwrap();
            {
                let mut remaining_nodes = nodes.clone();
                remaining_nodes.difference_with(&symmetric_cluster.contained_nodes());
                assert!(!contains_quorum(&remaining_nodes, fbas));
            }
            let mut quorums = symmetric_cluster.to_quorum_slices();
            if symmetric_cluster.contains_duplicates() {
                quorums = remove_non_minimal_x(quorums, is_minimal_for_quorum, fbas)
            }
            found_quorums.append(&mut quorums);
        } else {
            debug!("Sorting nodes by rank...");
            let sorted_nodes = sort_by_rank(nodes.into_iter().collect(), fbas);
            debug!("Sorted.");

            let unprocessed = sorted_nodes;
            let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());
            let mut available = unprocessed.iter().cloned().collect();

            debug!("Collecting quorums...");
            minimal_quorums_finder_step(
                &mut unprocessed.into(),
                &mut selection,
                &mut available,
                &mut found_quorums,
                fbas,
                true,
            );
        }
    }
    found_quorums
}
fn minimal_quorums_finder_step(
    unprocessed: &mut NodeIdDeque,
    selection: &mut NodeIdSet,
    available: &mut NodeIdSet,
    found_quorums: &mut Vec<NodeIdSet>,
    fbas: &Fbas,
    selection_changed: bool,
) {
    if selection_changed && fbas.is_quorum(selection) {
        if is_minimal_for_quorum(&selection, fbas) {
            found_quorums.push(selection.clone());
            if found_quorums.len() % 100_000 == 0 {
                debug!("...{} quorums found", found_quorums.len());
            }
        }
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        selection.insert(current_candidate);

        minimal_quorums_finder_step(unprocessed, selection, available, found_quorums, fbas, true);

        selection.remove(current_candidate);
        available.remove(current_candidate);

        if selection_satisfiable(selection, available, fbas) {
            minimal_quorums_finder_step(
                unprocessed,
                selection,
                available,
                found_quorums,
                fbas,
                false,
            );
        }
        unprocessed.push_front(current_candidate);
        available.insert(current_candidate);
    }
}

fn nonintersecting_quorums_finder(
    consensus_clusters: Vec<NodeIdSet>,
    fbas: &Fbas,
) -> Vec<NodeIdSet> {
    if consensus_clusters.len() > 1 {
        debug!("More than one consensus clusters - reducing to maximal quorums.");
        consensus_clusters
            .into_iter()
            .map(|node_set| find_unsatisfiable_nodes(&node_set, fbas).0)
            .collect()
    } else {
        warn!("There is only one consensus cluster - there might be no non-intersecting quorums and the subsequent search might be slow.");
        let nodes = consensus_clusters.into_iter().next().unwrap_or_default();
        debug!("Sorting nodes by rank...");
        let sorted_nodes = sort_by_rank(nodes.into_iter().collect(), fbas);
        debug!("Sorted.");

        let unprocessed = sorted_nodes;
        let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());
        let mut available: NodeIdSet = unprocessed.iter().cloned().collect();
        let mut antiselection = available.clone();
        if let Some(intersecting_quorums) = nonintersecting_quorums_finder_step(
            &mut unprocessed.into(),
            &mut selection,
            &mut available,
            &mut antiselection,
            fbas,
        ) {
            assert!(intersecting_quorums.iter().all(|x| fbas.is_quorum(x)));
            assert!(intersecting_quorums[0].is_disjoint(&intersecting_quorums[1]));
            intersecting_quorums.to_vec()
        } else {
            assert!(fbas.is_quorum(&available));
            vec![available.clone()]
        }
    }
}
fn nonintersecting_quorums_finder_step(
    unprocessed: &mut NodeIdDeque,
    selection: &mut NodeIdSet,
    available: &mut NodeIdSet,
    antiselection: &mut NodeIdSet,
    fbas: &Fbas,
) -> Option<[NodeIdSet; 2]> {
    debug_assert!(selection.is_disjoint(&antiselection));
    if fbas.is_quorum(selection) {
        let (potential_complement, _) = find_unsatisfiable_nodes(&antiselection, fbas);

        if !potential_complement.is_empty() {
            return Some([selection.clone(), potential_complement]);
        }
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        selection.insert(current_candidate);
        antiselection.remove(current_candidate);
        if let Some(intersecting_quorums) = nonintersecting_quorums_finder_step(
            unprocessed,
            selection,
            available,
            antiselection,
            fbas,
        ) {
            return Some(intersecting_quorums);
        }
        selection.remove(current_candidate);
        antiselection.insert(current_candidate);
        available.remove(current_candidate);

        if selection_satisfiable(selection, available, fbas) {
            if let Some(intersecting_quorums) = nonintersecting_quorums_finder_step(
                unprocessed,
                selection,
                available,
                antiselection,
                fbas,
            ) {
                return Some(intersecting_quorums);
            }
        }
        unprocessed.push_front(current_candidate);
        available.insert(current_candidate);
    }
    None
}

fn symmetric_clusters_finder(consensus_clusters: Vec<NodeIdSet>, fbas: &Fbas) -> Vec<QuorumSet> {
    let mut found_clusters_in_all_clusters = vec![];
    for (i, nodes) in consensus_clusters.into_iter().enumerate() {
        debug!("Finding symmetric quorum cluster in cluster {}...", i);
        found_clusters_in_all_clusters
            .append(&mut find_symmetric_clusters_in_node_set(&nodes, fbas));
    }
    found_clusters_in_all_clusters
}
fn find_symmetric_clusters_in_node_set(nodes: &NodeIdSet, fbas: &Fbas) -> Vec<QuorumSet> {
    // qset -> (#occurances, goal #occurances)
    let mut qset_occurances: BTreeMap<QuorumSet, (usize, usize)> = BTreeMap::new();
    let mut found_clusters = vec![];

    for node_id in nodes.iter() {
        let qset = &fbas.nodes[node_id].quorum_set;
        let (count, goal) = if let Some((counter, goal)) = qset_occurances.get_mut(qset) {
            *counter += 1;
            (*counter, *goal)
        } else {
            let goal = qset.contained_nodes().len();
            qset_occurances.insert(qset.clone(), (1, goal));
            (1, goal)
        };
        if count == goal {
            found_clusters.push(qset.clone());
        }
    }
    found_clusters
}

fn selection_satisfiable(selection: &NodeIdSet, available: &NodeIdSet, fbas: &Fbas) -> bool {
    selection
        .iter()
        .all(|x| fbas.nodes[x].is_quorum_slice(available))
}

pub(crate) fn contains_quorum(node_set: &NodeIdSet, fbas: &Fbas) -> bool {
    let mut satisfiable = node_set.clone();

    while let Some(unsatisfiable_node) = satisfiable
        .iter()
        .find(|&x| !fbas.nodes[x].is_quorum_slice(&satisfiable))
    {
        satisfiable.remove(unsatisfiable_node);
    }
    !satisfiable.is_empty()
}

fn is_minimal_for_quorum(quorum: &NodeIdSet, fbas: &Fbas) -> bool {
    let mut tester = quorum.clone();

    for node_id in quorum.iter() {
        tester.remove(node_id);
        if contains_quorum(&tester, fbas) {
            return false;
        }
        tester.insert(node_id);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn find_minimal_quorums_in_correct_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        let expected = vec![bitset![0, 1], bitset![0, 2], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_quorums_in_broken_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));

        let expected = vec![bitset![0], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_quorums_in_broken_trivial_reversed_node_ids() {
        let mut fbas = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));
        fbas.nodes.reverse();

        let expected = vec![bitset![2], bitset![0, 1]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_quorums_when_naive_remove_non_minimal_optimization_doesnt_work() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n3"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n3"] }
            }
        ]"#,
        );
        let expected = vec![bitset![0, 3], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn find_nonintersecting_quorums_in_broken() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken.json"));

        let expected = Some(vec![bitset![3, 10], bitset![4, 6]]);
        let actual = find_nonintersecting_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_symmetric_cluster_in_correct_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        let expected = vec![QuorumSet {
            validators: vec![0, 1, 2],
            threshold: 2,
            inner_quorum_sets: vec![],
        }];
        let actual = find_symmetric_clusters(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_unsatisfiable_nodes_in_unconfigured_fbas() {
        let fbas = Fbas::new_generic_unconfigured(10);
        let all_nodes: NodeIdSet = (0..10).collect();

        let actual = find_unsatisfiable_nodes(&all_nodes, &fbas);
        let expected = (bitset![], all_nodes);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_transitively_unsatisfiable_nodes() {
        let mut fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        let directly_unsatisfiable = fbas.add_generic_node(QuorumSet::new());
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
        let (_, unsatisfiable) = find_unsatisfiable_nodes(&all_nodes, &fbas);

        assert!(unsatisfiable.contains(directly_unsatisfiable));
        assert!(unsatisfiable.contains(transitively_unsatisfiable));
    }
}
