use super::*;
use std::collections::BTreeMap;

/// Find all minimal quorums in the FBAS...
pub fn find_minimal_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    info!("Starting to look for minimal quorums...");
    let quorums = find_quorums(fbas, minimal_quorums_finder);
    info!("Found {} (not necessarily minimal) quorums.", quorums.len());
    let minimal_quorums = remove_non_minimal_quorums(quorums, fbas);
    info!("Reduced to {} minimal quorums.", minimal_quorums.len());
    minimal_quorums
}

/// Find at least two non-intersecting quorums. Use this function if it is very likely that
/// the FBAS lacks quorum intersection and you want to stop early in such cases.
pub fn find_nonintersecting_quorums(fbas: &Fbas) -> Option<Vec<NodeIdSet>> {
    info!("Starting to look for potentially non-intersecting quorums...");
    let quorums = find_quorums(fbas, nonintersecting_quorums_finder);
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
    let quorums = find_quorums(fbas, symmetric_clusters_finder);
    info!("Found {} different quorum clusters.", quorums.len());
    quorums
}

/// Does preprocessing common to all finders
fn find_quorums<F, R>(fbas: &Fbas, finder: F) -> Vec<R>
where
    F: Fn(Vec<NodeIdSet>, &Fbas) -> Vec<R>,
{
    let all_nodes: NodeIdSet = (0..fbas.nodes.len()).collect();

    debug!("Removing nodes not part of any quorum...");
    let (satisfiable, unsatisfiable) = find_unsatisfiable_nodes(&all_nodes, fbas);
    if !unsatisfiable.is_empty() {
        warn!(
            "The quorum sets of {} nodes are not satisfiable at all in the given FBAS!",
            unsatisfiable.len()
        );
        info!(
            "Ignoring {} unsatisfiable nodes ({} nodes left).",
            unsatisfiable.len(),
            satisfiable.len()
        );
    } else {
        debug!("All nodes are satisfiable");
    }

    debug!("Partitioning into strongly connected components...");
    let sccs = partition_into_strongly_connected_components(&satisfiable, fbas);

    debug!("Reducing to strongly connected components that contain quorums...");
    let consensus_clusters: Vec<NodeIdSet> = sccs
        .into_iter()
        .filter(|node_set| contains_quorum(&node_set, fbas))
        .collect();
    if consensus_clusters.len() > 1 {
        warn!(
            "{} connected components contain quorums => the FBAS lacks quorum intersection!",
            consensus_clusters.len()
        );
    }
    finder(consensus_clusters, fbas)
}

fn minimal_quorums_finder(consensus_clusters: Vec<NodeIdSet>, fbas: &Fbas) -> Vec<NodeIdSet> {
    let mut found_quorums_in_all_clusters = vec![];
    for (i, nodes) in consensus_clusters.into_iter().enumerate() {
        debug!("Finding minimal quorums in cluster {}...", i);
        let mut found_quorums: Vec<NodeIdSet> = vec![];

        let quorum_clusters = find_symmetric_clusters_in_node_set(&nodes, fbas);
        if !quorum_clusters.is_empty() {
            assert!(quorum_clusters.len() == 1);
            debug!("Cluster contains a symmetric quorum cluster! Extracting quorums...");
            let quorum_cluster = quorum_clusters.into_iter().next().unwrap();
            {
                let mut remaining_nodes = nodes.clone();
                remaining_nodes.difference_with(&quorum_cluster.contained_nodes());
                assert!(!contains_quorum(&remaining_nodes, fbas));
            }
            found_quorums.extend_from_slice(&quorum_cluster.to_quorum_slices());
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
        found_quorums_in_all_clusters.append(&mut found_quorums);
    }
    found_quorums_in_all_clusters
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
        found_quorums.push(selection.clone());
        if found_quorums.len() % 100_000 == 0 {
            debug!("...{} quorums found", found_quorums.len());
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

fn contains_quorum(node_set: &NodeIdSet, fbas: &Fbas) -> bool {
    let mut satisfiable = node_set.clone();

    while let Some(unsatisfiable_node) = satisfiable
        .iter()
        .find(|&x| !fbas.nodes[x].is_quorum_slice(&satisfiable))
    {
        satisfiable.remove(unsatisfiable_node);
    }
    !satisfiable.is_empty()
}

fn remove_non_minimal_quorums(quorums: Vec<NodeIdSet>, fbas: &Fbas) -> Vec<NodeIdSet> {
    let mut minimal_quorums = vec![];
    let mut tester: NodeIdSet;
    let mut is_minimal;

    debug!("Filtering non-minimal quorums...");
    for (i, quorum) in quorums.into_iter().enumerate() {
        if i % 100_000 == 0 {
            debug!(
                "...at quorum {}; {} minimal quorums",
                i,
                minimal_quorums.len()
            );
        }
        is_minimal = true;
        // whyever, using clone() here seems to be faster than clone_from()
        tester = quorum.clone();

        for node_id in quorum.iter() {
            tester.remove(node_id);
            if contains_quorum(&tester, fbas) {
                is_minimal = false;
                break;
            }
            tester.insert(node_id);
        }
        if is_minimal {
            minimal_quorums.push(quorum);
        }
    }
    debug!("Filtering done.");
    debug_assert!(contains_only_minimal_node_sets(&minimal_quorums));
    minimal_quorums.sort();
    minimal_quorums.sort_by_key(|x| x.len());
    minimal_quorums
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

    #[test]
    fn unsatisfiable_nodes_not_returned_as_strongly_connected() {
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
        let (satisfiable, _) = find_unsatisfiable_nodes(&all_nodes, &fbas);
        let (strongly_connected, _) = reduce_to_strongly_connected_nodes(satisfiable, &fbas);

        assert!(strongly_connected.contains(0));
        assert!(strongly_connected.contains(1));
        assert!(!strongly_connected.contains(directly_unsatisfiable));
        assert!(!strongly_connected.contains(transitively_unsatisfiable));
    }

    #[test]
    fn reduce_to_strongly_connected_nodes_ignores_self_links() {
        let mut fbas = Fbas::new();
        let interconnected_qset = QuorumSet {
            validators: vec![0, 1],
            inner_quorum_sets: vec![],
            threshold: 2,
        };
        let self_connected_qset = QuorumSet {
            validators: vec![2],
            inner_quorum_sets: vec![],
            threshold: 1,
        };
        fbas.add_generic_node(interconnected_qset.clone());
        fbas.add_generic_node(interconnected_qset);
        fbas.add_generic_node(self_connected_qset);
        let (strongly_connected, not_strongly_connected) =
            reduce_to_strongly_connected_nodes(bitset![0, 1, 2], &fbas);
        assert_eq!(bitset![0, 1], strongly_connected);
        assert_eq!(bitset![2], not_strongly_connected);
    }
}
