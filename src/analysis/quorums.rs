use super::*;
use std::collections::BTreeMap;

/// Find all minimal quorums in the FBAS...
pub fn find_minimal_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    info!("Starting to look for minimal quorums...");
    let quorums = find_quorums(fbas, true, find_minimal_quorums_worker);
    info!("Found {} (not necessarily minimal) quorums.", quorums.len());
    let minimal_quorums = remove_non_minimal_quorums(quorums, fbas);
    info!("Reduced to {} minimal quorums.", minimal_quorums.len());
    minimal_quorums
}

/// Similar to `find_minimal_quorums`, but aggressively searches for non-intersecting complement
/// quorums to each found quorum and stops once such a quorum is found. Returns either two
/// non-intersecting quorums or one very big quorum. Use this function if it is very likely that
/// the FBAS lacks quorum intersection and you want to stop early in such cases.
pub fn find_nonintersecting_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    info!("Starting to look for potentially non-intersecting quorums...");
    let quorums = find_quorums(fbas, true, find_nonintersecting_quorums_worker);
    if quorums.len() < 2 {
        info!("Found no non-intersecting quorums.");
    } else {
        warn!("Found two non-intersecting quorums!");
    }
    quorums
}

/// Finds groups of nodes (represented as quorum sets) such that all members of the same group have
/// the exact identical quorum set, and the nodes contained in this quorum set are exactly the
/// group of nodes (a symmetric cluster). Once no more such clusters are found, returns the maximum
/// quorum of the remaining nodes. (So, getting a result with more than 1 entry implies that we
/// don't have quorum intersection.)
pub fn find_symmetric_quorum_clusters(fbas: &Fbas) -> Vec<QuorumSet> {
    info!("Starting to look for symmetric quorum clusters...");
    let quorums = find_quorums(fbas, false, find_symmetric_quorum_clusters_worker);
    info!("Found {} different quorum clusters.", quorums.len());
    quorums
}

fn find_quorums<F, R>(fbas: &Fbas, sort: bool, worker: F) -> Vec<R>
where
    F: Fn(Vec<NodeId>, &Fbas) -> Vec<R>,
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

    debug!("Reducing to strongly connected components...");
    let (strongly_connected, not_strongly_connected) =
        reduce_to_strongly_connected_components(satisfiable, fbas);
    info!(
        "Ignoring {} not strongly connected nodes ({} nodes left).",
        not_strongly_connected.len(),
        strongly_connected.len(),
    );

    let mut nodes = strongly_connected.into_iter().collect();
    if sort {
        debug!("Sorting nodes by rank...");
        nodes = sort_by_rank(nodes, fbas);
        debug!("Sorted.");
    }

    debug!("Collecting quorums...");
    worker(nodes, fbas)
}

fn find_minimal_quorums_worker(sorted_nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeIdSet> {
    let unprocessed = sorted_nodes;
    let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());
    let mut available = unprocessed.iter().cloned().collect();
    let mut found_quorums: Vec<NodeIdSet> = vec![];

    find_minimal_quorums_step(
        &mut unprocessed.into(),
        &mut selection,
        &mut available,
        &mut found_quorums,
        fbas,
    );
    found_quorums
}
fn find_minimal_quorums_step(
    unprocessed: &mut NodeIdDeque,
    selection: &mut NodeIdSet,
    available: &mut NodeIdSet,
    found_quorums: &mut Vec<NodeIdSet>,
    fbas: &Fbas,
) {
    if fbas.is_quorum(selection) {
        found_quorums.push(selection.clone());
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        selection.insert(current_candidate);

        find_minimal_quorums_step(unprocessed, selection, available, found_quorums, fbas);

        selection.remove(current_candidate);
        available.remove(current_candidate);

        if quorums_possible(selection, available, fbas) {
            find_minimal_quorums_step(unprocessed, selection, available, found_quorums, fbas);
        }
        unprocessed.push_front(current_candidate);
        available.insert(current_candidate);
    }
}

fn find_nonintersecting_quorums_worker(sorted_nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeIdSet> {
    let unprocessed = sorted_nodes;
    let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());
    let mut available: NodeIdSet = unprocessed.iter().cloned().collect();
    let mut antiselection = available.clone();
    if let Some(intersecting_quorums) = find_nonintersecting_quorums_step(
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
fn find_nonintersecting_quorums_step(
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
        if let Some(intersecting_quorums) = find_nonintersecting_quorums_step(
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

        if quorums_possible(selection, available, fbas) {
            if let Some(intersecting_quorums) = find_nonintersecting_quorums_step(
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

fn find_symmetric_quorum_clusters_worker(nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<QuorumSet> {
    // qset -> (#occurances, goal #occurances)
    let mut qset_occurances: BTreeMap<QuorumSet, (usize, usize)> = BTreeMap::new();

    for &node_id in nodes.iter() {
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
            let mut found_clusters = vec![qset.clone()];
            let qset_nodes = qset.contained_nodes();
            let remaining_nodes = nodes
                .iter()
                .copied()
                .filter(|&i| !qset_nodes.contains(i))
                .collect();
            let (remaining_satisfiable_nodes, _) = find_unsatisfiable_nodes(&remaining_nodes, fbas);
            if !remaining_satisfiable_nodes.is_empty() {
                found_clusters.append(&mut find_symmetric_quorum_clusters_worker(
                    remaining_satisfiable_nodes.into_iter().collect(),
                    fbas,
                ));
            }
            return found_clusters;
        }
    }
    // no cluster found
    assert!(fbas.is_quorum(&nodes.iter().copied().collect()));
    let mut validators = nodes;
    validators.sort();
    vec![QuorumSet {
        threshold: validators.len(),
        validators,
        inner_quorum_sets: vec![],
    }]
}

fn quorums_possible(selection: &NodeIdSet, available: &NodeIdSet, fbas: &Fbas) -> bool {
    selection
        .iter()
        .all(|x| fbas.nodes[x].is_quorum_slice(available))
}

pub fn find_unsatisfiable_nodes(nodes: &NodeIdSet, fbas: &Fbas) -> (NodeIdSet, NodeIdSet) {
    let (mut satisfiable, mut unsatisfiable) = (bitset![], bitset![]);
    for node_id in nodes.iter() {
        if fbas.nodes[node_id].is_quorum_slice(&nodes) {
            satisfiable.insert(node_id);
        } else {
            unsatisfiable.insert(node_id);
        }
    }
    if !unsatisfiable.is_empty() {
        // because more things might have changed now that we can't use some nodes
        let (new_satisfiable, new_unsatisfiable) = find_unsatisfiable_nodes(&satisfiable, fbas);
        unsatisfiable.union_with(&new_unsatisfiable);
        satisfiable = new_satisfiable;
    }
    (satisfiable, unsatisfiable)
}

pub(crate) fn reduce_to_strongly_connected_components(
    mut nodes: NodeIdSet,
    fbas: &Fbas,
) -> (NodeIdSet, NodeIdSet) {
    // can probably be done faster, all of this
    let mut removed_nodes = nodes.clone();
    for node_id in nodes.iter() {
        let node = &fbas.nodes[node_id];
        for included_node in node.quorum_set.contained_nodes().into_iter() {
            if included_node == node_id {
                continue;
            }
            removed_nodes.remove(included_node);
        }
    }
    if !removed_nodes.is_empty() {
        nodes.difference_with(&removed_nodes);
        let (reduced_nodes, new_removed_nodes) =
            reduce_to_strongly_connected_components(nodes, fbas);
        nodes = reduced_nodes;
        removed_nodes.union_with(&new_removed_nodes);
    }
    (nodes, removed_nodes)
}

fn remove_non_minimal_quorums(mut quorums: Vec<NodeIdSet>, fbas: &Fbas) -> Vec<NodeIdSet> {
    debug!("Removing duplicates...");
    let len_before = quorums.len();
    quorums.sort();
    quorums.dedup();
    debug!("Done; removed {} duplicates.", len_before - quorums.len());

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
    minimal_quorums.sort_by_key(|x| x.len());
    minimal_quorums
}

fn contains_quorum(node_set: &NodeIdSet, fbas: &Fbas) -> bool {
    !find_unsatisfiable_nodes(&node_set, fbas).0.is_empty()
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
    fn find_nonintersecting_quorums_in_broken() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken.json"));

        let expected = vec![bitset![3, 10], bitset![4, 6]];
        let actual = find_nonintersecting_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_symmetric_quorum_cluster_in_correct_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        let expected = vec![QuorumSet {
            validators: vec![0, 1, 2],
            threshold: 2,
            inner_quorum_sets: vec![],
        }];
        let actual = find_symmetric_quorum_clusters(&fbas);

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
    fn unsatisfiable_nodes_dont_end_up_in_strongly_connected_components() {
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
        let (strongly_connected, _) = reduce_to_strongly_connected_components(satisfiable, &fbas);

        assert!(strongly_connected.contains(0));
        assert!(strongly_connected.contains(1));
        assert!(!strongly_connected.contains(directly_unsatisfiable));
        assert!(!strongly_connected.contains(transitively_unsatisfiable));
    }

    #[test]
    fn reduce_to_strongly_connected_components_ignores_self_links() {
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
            reduce_to_strongly_connected_components(bitset![0, 1, 2], &fbas);
        assert_eq!(bitset![0, 1], strongly_connected);
        assert_eq!(bitset![2], not_strongly_connected);
    }
}
