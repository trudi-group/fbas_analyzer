use super::*;

/// Find all minimal quorums in the FBAS...
pub fn find_minimal_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    info!("Starting to look for minimal quorums...");
    let quorums = find_quorums(fbas, find_minimal_quorums_step);
    info!("Found {} (not necessarily minimal) quorums.", quorums.len());
    let minimal_quorums = remove_non_minimal_node_sets(quorums);
    info!("Reduced to {} minimal quorums.", minimal_quorums.len());
    minimal_quorums
}

/// Similar to `find_minimal_quorums`, but aggressively searches for non-intersecting complement
/// quorums to each found quorum and stops once such a quorum is found. Returns either two
/// non-intersecting quorums or all minimal quorums (like `find_minimal_quorums`). Use this
/// function if it is likely that the FBAS lacks quorum intersection and you want to stop early in
/// such cases.
pub fn find_nonintersecting_or_minimal_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    info!("Starting to look for potentially non-intersecting quorums...");
    let quorums = find_quorums(fbas, find_nonintersecting_quorums_step_wrapper);
    if all_intersect(&quorums) {
        info!(
            "Found no non-intersecting quorums out of {} found quorums.",
            quorums.len()
        );
        let minimal_quorums = remove_non_minimal_node_sets(quorums);
        info!("Reduced to {} minimal quorums.", minimal_quorums.len());
        minimal_quorums
    } else {
        warn!("Found two non-intersecting quorums!");
        quorums
    }
}

fn find_quorums<F>(fbas: &Fbas, step: F) -> Vec<NodeIdSet>
where
    F: Fn(&mut NodeIdDeque, &mut NodeIdSet, &mut NodeIdSet, &mut Vec<NodeIdSet>, &Fbas),
{
    let all_nodes: NodeIdSet = (0..fbas.nodes.len()).collect();

    debug!("Removing nodes not part of any quorum...");
    let (satisfiable, unsatisfiable) = find_unsatisfiable_nodes(&all_nodes, fbas);
    if !unsatisfiable.is_empty() {
        warn!(
            "The quorum sets of nodes {:?} are not satisfiable at all in the given FBAS!",
            unsatisfiable
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

    debug!("Sorting nodes by rank...");
    let sorted = sort_by_rank(strongly_connected.into_iter().collect(), fbas);
    debug!("Sorted.");

    let unprocessed = sorted;
    let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());
    let mut available = unprocessed.iter().cloned().collect();
    let mut found_quorums: Vec<NodeIdSet> = vec![];

    debug!("Collecting quorums...");
    step(
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

fn find_nonintersecting_quorums_step_wrapper(
    unprocessed: &mut NodeIdDeque,
    selection: &mut NodeIdSet,
    available: &mut NodeIdSet,
    found_quorums: &mut Vec<NodeIdSet>,
    fbas: &Fbas,
) {
    let mut antiselection = available.clone();
    if let Some(intersecting_quorums) = find_nonintersecting_quorums_step(
        unprocessed,
        selection,
        available,
        &mut antiselection,
        found_quorums,
        fbas,
    ) {
        assert!(intersecting_quorums.iter().all(|x| fbas.is_quorum(x)));
        assert!(intersecting_quorums[0].is_disjoint(&intersecting_quorums[1]));
        *found_quorums = intersecting_quorums.to_vec();
    }
}
fn find_nonintersecting_quorums_step(
    unprocessed: &mut NodeIdDeque,
    selection: &mut NodeIdSet,
    available: &mut NodeIdSet,
    antiselection: &mut NodeIdSet,
    found_quorums: &mut Vec<NodeIdSet>,
    fbas: &Fbas,
) -> Option<[NodeIdSet; 2]> {
    debug_assert!(selection.is_disjoint(&antiselection));
    if fbas.is_quorum(selection) {
        let (potential_complement, _) = find_unsatisfiable_nodes(&antiselection, fbas);

        if !potential_complement.is_empty() {
            return Some([selection.clone(), potential_complement]);
        } else {
            found_quorums.push(selection.clone());
        }
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        selection.insert(current_candidate);
        antiselection.remove(current_candidate);

        if let Some(intersecting_quorums) = find_nonintersecting_quorums_step(
            unprocessed,
            selection,
            available,
            antiselection,
            found_quorums,
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
                found_quorums,
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

fn quorums_possible(selection: &NodeIdSet, available: &NodeIdSet, fbas: &Fbas) -> bool {
    selection.iter().all(|x| fbas.nodes[x].is_quorum(available))
}

pub fn find_unsatisfiable_nodes(nodes: &NodeIdSet, fbas: &Fbas) -> (NodeIdSet, NodeIdSet) {
    let (mut satisfiable, mut unsatisfiable) = (bitset![], bitset![]);
    for node_id in nodes.iter() {
        if fbas.nodes[node_id].quorum_set.is_quorum(&nodes) {
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

fn reduce_to_strongly_connected_components(
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

        let expected = vec![bitset![4, 6], bitset![3, 10]];
        let actual = find_nonintersecting_or_minimal_quorums(&fbas);

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
