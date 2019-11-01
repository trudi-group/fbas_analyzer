use super::*;

pub fn find_minimal_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    let quorums = find_quorums(fbas);
    info!("Found {} (not necessarily minimal) quorums.", quorums.len());
    let minimal_quorums = remove_non_minimal_node_sets(quorums);
    info!("Reduced to {} minimal quorums.", minimal_quorums.len());
    minimal_quorums
}

fn find_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
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

    debug!("Collecting quorums...");
    fn step(
        unprocessed: &mut NodeIdDeque,
        selection: &mut NodeIdSet,
        available: &mut NodeIdSet,
        fbas: &Fbas,
    ) -> Vec<NodeIdSet> {
        let mut result: Vec<NodeIdSet> = vec![];

        if fbas.is_quorum(selection) {
            result.push(selection.clone());
        } else if let Some(current_candidate) = unprocessed.pop_front() {
            selection.insert(current_candidate);

            result.extend(step(unprocessed, selection, available, fbas));

            selection.remove(current_candidate);
            available.remove(current_candidate);

            if quorums_possible(selection, available, fbas) {
                result.extend(step(unprocessed, selection, available, fbas));
            }

            unprocessed.push_front(current_candidate);
            available.insert(current_candidate);
        }
        result
    }
    fn quorums_possible(selection: &NodeIdSet, available: &NodeIdSet, fbas: &Fbas) -> bool {
        selection.iter().all(|x| fbas.nodes[x].is_quorum(available))
    }
    step(
        &mut unprocessed.into(),
        &mut selection,
        &mut available,
        fbas,
    )
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

/// A quick and dirty something resembling page rank
fn sort_by_rank(nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeId> {
    // TODO not protected against overflows ...
    let mut scores: Vec<u64> = vec![1; fbas.nodes.len()];

    let runs = 10;

    for _ in 0..runs {
        let scores_snapshot = scores.clone();

        for node_id in nodes.iter().copied() {
            let node = &fbas.nodes[node_id];

            for trusted_node_id in node.quorum_set.contained_nodes().into_iter() {
                scores[trusted_node_id] += scores_snapshot[node_id];
            }
        }
    }
    let mut nodes = nodes;
    // sort by "highest score first"
    nodes.sort_by(|x, y| scores[*y].cmp(&scores[*x]));
    nodes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn find_minimal_quorums_correct_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        let expected = vec![bitset![0, 1], bitset![0, 2], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_quorums_broken_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));

        let expected = vec![bitset![0], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_quorums_broken_trivial_reversed_node_ids() {
        let mut fbas = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));
        fbas.nodes.reverse();

        let expected = vec![bitset![2], bitset![0, 1]];
        let actual = find_minimal_quorums(&fbas);

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
    fn find_transitivel_unsatisfiable_nodes() {
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
}
