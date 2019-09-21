use super::*;

pub fn find_minimal_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    let quorums = find_quorums(fbas);
    info!("Found {} quorums.", quorums.len());
    let minimal_quorums = remove_non_minimal_node_sets(quorums);
    info!("Reduced to {} minimal quorums.", minimal_quorums.len());
    minimal_quorums
}

pub fn find_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    let n = fbas.nodes.len();
    let mut unprocessed: Vec<NodeId> = (0..n).collect();

    info!("Reducing to strongly connected components...");
    unprocessed = reduce_to_strongly_connected_components(unprocessed, fbas);
    info!(
        "Reducing removed {} of {} nodes...",
        n - unprocessed.len(),
        n
    );

    info!("Sorting nodes by rank...");
    unprocessed = sort_by_rank(unprocessed, fbas);
    info!("Sorted.");

    let mut selection = NodeIdSet::with_capacity(n);
    let mut available = unprocessed.iter().cloned().collect();

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

/// A quick and dirty something resembling page rank
pub fn sort_by_rank(nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeId> {
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

fn reduce_to_strongly_connected_components(nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeId> {
    // can probably be done faster
    let k = nodes.len();
    let reduced_once = remove_nodes_not_included_in_quorum_slices(nodes, fbas);

    if reduced_once.len() < k {
        reduce_to_strongly_connected_components(reduced_once, fbas)
    } else {
        reduced_once
    }
}

fn remove_nodes_not_included_in_quorum_slices(nodes: Vec<NodeId>, fbas: &Fbas) -> Vec<NodeId> {
    let mut included_nodes = NodeIdSet::with_capacity(fbas.nodes.len());

    for node_id in nodes {
        let node = &fbas.nodes[node_id];
        for included_node in node.quorum_set.contained_nodes().into_iter() {
            included_nodes.insert(included_node);
        }
    }
    included_nodes.into_iter().collect()
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
}
