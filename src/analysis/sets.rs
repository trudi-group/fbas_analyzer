use super::*;

pub fn all_intersect(node_sets: &[NodeIdSet]) -> bool {
    // quick check
    let max_size = involved_nodes(node_sets).len();
    if node_sets.iter().all(|x| x.len() > max_size / 2) {
        true
    } else {
        // slow check
        node_sets
            .iter()
            .enumerate()
            .all(|(i, x)| node_sets.iter().skip(i + 1).all(|y| !x.is_disjoint(y)))
    }
}

/// Returns the union of all sets in `node_sets`.
pub fn involved_nodes(node_sets: &[NodeIdSet]) -> NodeIdSet {
    let mut all_nodes: NodeIdSet = bitset![];
    for node_set in node_sets {
        all_nodes.union_with(node_set);
    }
    all_nodes
}

/// Does pre- and postprocessing common to most finders
pub(crate) fn find_minimal_sets<F>(fbas: &Fbas, finder: F) -> Vec<NodeIdSet>
where
    F: Fn(Vec<NodeIdSet>, &Fbas) -> Vec<NodeIdSet>,
{
    let mut sets = find_sets(fbas, finder);
    debug_assert!(is_set_of_minimal_node_sets(&sets));
    sets.sort_unstable();
    sets.sort_by_key(|x| x.len());
    sets
}
/// Does preprocessing common to all finders
pub(crate) fn find_sets<F, R>(fbas: &Fbas, finder: F) -> Vec<R>
where
    F: Fn(Vec<NodeIdSet>, &Fbas) -> Vec<R>,
{
    let all_nodes: NodeIdSet = (0..fbas.nodes.len()).collect();

    debug!("Removing nodes not part of any quorum...");
    let (satisfiable, unsatisfiable) = find_satisfiable_nodes(&all_nodes, fbas);
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
        .filter(|node_set| contains_quorum(node_set, fbas))
        .collect();
    if consensus_clusters.len() > 1 {
        warn!(
            "{} connected components contain quorums => the FBAS lacks quorum intersection!",
            consensus_clusters.len()
        );
    }
    finder(consensus_clusters, fbas)
}

/// Reduce to minimal node sets, i.e. to a set of node sets so that no member set is a superset of another.
pub fn remove_non_minimal_node_sets(mut node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
    debug!("Removing duplicates...");
    let len_before = node_sets.len();
    node_sets.sort_unstable();
    node_sets.dedup();
    debug!("Done; removed {} duplicates.", len_before - node_sets.len());

    debug!("Sorting node sets into buckets, by length...");
    let max_len_upper_bound = node_sets.iter().map(|x| x.len()).max().unwrap_or(0) + 1;
    let mut buckets_by_len: Vec<Vec<NodeIdSet>> = vec![vec![]; max_len_upper_bound];
    for node_set in node_sets.into_iter() {
        buckets_by_len[node_set.len()].push(node_set);
    }
    debug!(
        "Sorting done; #nodes per bucket: {:?}",
        buckets_by_len
            .iter()
            .map(|x| x.len())
            .enumerate()
            .collect::<Vec<(usize, usize)>>()
    );
    remove_non_minimal_node_sets_from_buckets(buckets_by_len)
}

/// Removes non-minimal node sets based on a closure that determines minimality.
pub(crate) fn remove_non_minimal_x<F>(
    node_sets: Vec<NodeIdSet>,
    is_minimal: F,
    fbas: &Fbas,
) -> Vec<NodeIdSet>
where
    F: Fn(&NodeIdSet, &Fbas) -> bool,
{
    let mut minimal_x = BTreeSet::new();

    debug!("Filtering out non-minimal node sets...");
    for (i, node_set) in node_sets.into_iter().enumerate() {
        if i % 100_000 == 0 {
            debug!("...at set {}; {} minimal sets", i, minimal_x.len());
        }
        if !minimal_x.contains(&node_set) && is_minimal(&node_set, fbas) {
            minimal_x.insert(node_set);
        }
    }
    let minimal_x: Vec<NodeIdSet> = minimal_x.into_iter().collect();
    debug!("Filtering done.");
    debug_assert!(is_set_of_minimal_node_sets(&minimal_x));
    minimal_x
}

pub fn is_set_of_minimal_node_sets(node_sets: &[NodeIdSet]) -> bool {
    node_sets.iter().enumerate().all(|(i, x)| {
        node_sets
            .iter()
            .enumerate()
            .all(|(j, y)| i == j || !x.is_subset(y))
    })
}

fn remove_non_minimal_node_sets_from_buckets(
    buckets_by_len: Vec<impl IntoIterator<Item = NodeIdSet>>,
) -> Vec<NodeIdSet> {
    debug!("Filtering non-minimal node sets...");
    let mut minimal_node_sets: Vec<NodeIdSet> = vec![];
    let mut minimal_node_sets_current_len: Vec<NodeIdSet> = vec![];
    for (i, bucket) in buckets_by_len.into_iter().enumerate() {
        debug!(
            "...at bucket {}; {} minimal node sets",
            i,
            minimal_node_sets.len()
        );
        for node_set in bucket.into_iter() {
            if minimal_node_sets.iter().all(|x| !x.is_subset(&node_set)) {
                minimal_node_sets_current_len.push(node_set);
            }
        }
        minimal_node_sets.append(&mut minimal_node_sets_current_len);
    }
    debug!("Filtering done.");
    debug_assert!(is_set_of_minimal_node_sets(&minimal_node_sets));
    minimal_node_sets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_set_intersections() {
        assert!(all_intersect(&[
            bitset![0, 1],
            bitset![0, 2],
            bitset![1, 2]
        ]));
        assert!(!all_intersect(&[bitset![0], bitset![1, 2]]));
    }

    #[test]
    fn minimize_node_sets() {
        let non_minimal = vec![bitset![0, 1, 2], bitset![0, 1], bitset![0, 2]];
        let expected = vec![bitset![0, 1], bitset![0, 2]];
        let actual = remove_non_minimal_node_sets(non_minimal);
        assert_eq!(expected, actual);
    }

    #[test]
    fn is_set_of_minimal_node_sets_detects_duplicates() {
        let sets = vec![bitset![0, 1], bitset![0, 1]];
        assert!(!is_set_of_minimal_node_sets(&sets));
    }

    #[test]
    fn remove_non_minimal_x_removes_duplicates() {
        let sets = vec![bitset![0, 1], bitset![0, 1]];
        let actual = remove_non_minimal_x(sets, |_, _| true, &Fbas::new());
        let expected = vec![bitset![0, 1]];
        assert_eq!(expected, actual);
    }
}
