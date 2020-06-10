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

pub fn involved_nodes(node_sets: &[NodeIdSet]) -> NodeIdSet {
    let mut all_nodes: NodeIdSet = bitset![];
    for node_set in node_sets {
        all_nodes.union_with(node_set);
    }
    all_nodes
}

/// Reduce to minimal node sets, i.e. to a set of node sets so that no member set is a superset of another.
pub fn remove_non_minimal_node_sets(mut node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
    debug!("Removing duplicates...");
    let len_before = node_sets.len();
    node_sets.sort();
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

pub fn contains_only_minimal_node_sets(node_sets: &[NodeIdSet]) -> bool {
    node_sets.iter().all(|x| {
        node_sets
            .iter()
            .filter(|&y| x != y)
            .all(|y| !y.is_subset(x))
    })
}

pub(crate) fn remove_non_minimal_node_sets_from_buckets(
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
    debug_assert!(contains_only_minimal_node_sets(&minimal_node_sets));
    minimal_node_sets
}

// For each member node set, check if one of its "smaller by one" subsets is also a member.
// If yes, then filter it out, as it is obviously non-minimal.
// This function can be used to reduce (in some cases even eliminate) the workload on the slower
// `remove_non_minimal_node_sets`.
pub(crate) fn remove_node_sets_that_are_non_minimal_by_one(
    node_sets: HashSet<NodeIdSet>,
) -> Vec<NodeIdSet> {
    let mut remaining_sets = vec![];
    let mut tester: NodeIdSet;
    let mut is_minimal_by_one;

    debug!("Filtering node sets that are non-minimal by one...");
    for (i, node_set) in node_sets.iter().enumerate() {
        if i % 100_000 == 0 {
            debug!(
                "...at node set {}; {} remaining sets",
                i,
                remaining_sets.len()
            );
        }
        is_minimal_by_one = true;
        // whyever, using clone() here seems to be faster than clone_from()
        tester = node_set.clone();

        for node_id in node_set.iter() {
            tester.remove(node_id);
            if node_sets.contains(&tester) {
                is_minimal_by_one = false;
                break;
            }
            tester.insert(node_id);
        }
        if is_minimal_by_one {
            remaining_sets.push(node_set.clone());
        }
    }
    debug!("Filtering done.");
    remaining_sets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_set_intersections() {
        assert!(all_intersect(&vec![
            bitset![0, 1],
            bitset![0, 2],
            bitset![1, 2]
        ]));
        assert!(!all_intersect(&vec![bitset![0], bitset![1, 2]]));
    }

    #[test]
    fn minimize_node_sets() {
        let non_minimal = vec![bitset![0, 1, 2], bitset![0, 1], bitset![0, 2]];
        let expected = vec![bitset![0, 1], bitset![0, 2]];
        let actual = remove_non_minimal_node_sets(non_minimal);
        assert_eq!(expected, actual);
    }
}
