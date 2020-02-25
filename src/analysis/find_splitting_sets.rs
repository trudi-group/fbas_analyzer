use super::*;

pub fn find_minimal_splitting_sets(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    debug!("Sorting pairwise intersections by length...");
    let buckets_by_len = find_splitting_sets_into_buckets(node_sets);
    info!(
        "Found {} unique pairwise intersections.",
        buckets_by_len.iter().map(|x| x.len()).sum::<usize>()
    );

    debug!("Reducing to minimal splitting sets...");
    let minimal_splitting_sets = remove_non_minimal_node_sets_from_buckets(buckets_by_len);
    info!(
        "Found {} minimal splitting sets.",
        minimal_splitting_sets.len()
    );
    minimal_splitting_sets
}

fn find_splitting_sets_into_buckets(node_sets: &[NodeIdSet]) -> Vec<BTreeSet<NodeIdSet>> {
    // we use BTreeSets here to avoid storing duplicates
    let max_len_upper_bound = node_sets.iter().map(|x| x.len()).max().unwrap_or(0) + 1;
    let mut buckets_by_len: Vec<BTreeSet<NodeIdSet>> = vec![BTreeSet::new(); max_len_upper_bound];
    let mut intersection; // defining this here saves allocations...
    for (i, ns1) in node_sets.iter().enumerate() {
        for ns2 in node_sets.iter().skip(i + 1) {
            intersection = ns1.clone();
            intersection.intersect_with(ns2);
            buckets_by_len[intersection.len()].insert(intersection);
        }
    }
    buckets_by_len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_minimal_splitting_sets_simple() {
        let node_sets = vec![bitset![0, 1, 2], bitset![0, 2], bitset![0, 3]];

        let expected = vec![bitset![0]];
        let actual = find_minimal_splitting_sets(&node_sets);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_splitting_sets_less_simple() {
        let node_sets = vec![
            bitset![0, 1, 2],
            bitset![0, 1, 3],
            bitset![1, 2, 3],
            bitset![0, 3],
        ];
        let expected = vec![bitset![0], bitset![3], bitset![1, 2]];
        let actual = find_minimal_splitting_sets(&node_sets);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_splitting_sets_some_dont_intersect() {
        let node_sets = vec![bitset![0, 1], bitset![0, 2], bitset![1, 3]];
        let expected = vec![bitset![]];
        let actual = find_minimal_splitting_sets(&node_sets);

        assert_eq!(expected, actual);
    }
}
