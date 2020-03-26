use super::*;

pub fn find_minimal_splitting_sets(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    debug!("Enumerating all unique pairwise intersections...");
    let splitting_sets = find_splitting_sets(node_sets);
    info!(
        "Found {} unique pairwise intersections.",
        splitting_sets.len()
    );

    debug!("Reducing to minimal splitting sets...");
    let minimal_splitting_sets =
        remove_non_minimal_node_sets(remove_node_sets_that_are_non_minimal_by_one(splitting_sets));
    info!(
        "Found {} minimal splitting sets.",
        minimal_splitting_sets.len()
    );
    minimal_splitting_sets
}

fn find_splitting_sets(node_sets: &[NodeIdSet]) -> HashSet<NodeIdSet> {
    // we use a HashSet here to avoid storing duplicates
    let mut splitting_sets: HashSet<NodeIdSet> = HashSet::new();
    let mut intersection; // defining this here saves allocations...
    for (i, ns1) in node_sets.iter().enumerate() {
        if i % 1000 == 0 {
            debug!(
                "...at pair ({}, {}); {} splitting sets",
                i,
                i + 1,
                splitting_sets.len()
            );
        }
        for ns2 in node_sets.iter().skip(i) {
            intersection = ns1.clone();
            intersection.intersect_with(ns2);
            splitting_sets.insert(intersection);
        }
    }
    splitting_sets
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

    #[test]
    fn find_minimal_splitting_sets_if_one_quorum() {
        let node_sets = vec![bitset![0, 1, 2]];

        // If there is only one node set, this node set (usually a quorum) is the splitting set.
        // This ensures correct results in cases with multiple quorums but only one minimal quorum.
        // This also makes sense if there is only one quorum in the FBAS: if the whole quorum is
        // faulty, it can fail (and split itself) in arbitrary ways.
        let expected = vec![bitset![0, 1, 2]];
        let actual = find_minimal_splitting_sets(&node_sets);

        assert_eq!(expected, actual);
    }
}
