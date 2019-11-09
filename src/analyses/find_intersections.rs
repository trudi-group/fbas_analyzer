use super::*;

pub fn find_minimal_intersections(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    let max_len_upper_bound = node_sets.iter().map(|x| x.len()).max().unwrap_or(0);
    let mut buckets_by_len: Vec<Vec<(usize, usize)>> = vec![vec![]; max_len_upper_bound];

    // we don't store intersections themselves at this stage to save memory
    // (this also improves cache locality during second step?)
    debug!("Sorting pairwise intersections by length...");
    let mut intersection;
    for (i, ns1) in node_sets.iter().enumerate() {
        for (j, ns2) in node_sets.iter().enumerate().skip(i + 1) {
            intersection = ns1.clone();
            intersection.intersect_with(ns2);
            buckets_by_len[intersection.len()].push((i, j));
        }
    }

    debug!("Reducing to minimal intersections...");
    let mut minimal_intersections: Vec<NodeIdSet> = vec![];
    let mut minimal_intersections_current_len: Vec<NodeIdSet> = vec![];
    for bucket in buckets_by_len.into_iter() {
        for (i, j) in bucket.into_iter() {
            intersection = node_sets[i].clone();
            intersection.intersect_with(&node_sets[j]);
            if minimal_intersections
                .iter()
                .all(|x| !x.is_subset(&intersection))
            {
                minimal_intersections.push(intersection);
            }
        }
        minimal_intersections.append(&mut minimal_intersections_current_len);
    }
    info!(
        "Found {} minimal intersections.",
        minimal_intersections.len()
    );
    minimal_intersections
}

fn _old_find_minimal_intersections(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    debug!("Collecting pairwise intersections...");
    let intersections = _old_find_intersections(node_sets);
    info!("Found {} pairwise intersections.", intersections.len());

    debug!("Reducing to minimal intersections...");
    let minimal_intersections = remove_non_minimal_node_sets(intersections);
    info!(
        "Reduced to {} minimal intersections.",
        minimal_intersections.len()
    );
    minimal_intersections
}

// TODO never used anymore, clean me up
fn _old_find_intersections(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    let expected_len = node_sets.len() * (node_sets.len() - 1) / 2;
    let mut intersections: Vec<NodeIdSet> = Vec::with_capacity(expected_len);

    let mut intersection; // defining this here saves allocations...
    for (i, ns1) in node_sets.iter().enumerate() {
        for ns2 in node_sets.iter().skip(i + 1) {
            intersection = ns1.clone();
            intersection.intersect_with(ns2);
            intersections.push(intersection.clone());
        }
    }
    intersections
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_minimal_intersections_simple() {
        let node_sets = vec![bitset![0, 1, 2], bitset![0, 2], bitset![0, 3]];

        let expected = vec![bitset![0]];
        let actual = find_minimal_intersections(&node_sets);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_intersections_less_simple() {
        let node_sets = vec![
            bitset![0, 1, 2],
            bitset![0, 1, 3],
            bitset![1, 2, 3],
            bitset![0, 3],
        ];
        let expected = vec![bitset![0], bitset![3], bitset![1, 2]];
        let actual = find_minimal_intersections(&node_sets);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_intersections_some_dont_intersect() {
        let node_sets = vec![bitset![0, 1], bitset![0, 2], bitset![1, 3]];
        let expected = vec![bitset![]];
        let actual = find_minimal_intersections(&node_sets);

        assert_eq!(expected, actual);
    }
}
