use super::*;

pub fn find_minimal_intersections(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    // TODO clean me up
    // find_optionally_smallest_minimal_intersections(node_sets, None)

    let max_len_upper_bound = node_sets.iter().map(|x| x.len()).max().unwrap_or(0);
    let mut buckets_by_len: Vec<Vec<(usize, usize)>> = vec![vec![]; max_len_upper_bound];

    // we don't store intersections themselves at this stage to save memory
    // (this also improves cache locality during second step?)
    let mut intersection;
    for (i, ns1) in node_sets.iter().enumerate() {
        for (j, ns2) in node_sets.iter().enumerate().skip(i + 1) {
            intersection = ns1.clone();
            intersection.intersect_with(ns2);
            buckets_by_len[intersection.len()].push((i, j));
        }
    }

    // second step: reduce to minimal
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
    minimal_intersections
}

pub fn find_smallest_minimal_intersections(
    node_sets: &[NodeIdSet],
    epsilon: usize,
) -> Vec<NodeIdSet> {
    find_optionally_smallest_minimal_intersections(node_sets, Some(epsilon))
}

pub fn find_optionally_smallest_minimal_intersections(
    node_sets: &[NodeIdSet],
    o_epsilon: Option<usize>,
) -> Vec<NodeIdSet> {
    debug!("Collecting pairwise intersections...");
    let mut intersections = match o_epsilon {
        None => find_intersections(node_sets),
        Some(epsilon) => find_small_intersections(node_sets, epsilon),
    };
    info!("Found {} pairwise intersections.", intersections.len());
    if intersections.is_empty() {
        return intersections;
    } else if let Some(epsilon) = o_epsilon {
        debug!(
            "Reducing to smallest intersections, with epsilon {}.",
            epsilon
        );
        intersections = reduce_to_smallest(intersections, epsilon);
        let minimal_size = intersections[0].len();
        info!(
            "Reduced to {} intersections with size between {} and {}.",
            intersections.len(),
            minimal_size,
            minimal_size + epsilon
        );
    }
    debug!("Reducing to minimal intersections...");
    let minimal_intersections = remove_non_minimal_node_sets(intersections);
    info!(
        "Reduced to {} minimal intersections.",
        minimal_intersections.len()
    );
    minimal_intersections
}

// TODO never used anymore, clean me up
fn find_intersections(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
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

fn find_small_intersections(node_sets: &[NodeIdSet], epsilon: usize) -> Vec<NodeIdSet> {
    let mut intersections: Vec<NodeIdSet> = vec![];
    let mut smallest_intersection_size = node_sets.first().unwrap_or(&bitset![]).len();

    for (i, ns1) in node_sets.iter().enumerate() {
        for ns2 in node_sets.iter().skip(i + 1) {
            let mut intersection = ns1.clone();
            intersection.intersect_with(ns2);
            if intersection.len() <= smallest_intersection_size + epsilon {
                if intersection.len() < smallest_intersection_size {
                    smallest_intersection_size = intersection.len();
                }
                intersections.push(intersection);
            }
        }
    }
    intersections
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_intersections_simple() {
        let node_sets = vec![bitset![0, 1, 2], bitset![0, 2, 3], bitset![0, 1, 3]];

        let expected = vec![bitset![0, 2], bitset![0, 1], bitset![0, 3]];
        let actual = find_intersections(&node_sets);

        assert_eq!(expected, actual);
    }

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
    fn find_smallest_minimal_intersections_less_simple() {
        let node_sets = vec![
            bitset![0, 1, 2],
            bitset![0, 1, 3],
            bitset![1, 2, 3],
            bitset![0, 3],
        ];
        let expected = vec![bitset![0], bitset![3]];
        let actual = find_smallest_minimal_intersections(&node_sets, 0);

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
