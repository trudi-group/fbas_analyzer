use super::*;

pub fn find_minimal_intersections(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    find_optionally_smallest_minimal_intersections(node_sets, None)
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

fn find_intersections(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    let mut intersections: Vec<NodeIdSet> = vec![];

    for (i, ns1) in node_sets.iter().enumerate() {
        for ns2 in node_sets.iter().skip(i + 1) {
            let mut intersection = ns1.clone();
            intersection.intersect_with(ns2);
            intersections.push(intersection);
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
