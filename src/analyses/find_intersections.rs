use super::*;

pub fn find_minimal_intersections(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    info!("Getting pairwise intersections...");
    let intersections = find_intersections(node_sets);
    info!("Found {} pairwise intersections.", intersections.len());
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
            let mut intersection_tmp = ns1.clone();
            intersection_tmp.intersect_with(ns2);
            intersections.push(intersection_tmp);
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
            bitset![0, 1],
            bitset![0, 2, 3],
            bitset![1, 3],
            bitset![0, 1, 2],
        ];
        let expected = vec![bitset![0], bitset![1], bitset![3]];
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
