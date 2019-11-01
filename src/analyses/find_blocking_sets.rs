use super::*;
use std::ops::Index;

pub fn find_minimal_blocking_sets(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    find_optionally_smallest_minimal_blocking_sets(node_sets, None)
}

pub fn find_smallest_minimal_blocking_sets(
    node_sets: &[NodeIdSet],
    epsilon: usize,
) -> Vec<NodeIdSet> {
    find_optionally_smallest_minimal_blocking_sets(node_sets, Some(epsilon))
}

pub fn find_optionally_smallest_minimal_blocking_sets(
    node_sets: &[NodeIdSet],
    o_epsilon: Option<usize>,
) -> Vec<NodeIdSet> {
    debug!("Getting blocking sets...");
    let mut blocking_sets = find_blocking_sets(node_sets);
    info!("Found {} blocking sets.", blocking_sets.len());
    if blocking_sets.is_empty() {
        return blocking_sets;
    } else if let Some(epsilon) = o_epsilon {
        debug!(
            "Reducing to smallest blocking sets, with epsilon {}.",
            epsilon
        );
        blocking_sets = reduce_to_smallest(blocking_sets, epsilon);
        let minimal_size = blocking_sets[0].len();
        info!(
            "Reduced to {} blocking sets with size between {} and {}.",
            blocking_sets.len(),
            minimal_size,
            minimal_size + epsilon
        );
    }
    let minimal_blocking_sets = remove_non_minimal_node_sets(blocking_sets);
    info!(
        "Reduced to {} minimal blocking sets.",
        minimal_blocking_sets.len()
    );
    minimal_blocking_sets
}

fn find_blocking_sets(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    let (mut unprocessed, memberships) = extract_nodes_and_node_set_memberships(node_sets);

    debug!("Sorting nodes by number of memberships...");
    unprocessed = sort_by_number_of_node_set_memberships(unprocessed, &memberships);
    debug!("Sorted.");

    let mut unprocessed = NodeIdDeque::from(unprocessed);
    let mut selection = NodeIdSet::new();
    let missing_node_sets: BitSet = (0..node_sets.len()).collect();

    debug!("Collecting blocking sets...");
    fn step(
        unprocessed: &mut NodeIdDeque,
        selection: &mut NodeIdSet,
        missing_node_sets: BitSet,
        memberships: &MembershipsMap,
    ) -> Vec<NodeIdSet> {
        let mut result: Vec<NodeIdSet> = vec![];

        if missing_node_sets.is_empty() {
            result.push(selection.clone());
        } else if let Some(current_candidate) = unprocessed.pop_front() {
            let useful = !missing_node_sets.is_disjoint(&memberships[current_candidate]);

            if useful {
                selection.insert(current_candidate);
                let mut updated_missing_node_sets = missing_node_sets.clone();
                for node_set in memberships[current_candidate].iter() {
                    updated_missing_node_sets.remove(node_set);
                }
                result.extend(step(
                    unprocessed,
                    selection,
                    updated_missing_node_sets,
                    memberships,
                ));
                selection.remove(current_candidate);
            }
            result.extend(step(unprocessed, selection, missing_node_sets, memberships));
            unprocessed.push_front(current_candidate);
        }
        result
    }
    step(
        &mut unprocessed,
        &mut selection,
        missing_node_sets,
        &memberships,
    )
}

fn extract_nodes_and_node_set_memberships(
    node_sets: &[NodeIdSet],
) -> (Vec<NodeId>, MembershipsMap) {
    let nodes: NodeIdSet = node_sets.iter().flatten().collect();
    let max_node_id = nodes.iter().max().unwrap_or(0);

    let mut memberships = MembershipsMap::new(max_node_id);

    for (node_set_id, node_set) in node_sets.iter().enumerate() {
        for node_id in node_set.iter() {
            memberships.insert(node_id, node_set_id);
        }
    }
    (nodes.into_iter().collect(), memberships)
}

/// Sort so that nodes included in many node sets are first
fn sort_by_number_of_node_set_memberships(
    nodes: Vec<NodeId>,
    memberships: &MembershipsMap,
) -> Vec<NodeId> {
    let mut nodes = nodes;
    nodes.sort_by(|x, y| memberships[*y].len().cmp(&memberships[*x].len()));
    nodes
}

struct MembershipsMap(Vec<BitSet>);
impl Index<NodeId> for MembershipsMap {
    type Output = BitSet;
    fn index(&self, i: NodeId) -> &BitSet {
        &self.0[i]
    }
}
impl MembershipsMap {
    pub fn new(biggest_index: NodeId) -> Self {
        MembershipsMap((0..=biggest_index).map(|_| BitSet::new()).collect())
    }
    pub fn insert(&mut self, member_id: NodeId, node_set_id: usize) -> bool {
        self.0[member_id].insert(node_set_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn find_minimal_blocking_sets_simple() {
        let minimal_node_sets = vec![bitset![0, 1], bitset![0, 2]];

        let expected = vec![bitset![0], bitset![1, 2]];
        let actual = find_minimal_blocking_sets(&minimal_node_sets);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_blocking_sets_less_simple() {
        let node_sets = vec![
            bitset![0, 2, 7],
            bitset![1, 3, 8],
            bitset![0, 1, 4, 9],
            bitset![0, 1, 2, 5],
        ];
        let expected = vec![
            bitset![0, 1],
            bitset![0, 3],
            bitset![0, 8],
            bitset![1, 2],
            bitset![1, 7],
            bitset![2, 3, 4],
            bitset![2, 3, 9],
            bitset![2, 4, 8],
            bitset![2, 8, 9],
            bitset![3, 4, 5, 7],
            bitset![3, 5, 7, 9],
            bitset![4, 5, 7, 8],
            bitset![5, 7, 8, 9],
        ];
        let actual = find_minimal_blocking_sets(&node_sets);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_smallest_minimal_blocking_sets_less_simple() {
        let node_sets = vec![
            bitset![0, 2, 7],
            bitset![1, 3, 8],
            bitset![0, 1, 4, 9],
            bitset![0, 1, 2, 5],
        ];
        let expected = vec![
            bitset![0, 1],
            bitset![0, 3],
            bitset![0, 8],
            bitset![1, 2],
            bitset![1, 7],
            bitset![2, 3, 4],
            bitset![2, 3, 9],
            bitset![2, 4, 8],
            bitset![2, 8, 9],
        ];
        let actual = find_smallest_minimal_blocking_sets(&node_sets, 1);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_blocking_sets_nontrivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));

        let minimal_quorums = find_minimal_quorums(&fbas);
        let minimal_blocking_sets = find_minimal_blocking_sets(&minimal_quorums);

        assert_eq!(minimal_quorums, minimal_blocking_sets);
    }

    #[test]
    #[ignore]
    fn minimal_blocking_sets_more_minimal_than_minimal_quorums() {
        let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let minimal_quorums = find_minimal_quorums(&fbas);
        let minimal_blocking_sets = find_minimal_blocking_sets(&minimal_quorums);

        let minimal_all = remove_non_minimal_node_sets(
            minimal_blocking_sets
                .iter()
                .chain(minimal_quorums.iter())
                .cloned()
                .collect(),
        );
        assert_eq!(minimal_blocking_sets, minimal_all);
    }
}
