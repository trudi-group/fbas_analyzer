use super::*;
use std::ops::Index;

struct MembershipsMap(Vec<BitSet>);

pub fn find_minimal_blocking_sets(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    info!("Getting blocking sets...");
    let blocking_sets = find_blocking_sets(node_sets);
    info!("Found {} blocking sets.", blocking_sets.len());
    let minimal_blocking_sets = remove_non_minimal_node_sets(blocking_sets);
    info!(
        "Reduced to {} minimal blocking sets.",
        minimal_blocking_sets.len()
    );
    minimal_blocking_sets
}

fn find_blocking_sets(node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
    let (mut unprocessed, memberships) = extract_nodes_and_node_set_memberships(node_sets);

    info!("Sorting nodes by number of memberships...");
    unprocessed = sort_by_number_of_node_set_memberships(unprocessed, &memberships);
    info!("Sorted.");

    let mut unprocessed = NodeIdDeque::from(unprocessed);
    let mut selection = NodeIdSet::new();
    let missing_node_sets: BitSet = (0..node_sets.len()).collect();

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
            bitset![0, 1],
            bitset![0, 2],
            bitset![1, 3],
            bitset![0, 1, 2],
        ];

        let expected = vec![bitset![0, 1], bitset![0, 3], bitset![1, 2]];
        let actual = find_minimal_blocking_sets(&node_sets);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_blocking_sets_nontrivial() {
        let fbas = Fbas::from_json_file("test_data/correct.json");

        let minimal_quorums = find_minimal_quorums(&fbas);
        let minimal_blocking_sets = find_minimal_blocking_sets(&minimal_quorums);

        assert_eq!(minimal_quorums, minimal_blocking_sets);
    }

    #[test]
    #[ignore]
    fn minimal_blocking_sets_more_minimal_than_minimal_quorums() {
        let fbas = Fbas::from_json_file("test_data/stellarbeat_nodes_2019-09-17.json");
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
