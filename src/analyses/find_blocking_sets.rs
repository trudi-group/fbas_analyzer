use super::*;
use std::collections::BTreeMap;

pub fn find_minimal_blocking_sets(quorums: &[NodeIdSet]) -> Vec<NodeIdSet> {
    info!("Getting blocking sets...");
    let blocking_sets = find_blocking_sets(quorums);
    info!("Found {} blocking sets.", blocking_sets.len());
    let minimal_blocking_sets = remove_non_minimal_node_sets(blocking_sets);
    info!(
        "Reduced to {} minimal blocking sets.",
        minimal_blocking_sets.len()
    );
    minimal_blocking_sets
}

fn find_blocking_sets(quorums: &[NodeIdSet]) -> Vec<NodeIdSet> {
    // TODO has refactoring and performance tuning potential

    let (quorum_memberships, mut quorum_members) = extract_quorum_memberships(quorums);

    let mut unprocessed: Vec<NodeId> = quorum_memberships.keys().cloned().collect();

    info!("Sorting nodes by number of quorum membership...");
    unprocessed = sort_by_number_of_quorum_memberships(unprocessed, &quorum_memberships);
    info!("Sorted.");

    let mut unprocessed = NodeIdDeque::from(unprocessed);
    let mut selection = Vec::new();
    let missing_quorums: BitSet = (0..quorums.len()).collect();

    fn step(
        unprocessed: &mut NodeIdDeque,
        selection: &mut Vec<NodeId>,
        remaining_quorum_members: &mut Vec<u32>,
        missing_quorums: BitSet,
        quorum_memberships: &BTreeMap<NodeId, BitSet>,
    ) -> Vec<NodeIdSet> {
        let mut result: Vec<NodeIdSet> = vec![];

        if missing_quorums.is_empty() {
            result.push(selection.iter().cloned().collect());
        } else if missing_quorums
            .iter()
            .all(|x| remaining_quorum_members[x] == 0)
        {
        } else if let Some(current_candidate) = unprocessed.pop_front() {
            for quorum_id in quorum_memberships[&current_candidate].iter() {
                remaining_quorum_members[quorum_id] -= 1;
            }

            selection.push(current_candidate);
            let mut updated_missing_quorums = missing_quorums.clone();
            for quorum in quorum_memberships[&current_candidate].iter() {
                updated_missing_quorums.remove(quorum);
            }
            result.extend(step(
                unprocessed,
                selection,
                remaining_quorum_members,
                updated_missing_quorums,
                quorum_memberships,
            ));

            selection.pop();
            result.extend(step(
                unprocessed,
                selection,
                remaining_quorum_members,
                missing_quorums,
                quorum_memberships,
            ));

            unprocessed.push_front(current_candidate);
            for quorum_id in quorum_memberships[&current_candidate].iter() {
                remaining_quorum_members[quorum_id] += 1;
            }
        }
        result
    }
    step(
        &mut unprocessed,
        &mut selection,
        &mut quorum_members,
        missing_quorums,
        &quorum_memberships,
    )
}

fn extract_quorum_memberships(quorums: &[NodeIdSet]) -> (BTreeMap<NodeId, BitSet>, Vec<u32>) {

    let mut quorum_memberships: BTreeMap<NodeId, BitSet> = BTreeMap::new();
    let mut quorum_members: Vec<u32> = vec![0; quorums.len()];

    for (quorum_id, quorum) in quorums.iter().enumerate() {
        for node_id in quorum.iter() {
            (*quorum_memberships
                .entry(node_id)
                .or_insert_with(BitSet::new))
            .insert(quorum_id);
            quorum_members[quorum_id] += 1;
        }
    }
    (quorum_memberships, quorum_members)
}

/// Sort so that nodes included in many quorums are first
fn sort_by_number_of_quorum_memberships(nodes: Vec<NodeId>, quorum_memberships: &BTreeMap<NodeId, BitSet>) -> Vec<NodeId> {

    let mut nodes = nodes;
    nodes.sort_by(|x, y| {
        quorum_memberships[y]
            .len()
            .cmp(&quorum_memberships[x].len())
    });
    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_minimal_blocking_sets_simple() {
        let minimal_quorums = vec![bitset![0, 1], bitset![0, 2]];

        let expected = vec![bitset![0], bitset![1, 2]];
        let actual = find_minimal_blocking_sets(&minimal_quorums);

        assert_eq!(expected, actual);
    }

    #[test]
    #[ignore]
    fn minimal_blocking_sets_more_minimal_than_minimal_quorums() {
        let fbas = Fbas::from_json_file("test_data/stellarbeat_2019-08-02.json");
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
