use super::*;

impl Fbas {
    pub fn shrunken(fbas: &Self) -> (Self, Vec<NodeId>) {
        let (satisfiable_nodes, _) = fbas.unsatisfiable_nodes();
        let (strongly_connected_nodes, _) =
            reduce_to_strongly_connected_components(satisfiable_nodes, fbas);

        let shrink_map: HashMap<NodeId, NodeId> = strongly_connected_nodes
            .iter()
            .enumerate()
            .map(|(new, old)| (old, new))
            .collect();
        let unshrink_table: Vec<NodeId> = strongly_connected_nodes.into_iter().collect();

        let mut fbas_shrunken = Fbas::new_generic_unconfigured(unshrink_table.len());
        for old_id in 0..fbas.nodes.len() {
            if let Some(&new_id) = shrink_map.get(&old_id) {
                fbas_shrunken.nodes[new_id] = Node::shrunken(&fbas.nodes[old_id], &shrink_map);
            }
        }
        (fbas_shrunken, unshrink_table)
    }
}
impl Node {
    fn shrunken(node: &Self, shrink_map: &HashMap<NodeId, NodeId>) -> Self {
        Node {
            public_key: node.public_key.clone(),
            quorum_set: QuorumSet::shrunken(&node.quorum_set, shrink_map),
        }
    }
}
impl QuorumSet {
    fn shrunken(quorum_set: &Self, shrink_map: &HashMap<NodeId, NodeId>) -> Self {
        let mut validators = vec![];
        for old_id in quorum_set.validators.iter() {
            if let Some(&new_id) = shrink_map.get(&old_id) {
                validators.push(new_id);
            }
        }
        validators.sort();

        let mut inner_quorum_sets = vec![];
        for inner_quorum_set in quorum_set.inner_quorum_sets.iter() {
            let shrunken_inner_quorum_set = QuorumSet::shrunken(inner_quorum_set, shrink_map);
            if shrunken_inner_quorum_set != QuorumSet::new() {
                inner_quorum_sets.push(shrunken_inner_quorum_set);
            }
        }
        inner_quorum_sets.sort();

        let threshold = quorum_set.threshold;

        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn shrink_fbas_reduces_size() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let (fbas_shrunken, _) = Fbas::shrunken(&fbas);
        assert!(fbas_shrunken.number_of_nodes() < fbas.number_of_nodes());
    }

    #[test]
    fn shrink_quorum_set() {
        let qset = QuorumSet {
            threshold: 2,
            validators: vec![2, 3, 4],
            inner_quorum_sets: vec![],
        };
        let shrink_map: HashMap<NodeId, NodeId> = vec![(2, 0), (4, 1)].into_iter().collect();
        let expected = QuorumSet {
            threshold: 2,
            validators: vec![0, 1],
            inner_quorum_sets: vec![],
        };
        let actual = QuorumSet::shrunken(&qset, &shrink_map);
        assert_eq!(expected, actual);
    }

    #[test]
    fn shrink_unshrink_find_minimal_quorums() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let (fbas_shrunken, unshrink_table) = Fbas::shrunken(&fbas);

        let expected = find_minimal_quorums(&fbas);
        let actual = unshrink(find_minimal_quorums(&fbas_shrunken), &unshrink_table);
        assert_eq!(expected, actual);
    }
}
