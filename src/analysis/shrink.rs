use super::*;

pub fn unshrink_set(node_set: &NodeIdSet, unshrink_table: &[NodeId]) -> NodeIdSet {
    node_set.iter().map(|id| unshrink_table[id]).collect()
}

pub fn unshrink_sets(node_sets: &[NodeIdSet], unshrink_table: &[NodeId]) -> Vec<NodeIdSet> {
    node_sets
        .iter()
        .map(|node_set| unshrink_set(node_set, unshrink_table))
        .collect()
}

impl Fbas {
    pub fn shrunken(fbas: &Self) -> (Self, Vec<NodeId>, HashMap<NodeId, NodeId>) {
        let (satisfiable_nodes, _) = fbas.unsatisfiable_nodes();
        let (strongly_connected_nodes, _) =
            reduce_to_strongly_connected_nodes(satisfiable_nodes, fbas);

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
        (fbas_shrunken, unshrink_table, shrink_map)
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

impl<'fbas> Organizations<'fbas> {
    pub fn shrunken(
        orgs: &Self,
        shrink_map: &HashMap<NodeId, NodeId>,
        shrunken_fbas: &'fbas Fbas,
    ) -> Self {
        let organizations = orgs
            .organizations
            .iter()
            .map(|org| Organization::shrunken(org, shrink_map))
            .collect();
        Self::new(organizations, shrunken_fbas)
    }
}
impl Organization {
    fn shrunken(organization: &Self, shrink_map: &HashMap<NodeId, NodeId>) -> Self {
        let mut validators = vec![];
        for old_id in organization.validators.iter() {
            if let Some(&new_id) = shrink_map.get(&old_id) {
                validators.push(new_id);
            }
        }
        validators.sort();
        Organization {
            name: organization.name.clone(),
            validators,
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
        let (fbas_shrunken, _, _) = Fbas::shrunken(&fbas);
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
    fn shrink_organization() {
        let org = Organization {
            name: "test".to_string(),
            validators: vec![2, 3, 4],
        };
        let shrink_map: HashMap<NodeId, NodeId> = vec![(2, 0), (4, 1)].into_iter().collect();
        let expected = Organization {
            name: "test".to_string(),
            validators: vec![0, 1],
        };
        let actual = Organization::shrunken(&org, &shrink_map);
        assert_eq!(expected, actual);
    }

    #[test]
    fn shrink_organizations() {
        let fbas = Fbas::new_generic_unconfigured(43);
        let organizations = Organizations::new(
            vec![
                Organization {
                    name: "test1".to_string(),
                    validators: vec![2, 3, 4],
                },
                Organization {
                    name: "test2".to_string(),
                    validators: vec![23, 42],
                },
            ],
            &fbas,
        );
        let fbas_shrunken = Fbas::new_generic_unconfigured(4);
        let shrink_map: HashMap<NodeId, NodeId> =
            vec![(2, 0), (4, 1), (23, 2), (42, 3)].into_iter().collect();
        let expected = Organizations::new(
            vec![
                Organization {
                    name: "test1".to_string(),
                    validators: vec![0, 1],
                },
                Organization {
                    name: "test2".to_string(),
                    validators: vec![2, 3],
                },
            ],
            &fbas_shrunken,
        );
        let actual = Organizations::shrunken(&organizations, &shrink_map, &fbas_shrunken);
        assert_eq!(expected, actual);
    }

    #[test]
    fn shrink_unshrink_find_minimal_quorums() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let (fbas_shrunken, unshrink_table, _) = Fbas::shrunken(&fbas);

        let expected = find_minimal_quorums(&fbas);
        let actual = unshrink_sets(&find_minimal_quorums(&fbas_shrunken), &unshrink_table);
        assert_eq!(expected, actual);
    }
}
