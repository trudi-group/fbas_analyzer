//! Shrinking means reducing the node ID space, which results in smaller bit sets, which results in
//! faster computation and a vastly lower memory footprint.

use super::*;

#[derive(Debug)]
pub struct ShrinkManager {
    unshrink_table: Vec<NodeId>,
    shrink_map: HashMap<NodeId, NodeId>,
}
impl ShrinkManager {
    pub fn new(ids_to_keep: NodeIdSet) -> Self {
        let shrink_map: HashMap<NodeId, NodeId> = ids_to_keep
            .iter()
            .enumerate()
            .map(|(new, old)| (old, new))
            .collect();
        let unshrink_table: Vec<NodeId> = ids_to_keep.into_iter().collect();
        ShrinkManager {
            unshrink_table,
            shrink_map,
        }
    }
    pub fn shrink_set(&self, node_set: &NodeIdSet) -> NodeIdSet {
        shrink_set(node_set, &self.shrink_map)
    }
    pub fn shrink_sets(&self, node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
        shrink_sets(node_sets, &self.shrink_map)
    }
    pub fn unshrink_set(&self, node_set: &NodeIdSet) -> NodeIdSet {
        unshrink_set(node_set, &self.unshrink_table)
    }
    pub fn unshrink_sets(&self, node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
        unshrink_sets(node_sets, &self.unshrink_table)
    }
    pub fn reshrink_sets(
        &self,
        node_sets: &[NodeIdSet],
        old_shrink_manager: &ShrinkManager,
    ) -> Vec<NodeIdSet> {
        let reshrink_map: HashMap<NodeId, NodeId> = old_shrink_manager
            .unshrink_table
            .iter()
            .enumerate()
            .filter_map(|(current_id, original_id)| {
                self.shrink_map
                    .get(original_id)
                    .map(|&new_id| (current_id, new_id))
            })
            .collect();
        shrink_sets(node_sets, &reshrink_map)
    }
    pub fn unshrink_table<'a>(&'a self) -> &'a Vec<NodeId> {
        &self.unshrink_table
    }
}

pub fn shrink_set(node_set: &NodeIdSet, shrink_map: &HashMap<NodeId, NodeId>) -> NodeIdSet {
    node_set
        .iter()
        .map(|id| shrink_map.get(&id).unwrap())
        .cloned()
        .collect()
}

pub fn shrink_sets(
    node_sets: &[NodeIdSet],
    shrink_map: &HashMap<NodeId, NodeId>,
) -> Vec<NodeIdSet> {
    node_sets
        .iter()
        .map(|node_set| shrink_set(node_set, shrink_map))
        .collect()
}

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
    pub fn shrunken(&self, ids_to_keep: NodeIdSet) -> (Self, ShrinkManager) {
        let shrink_manager = ShrinkManager::new(ids_to_keep);
        let unshrink_table = &shrink_manager.unshrink_table;
        let shrink_map = &shrink_manager.shrink_map;

        let mut fbas_shrunken = Fbas::new_generic_unconfigured(unshrink_table.len());
        for old_id in 0..self.nodes.len() {
            if let Some(&new_id) = shrink_map.get(&old_id) {
                fbas_shrunken.nodes[new_id] = Node::shrunken(&self.nodes[old_id], &shrink_map);
            }
        }
        (fbas_shrunken, shrink_manager)
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
        shrink_manager: &ShrinkManager,
        shrunken_fbas: &'fbas Fbas,
    ) -> Self {
        let organizations = orgs
            .organizations
            .iter()
            .map(|org| Organization::shrunken(org, &shrink_manager.shrink_map))
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
    fn shrunken_fbas_has_correct_size() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let reduce_to = bitset![0, 23, 42];
        let (fbas_shrunken, _) = Fbas::shrunken(&fbas, reduce_to);
        let expected = 3;
        let actual = fbas_shrunken.number_of_nodes();
        assert_eq!(expected, actual);
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
        let shrink_manager = ShrinkManager {
            unshrink_table: vec![],
            shrink_map: vec![(2, 0), (4, 1), (23, 2), (42, 3)].into_iter().collect(),
        };
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
        let actual = Organizations::shrunken(&organizations, &shrink_manager, &fbas_shrunken);
        assert_eq!(expected, actual);
    }

    #[test]
    fn shrink_unshrink_find_minimal_quorums() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let (fbas_shrunken, shrink_manager) = Fbas::shrunken(&fbas, fbas.relevant_nodes());

        let expected = find_minimal_quorums(&fbas);
        let actual = shrink_manager.unshrink_sets(&find_minimal_quorums(&fbas_shrunken));
        assert_eq!(expected, actual);
    }

    #[test]
    fn shrink_set_shrinks_ids() {
        let set = bitset![2, 4, 42];
        let shrink_map: HashMap<NodeId, NodeId> =
            vec![(2, 0), (4, 1), (23, 2), (42, 3)].into_iter().collect();
        let expected = bitset![0, 1, 3];
        let actual = shrink_set(&set, &shrink_map);
        assert_eq!(expected, actual);
    }

    #[test]
    #[should_panic]
    fn shrink_set_panics_if_cant_map_id() {
        let set = bitset![2, 3, 4];
        let shrink_map: HashMap<NodeId, NodeId> =
            vec![(2, 0), (4, 1), (23, 2), (42, 3)].into_iter().collect();
        shrink_set(&set, &shrink_map);
    }

    #[test]
    fn shrink_sets_shrinks_ids() {
        let sets = vec![bitset![2, 4, 23], bitset![23, 42, 404]];
        let shrink_map: HashMap<NodeId, NodeId> = vec![(2, 0), (4, 1), (23, 2), (42, 3), (404, 4)]
            .into_iter()
            .collect();
        let expected = vec![bitset![0, 1, 2], bitset![2, 3, 4]];
        let actual = shrink_sets(&sets, &shrink_map);
        assert_eq!(expected, actual);
    }

    #[test]
    fn reshrink_sets_reencodes_sets() {
        let sets = vec![bitset![0, 1, 2], bitset![2, 3, 4]];
        let old_shrink_manager = ShrinkManager {
            unshrink_table: vec![2, 4, 23, 42, 404],
            shrink_map: HashMap::new(),
        };
        let new_shrink_manager = ShrinkManager {
            unshrink_table: vec![],
            shrink_map: vec![(2, 0), (4, 1), (23, 2), (42, 3), (99, 4), (404, 5)]
                .into_iter()
                .collect(),
        };
        let expected = vec![bitset![0, 1, 2], bitset![2, 3, 5]];
        let actual = new_shrink_manager.reshrink_sets(&sets, &old_shrink_manager);
        assert_eq!(expected, actual);
    }
}
