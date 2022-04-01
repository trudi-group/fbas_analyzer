use super::*;
use itertools::Itertools;

/// Find groups of nodes that can be freely exchanged with one another. We currently don't find all
/// such groups of nodes but only such nodes that are always included in the same
/// validator-containing quorum set. For illustration, in a Stellar-type FBAS the nodes belonging
/// to the same organization are typically symmetric to each other.
pub(crate) fn find_symmetric_nodes_in_node_set(
    nodes: &NodeIdSet,
    fbas: &Fbas,
) -> SymmetricNodesMap {
    let mut referencing_qsets = vec![BTreeSet::new(); fbas.number_of_nodes()];

    for vc_qset in nodes
        .iter()
        .map(|node_id| fbas.nodes[node_id].quorum_set.to_standard_form(node_id))
        .unique()
        .flat_map(|qset| qset.validator_containing_quorum_sets())
        .unique()
    {
        for node_id in vc_qset.contained_nodes().iter() {
            referencing_qsets[node_id].insert(vc_qset.clone());
        }
    }
    let symmetry_qsets = referencing_qsets
        .iter()
        .filter(|qsets| qsets.len() == 1)
        .flatten()
        .filter(|qset| {
            qset.contained_nodes()
                .iter()
                .all(|node_id| referencing_qsets[node_id].len() == 1)
        });
    let mut result = HashMap::new();
    for qset in symmetry_qsets {
        let mut symmetric_nodes = qset.contained_nodes();
        // ensure that we don't count nodes that don't exist
        symmetric_nodes.intersect_with(nodes);
        for node_id in symmetric_nodes.iter() {
            result.insert(node_id, symmetric_nodes.clone());
        }
    }
    SymmetricNodesMap(result)
}

#[derive(Debug, Clone)]
pub(crate) struct SymmetricNodesMap(pub(crate) HashMap<NodeId, NodeIdSet>);
impl SymmetricNodesMap {
    /// If we add nodes to a candidate set one by one, this function helps us ensure that only one
    /// ordering of a group of symmetric nodes is allowed, which helps us to avoid redundant
    /// branches in a few algorithms.
    pub(crate) fn is_non_redundant_next(&self, node: NodeId, previous: &NodeIdSet) -> bool {
        if let Some(symmetric_nodes) = self.0.get(&node) {
            symmetric_nodes
                .difference(previous)
                .next()
                .map(|expected_next| expected_next == node)
                .unwrap_or(false)
        } else {
            true
        }
    }
    pub(crate) fn expand_sets(&self, node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
        debug!("Expanding symmetric nodes...");
        let mut expanded_sets: Vec<NodeIdSet> = vec![];

        for unexpanded_set in node_sets.into_iter() {
            let matching_symmetric_nodes = unexpanded_set
                .iter()
                .filter_map(|node| self.0.get(&node).cloned())
                .unique()
                .collect_vec();

            if matching_symmetric_nodes.is_empty() {
                expanded_sets.push(unexpanded_set);
            } else {
                expanded_sets.append(&mut expand_symmetric_nodes_in_set(
                    unexpanded_set,
                    matching_symmetric_nodes,
                ));
            }
        }
        expanded_sets.sort_unstable();
        expanded_sets
    }
}

fn expand_symmetric_nodes_in_set(
    unexpanded_set: BitSet,
    matching_symmetric_nodes: Vec<NodeIdSet>,
) -> Vec<NodeIdSet> {
    let mut base = unexpanded_set.clone();
    for symmetric_nodes in matching_symmetric_nodes.iter() {
        base.difference_with(symmetric_nodes);
    }
    matching_symmetric_nodes
        .into_iter()
        .map(|nodes| {
            nodes
                .iter()
                .combinations(unexpanded_set.intersection(&nodes).count())
                .collect::<Vec<Vec<NodeId>>>()
        })
        .multi_cartesian_product()
        .map(|expansion_parts| {
            let mut expanded_set = base.clone();
            for node in expansion_parts.into_iter().flatten() {
                expanded_set.insert(node);
            }
            expanded_set
        })
        .collect()
}

impl QuorumSet {
    fn validator_containing_quorum_sets(&self) -> Vec<QuorumSet> {
        if !self.validators.is_empty() || self.inner_quorum_sets.is_empty() {
            vec![self.clone()]
        } else {
            self.inner_quorum_sets
                .iter()
                .flat_map(|qset| qset.validator_containing_quorum_sets())
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symmetric_nodes_in_symmetric_cluster() {
        let symmetric_inner_qsets = vec![
            QuorumSet::new(vec![0, 1, 2], vec![], 2),
            QuorumSet::new(vec![3, 4, 5], vec![], 2),
            QuorumSet::new(vec![6, 7, 8], vec![], 2),
        ];
        let symmetric_qset = QuorumSet::new(vec![], symmetric_inner_qsets, 2);
        let mut fbas = Fbas::new();
        for _ in 0..9 {
            fbas.add_generic_node(symmetric_qset.clone());
        }
        let actual = find_symmetric_nodes_in_node_set(&fbas.all_nodes(), &fbas)
            .0
            .get(&0)
            .cloned();
        let expected = Some(bitset![0, 1, 2]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn symmetric_nodes_in_not_quite_symmetric_cluster() {
        let symmetric_inner_qsets = vec![
            QuorumSet::new(vec![0, 1, 2], vec![], 2),
            QuorumSet::new(vec![3, 4, 5], vec![], 2),
            QuorumSet::new(vec![6, 7, 8], vec![], 2),
        ];
        let symmetric_qset = QuorumSet::new(vec![], symmetric_inner_qsets, 2);
        let mut fbas = Fbas::new();
        for _ in 0..9 {
            fbas.add_generic_node(symmetric_qset.clone());
        }
        // modify an inner quorum set
        fbas.nodes[8].quorum_set.inner_quorum_sets[0].validators = vec![0, 1];
        let actual = find_symmetric_nodes_in_node_set(&fbas.all_nodes(), &fbas)
            .0
            .get(&0)
            .cloned();
        let expected = None;
        assert_eq!(expected, actual);
    }

    #[test]
    fn expand_symmetric_nodes_in_simple_set() {
        let set = bitset![0, 1, 2, 4];
        let matching_nodes = vec![bitset![2, 3]];
        let actual = expand_symmetric_nodes_in_set(set, matching_nodes);
        let expected = bitsetvec![[0, 1, 2, 4], [0, 1, 3, 4]];
        assert_eq!(expected, actual);
    }

    #[test]
    fn expand_symmetric_nodes_in_complex_set() {
        let set = bitset![0, 1, 2, 4, 5];
        let matching_nodes = vec![bitset![2, 3], bitset![4, 5, 6]];
        let actual = expand_symmetric_nodes_in_set(set, matching_nodes);
        let expected = bitsetvec![
            [0, 1, 2, 4, 5],
            [0, 1, 2, 4, 6],
            [0, 1, 2, 5, 6],
            [0, 1, 3, 4, 5],
            [0, 1, 3, 4, 6],
            [0, 1, 3, 5, 6]
        ];
        assert_eq!(expected, actual);
    }
}
