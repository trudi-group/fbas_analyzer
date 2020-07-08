use super::*;
use itertools::Itertools;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuorumSet {
    pub threshold: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub validators: Vec<NodeId>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inner_quorum_sets: Vec<QuorumSet>,
}
impl QuorumSet {
    pub fn new() -> Self {
        QuorumSet {
            threshold: 0,
            validators: vec![],
            inner_quorum_sets: vec![],
        }
    }
    pub fn contained_nodes(&self) -> NodeIdSet {
        let mut nodes: NodeIdSet = self.validators.iter().cloned().collect();
        for inner_quorum_set in self.inner_quorum_sets.iter() {
            nodes.union_with(&inner_quorum_set.contained_nodes());
        }
        nodes
    }
    pub fn is_quorum_slice(&self, node_set: &NodeIdSet) -> bool {
        if self.threshold == 0 {
            false // badly configured quorum set
        } else {
            let found_validator_matches = self
                .validators
                .iter()
                .filter(|x| node_set.contains(**x))
                .take(self.threshold)
                .count();
            let found_inner_quorum_set_matches = self
                .inner_quorum_sets
                .iter()
                .filter(|x| x.is_quorum_slice(node_set))
                .take(self.threshold - found_validator_matches)
                .count();

            found_validator_matches + found_inner_quorum_set_matches == self.threshold
        }
    }
    /// Each valid quorum slice for this quorum set is a superset (i.e., equal to or a proper superset of)
    /// of at least one of the sets returned by this function.
    pub fn to_quorum_slices(&self) -> Vec<NodeIdSet> {
        let mut subslice_groups: Vec<Vec<NodeIdSet>> = vec![];
        subslice_groups.extend(
            self.validators
                .iter()
                .map(|&node_id| vec![bitset![node_id]]),
        );
        subslice_groups.extend(
            self.inner_quorum_sets
                .iter()
                .map(|qset| qset.to_quorum_slices()),
        );
        subslice_groups
            .into_iter()
            .combinations(self.threshold)
            .map(|group_combination| {
                group_combination
                    .into_iter()
                    .map(|subslice_group| subslice_group.into_iter())
                    .multi_cartesian_product()
                    .map(|subslice_combination| {
                        let mut slice = bitset![];
                        for node_set in subslice_combination.into_iter() {
                            slice.union_with(&node_set);
                        }
                        slice
                    })
                    .collect()
            })
            .concat()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flat_qset(validators: &[NodeId], threshold: usize) -> QuorumSet {
        QuorumSet {
            threshold,
            validators: validators.iter().copied().collect(),
            inner_quorum_sets: vec![],
        }
    }

    #[test]
    fn is_quorum_slice_if_not_quorum_slice() {
        let quorum_set = flat_qset(&[0, 1, 2], 3);
        let node_set = bitset![1, 2, 3];
        assert!(!quorum_set.is_quorum_slice(&node_set));
    }

    #[test]
    fn is_quorum_if_quorum() {
        let quorum_set = flat_qset(&[0, 1, 2], 2);
        let node_set = bitset![1, 2, 3];
        assert!(quorum_set.is_quorum_slice(&node_set));
    }

    #[test]
    fn is_quorum_slice_with_inner_quorum_sets() {
        let mut quorum_set = flat_qset(&[0, 1], 3);
        quorum_set.inner_quorum_sets = vec![
            QuorumSet {
                threshold: 2,
                validators: vec![2, 3, 4],
                inner_quorum_sets: vec![],
            },
            QuorumSet {
                threshold: 2,
                validators: vec![4, 5, 6],
                inner_quorum_sets: vec![],
            },
        ];
        let not_quorum = bitset![1, 2, 3];
        let quorum = bitset![0, 3, 4, 5];
        assert!(!quorum_set.is_quorum_slice(&not_quorum));
        assert!(quorum_set.is_quorum_slice(&quorum));
    }

    #[test]
    fn empty_set_is_not_quorum_slice() {
        let quorum_set = flat_qset(&[0, 1, 2], 2);
        assert!(!quorum_set.is_quorum_slice(&bitset![]));
    }

    #[test]
    fn quorum_set_with_threshold_0_trusts_no_one() {
        let quorum_set = flat_qset(&[0, 1, 2], 0);
        assert!(!quorum_set.is_quorum_slice(&bitset![]));
        assert!(!quorum_set.is_quorum_slice(&bitset![0]));
        assert!(!quorum_set.is_quorum_slice(&bitset![0, 1]));
        assert!(!quorum_set.is_quorum_slice(&bitset![0, 1, 2]));
    }

    #[test]
    fn flast_quorum_set_to_quorum_slices() {
        let quorum_set = flat_qset(&[0, 1, 2], 1);
        let expected = bitsetvec![[0], [1], [2]];
        let actual = quorum_set.to_quorum_slices();
        assert_eq!(expected, actual);
    }

    #[test]
    fn nested_quorum_set_to_quorum_slices() {
        let quorum_set = QuorumSet {
            threshold: 3,
            validators: vec![0, 1],
            inner_quorum_sets: vec![
                QuorumSet {
                    threshold: 1,
                    validators: vec![2, 3],
                    inner_quorum_sets: vec![],
                },
                QuorumSet {
                    threshold: 3,
                    validators: vec![3, 4],
                    inner_quorum_sets: vec![QuorumSet {
                        threshold: 1,
                        validators: vec![5],
                        inner_quorum_sets: vec![],
                    }],
                },
            ],
        };
        let expected = bitsetvec![
            [0, 1, 2],
            [0, 1, 3],
            [0, 1, 3, 4, 5],
            [0, 2, 3, 4, 5],
            [0, 3, 4, 5],
            [1, 2, 3, 4, 5],
            [1, 3, 4, 5]
        ];
        let actual = quorum_set.to_quorum_slices();
        assert_eq!(expected, actual);
    }
}
