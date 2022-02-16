use super::*;
use itertools::Itertools;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuorumSet {
    pub threshold: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub validators: Vec<NodeId>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inner_quorum_sets: Vec<QuorumSet>,
}
impl QuorumSet {
    pub fn new(
        validators: Vec<NodeId>,
        inner_quorum_sets: Vec<QuorumSet>,
        threshold: usize,
    ) -> Self {
        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
    }
    /// A quorum set tho basically mark a node as broken.
    pub fn new_unsatisfiable() -> Self {
        Self::new(vec![], vec![], 1)
    }
    /// A quorum set that is always satisfiable and induces a one-node quorum.
    pub fn new_empty() -> Self {
        Self::new(vec![], vec![], 0)
    }
    pub fn contained_nodes(&self) -> NodeIdSet {
        self.contained_nodes_with_duplicates().into_iter().collect()
    }
    /// Whether some nodes appear more than once
    pub fn contains_duplicates(&self) -> bool {
        let nodes_vec = self.contained_nodes_with_duplicates();
        let nodes_set: NodeIdSet = nodes_vec.iter().copied().collect();
        nodes_vec.len() != nodes_set.len()
    }
    pub fn is_satisfiable(&self) -> bool {
        self.validators.len() + self.inner_quorum_sets.len() >= self.threshold
    }
    pub fn is_quorum_slice(&self, node_set: &NodeIdSet) -> bool {
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
    /// Each valid quorum slice for this quorum set is a superset (i.e., equal to or a proper superset of)
    /// of at least one of the sets returned by this function. The slices returned here are not
    /// necessarily minimal!
    pub fn to_quorum_slices(&self) -> Vec<NodeIdSet> {
        if self.threshold == 0 {
            return vec![bitset![]];
        }
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
    fn contained_nodes_with_duplicates(&self) -> Vec<NodeId> {
        let mut nodes = self.validators.clone();
        for inner_quorum_set in self.inner_quorum_sets.iter() {
            nodes.append(&mut inner_quorum_set.contained_nodes_with_duplicates());
        }
        nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flat_qset(validators: &[NodeId], threshold: usize) -> QuorumSet {
        QuorumSet::new(validators.iter().copied().collect(), vec![], threshold)
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
    fn empty_set_is_not_quorum_slice_of_nonempty_quorum_set() {
        let quorum_set = flat_qset(&[0, 1, 2], 2);
        assert!(!quorum_set.is_quorum_slice(&bitset![]));
    }

    #[test]
    fn empty_set_is_not_quorum_slice_of_unsatisfiable_quorum_set() {
        let quorum_set = QuorumSet::new_unsatisfiable();
        assert!(!quorum_set.is_quorum_slice(&bitset![]));
    }

    #[test]
    fn empty_set_is_quorum_slice_of_empty_quorum_set() {
        let quorum_set = QuorumSet::new_empty();
        assert!(quorum_set.is_quorum_slice(&bitset![]));
    }

    #[test]
    fn quorum_set_with_threshold_0_matches_always() {
        let quorum_set = flat_qset(&[0, 1, 2], 0);
        assert!(quorum_set.is_quorum_slice(&bitset![]));
        assert!(quorum_set.is_quorum_slice(&bitset![0]));
        assert!(quorum_set.is_quorum_slice(&bitset![0, 1]));
        assert!(quorum_set.is_quorum_slice(&bitset![0, 1, 2]));
    }

    #[test]
    fn flat_quorum_set_to_quorum_slices() {
        let quorum_set = flat_qset(&[0, 1, 2], 1);
        let expected = bitsetvec![[0], [1], [2]];
        let actual = quorum_set.to_quorum_slices();
        assert_eq!(expected, actual);
    }

    #[test]
    fn unsatisfiable_quorum_set_to_quorum_slices() {
        let quorum_set = QuorumSet::new_unsatisfiable();
        let expected: Vec<NodeIdSet> = bitsetvec![];
        let actual = quorum_set.to_quorum_slices();
        assert_eq!(expected, actual);
    }

    #[test]
    fn empty_quorum_set_to_quorum_slices() {
        let quorum_set = QuorumSet::new_empty();
        let expected = bitsetvec![{}];
        let actual = quorum_set.to_quorum_slices();
        assert_eq!(expected, actual);
    }

    #[test]
    fn nested_quorum_set_to_quorum_slices() {
        let quorum_set = QuorumSet {
            threshold: 4,
            validators: vec![0, 1],
            inner_quorum_sets: vec![
                QuorumSet {
                    threshold: 0,
                    validators: vec![7, 8],
                    inner_quorum_sets: vec![],
                },
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
            [0, 1, 2, 3, 4, 5],
            [0, 1, 3, 4, 5],
            [0, 2, 3, 4, 5],
            [0, 3, 4, 5],
            [1, 2, 3, 4, 5],
            [1, 3, 4, 5]
        ];
        let actual = quorum_set.to_quorum_slices();
        assert_eq!(expected, actual);
    }

    #[test]
    fn weird_nested_quorum_set_to_quorum_slices() {
        let quorum_set = QuorumSet {
            threshold: 4,
            validators: vec![],
            inner_quorum_sets: vec![
                QuorumSet {
                    threshold: 0,
                    validators: vec![25, 39],
                    inner_quorum_sets: vec![],
                },
                QuorumSet {
                    threshold: 1,
                    validators: vec![4, 27],
                    inner_quorum_sets: vec![],
                },
                QuorumSet {
                    threshold: 1,
                    validators: vec![15, 74],
                    inner_quorum_sets: vec![],
                },
                QuorumSet {
                    threshold: 2,
                    validators: vec![11, 31, 71],
                    inner_quorum_sets: vec![],
                },
                QuorumSet {
                    threshold: 2,
                    validators: vec![12, 48, 70],
                    inner_quorum_sets: vec![],
                },
            ],
        };
        let miss_problem = bitset! {4, 11, 12, 15, 25, 31};
        assert!(quorum_set.is_quorum_slice(&miss_problem));
        assert!(quorum_set
            .to_quorum_slices()
            .iter()
            .find(|&x| x.is_subset(&miss_problem))
            .is_some());
    }

    #[test]
    fn duplicate_validators() {
        let quorum_set = QuorumSet {
            threshold: 2,
            validators: vec![0, 1],
            inner_quorum_sets: vec![QuorumSet {
                threshold: 1,
                validators: vec![1, 3],
                inner_quorum_sets: vec![],
            }],
        };
        assert!(quorum_set.contains_duplicates());
    }

    #[test]
    fn no_duplicate_validators() {
        let quorum_set = QuorumSet {
            threshold: 2,
            validators: vec![0, 1],
            inner_quorum_sets: vec![QuorumSet {
                threshold: 1,
                validators: vec![2, 3],
                inner_quorum_sets: vec![],
            }],
        };
        assert!(!quorum_set.contains_duplicates());
    }
}
