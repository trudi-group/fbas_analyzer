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
    /// A quorum set that basically marks a node as broken. It is never satisfied and can therefore
    /// never be part of a quorum.
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
        self.contained_nodes_with_duplicates()
            .into_iter()
            .duplicates()
            .next()
            .is_some()
    }
    pub fn is_satisfiable(&self) -> bool {
        self.validators.len() + self.inner_quorum_sets.len() >= self.threshold
    }
    pub fn is_quorum_slice(&self, node_set: &NodeIdSet) -> bool {
        self.is_slice(node_set, |qset| qset.threshold)
    }
    /// Each valid quorum slice for this quorum set is a superset (i.e., equal to or a proper superset of)
    /// of at least one of the sets returned by this function. The slices returned here are not
    /// necessarily minimal! Also: The returned slices are not (yet) valid quorum slices for a
    /// specific *node*; for that we would need to make sure that that the node itself is included
    /// in the slices (e.g., by inserting it into each slice).
    pub fn to_quorum_slices(&self) -> Vec<NodeIdSet> {
        self.to_slices(|qset| qset.threshold)
    }
    /// Returns some pair of nonintersecting slices if there are any, `None` otherwise.
    pub fn has_nonintersecting_quorum_slices(&self) -> Option<(NodeIdSet, NodeIdSet)> {
        if self.threshold == 0 {
            Some((bitset! {}, bitset! {}))
        } else if self.contained_nodes().len() < self.contained_nodes_with_duplicates().len() {
            self.has_nonintersecting_quorum_slices_if_duplicates()
        } else {
            self.has_nonintersecting_quorum_slices_if_no_duplicates()
        }
    }
    pub(crate) fn is_slice(
        &self,
        node_set: &NodeIdSet,
        relevant_threshold: impl Fn(&QuorumSet) -> usize,
    ) -> bool {
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
            .take(relevant_threshold(self) - found_validator_matches)
            .count();

        found_validator_matches + found_inner_quorum_set_matches == relevant_threshold(self)
    }
    pub(crate) fn to_slices(
        &self,
        relevant_threshold: impl Copy + Fn(&QuorumSet) -> usize,
    ) -> Vec<NodeIdSet> {
        if relevant_threshold(self) == 0 {
            vec![bitset![]]
        } else {
            self.nonempty_slices_iter(relevant_threshold).collect()
        }
    }
    pub(crate) fn nonempty_slices_iter<'a>(
        &'a self,
        relevant_threshold: impl Copy + Fn(&QuorumSet) -> usize + 'a,
    ) -> impl Iterator<Item = NodeIdSet> + '_ {
        self.to_subslice_groups(relevant_threshold)
            .combinations(relevant_threshold(self))
            .flat_map(|group_combination| {
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
            })
    }
    fn to_subslice_groups<'a>(
        &'a self,
        relevant_threshold: impl Copy + Fn(&QuorumSet) -> usize + 'a,
    ) -> impl Iterator<Item = Vec<NodeIdSet>> + 'a {
        self.validators
            .iter()
            .map(|&node_id| vec![bitset![node_id]])
            .chain(
                self.inner_quorum_sets
                    .iter()
                    .map(move |qset| qset.to_slices(relevant_threshold)),
            )
    }
    fn contained_nodes_with_duplicates(&self) -> Vec<NodeId> {
        self.validators
            .iter()
            .copied()
            .chain(
                self.inner_quorum_sets
                    .iter()
                    .flat_map(|inner_qset| inner_qset.contained_nodes_with_duplicates()),
            )
            .collect()
    }
    fn has_nonintersecting_quorum_slices_if_duplicates(&self) -> Option<(NodeIdSet, NodeIdSet)> {
        let mut tester = NodeIdSet::new();
        let contained_unique_nodes = self.contained_nodes();
        self.nonempty_slices_iter(|qset| qset.threshold)
            .find_map(|slice| {
                if slice.len() < contained_unique_nodes.len() / 2 {
                    tester.union_with(&contained_unique_nodes);
                    tester.difference_with(&slice);
                    if self.is_quorum_slice(&tester) {
                        Some((slice, tester.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
    }
    fn has_nonintersecting_quorum_slices_if_no_duplicates(&self) -> Option<(NodeIdSet, NodeIdSet)> {
        let mut slices = [bitset![], bitset![]];
        let mut i = 0;
        for &validator in self.validators.iter().take(2 * self.threshold) {
            slices[i % 2].insert(validator);
            i += 1;
        }
        for inner_qset in self.inner_quorum_sets.iter() {
            if i >= 2 * self.threshold {
                break;
            } else if let Some(subslices) =
                inner_qset.has_nonintersecting_quorum_slices_if_no_duplicates()
            {
                slices[i % 2].union_with(&subslices.0);
                slices[(i + 1) % 2].union_with(&subslices.1);
                i += 2;
            } else if let Some(subslice) = inner_qset
                .nonempty_slices_iter(|qset| qset.threshold)
                .next()
            {
                slices[i % 2].union_with(&subslice);
                i += 1;
            }
        }
        if i == 2 * self.threshold {
            let [slice1, slice2] = slices;
            debug_assert!(slice1.is_disjoint(&slice2));
            debug_assert!(self.is_quorum_slice(&slice1));
            debug_assert!(self.is_quorum_slice(&slice2));
            Some((slice1, slice2))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flat_qset(validators: &[NodeId], threshold: usize) -> QuorumSet {
        QuorumSet::new(validators.to_vec(), vec![], threshold)
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
            .any(|x| x.is_subset(&miss_problem)));
    }

    #[test]
    fn nested_quorum_set_has_nonintersecting_quorum_slices() {
        let quorum_set = QuorumSet {
            threshold: 3,
            validators: vec![],
            inner_quorum_sets: vec![
                QuorumSet {
                    threshold: 1,
                    validators: vec![0, 1],
                    inner_quorum_sets: vec![],
                },
                QuorumSet {
                    threshold: 1,
                    validators: vec![2, 3],
                    inner_quorum_sets: vec![],
                },
                QuorumSet {
                    threshold: 1,
                    validators: vec![4, 6],
                    inner_quorum_sets: vec![],
                },
            ],
        };
        let result = quorum_set.has_nonintersecting_quorum_slices();
        assert!(result.is_some());
        let (slice_1, slice_2) = result.unwrap();
        assert!(quorum_set.is_quorum_slice(&slice_1));
        assert!(quorum_set.is_quorum_slice(&slice_2));
        assert!(slice_1.is_disjoint(&slice_2));
    }

    #[test]
    fn nested_quorum_set_with_duplicates_has_no_nonintersecting_quorum_slices() {
        let quorum_set = QuorumSet {
            threshold: 3,
            validators: vec![],
            inner_quorum_sets: vec![
                QuorumSet {
                    threshold: 1,
                    validators: vec![0, 1],
                    inner_quorum_sets: vec![],
                },
                QuorumSet {
                    threshold: 1,
                    validators: vec![1, 2],
                    inner_quorum_sets: vec![],
                },
                QuorumSet {
                    threshold: 1,
                    validators: vec![2, 3],
                    inner_quorum_sets: vec![],
                },
            ],
        };
        let result = quorum_set.has_nonintersecting_quorum_slices();
        assert!(result.is_none());
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
