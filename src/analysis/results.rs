use super::*;

/// Wraps a node ID set.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct NodeIdSetResult {
    pub(crate) node_set: NodeIdSet,
}
impl NodeIdSetResult {
    pub(crate) fn new(node_set: NodeIdSet, shrink_manager: Option<&ShrinkManager>) -> Self {
        if let Some(shrink_manager) = shrink_manager {
            NodeIdSetResult {
                node_set: shrink_manager.unshrink_set(&node_set),
            }
        } else {
            NodeIdSetResult { node_set }
        }
    }
    pub fn unwrap(self) -> NodeIdSet {
        self.node_set
    }
    pub fn into_vec(self) -> Vec<NodeId> {
        self.unwrap().into_iter().collect()
    }
    pub fn involved_nodes(&self) -> NodeIdSet {
        self.node_set.clone()
    }
    pub fn len(&self) -> usize {
        self.node_set.len()
    }
    pub fn is_empty(&self) -> bool {
        self.node_set.is_empty()
    }
    pub fn without_nodes(&self, nodes: &[NodeId]) -> Self {
        let mut new = self.clone();
        for node in nodes.iter().copied() {
            new.node_set.remove(node);
        }
        new
    }
    pub fn without_nodes_pretty(
        &self,
        nodes: &[PublicKey],
        fbas: &Fbas,
        groupings: Option<&Groupings>,
    ) -> Self {
        let nodes_by_id = if let Some(orgs) = groupings {
            from_grouping_names(nodes, fbas, orgs)
        } else {
            from_public_keys(nodes, fbas)
        };
        self.without_nodes(&nodes_by_id)
    }
    /// Merge contained nodes so that all nodes of the same grouping get the same ID.
    pub fn merged_by_group(&self, groupings: &Groupings) -> Self {
        Self {
            node_set: groupings.merge_node_set(self.node_set.clone()),
        }
    }
}
impl From<NodeIdSet> for NodeIdSetResult {
    fn from(set: NodeIdSet) -> Self {
        Self::new(set, None)
    }
}

/// Wraps a vector of node ID sets. Node ID sets are stored in shrunken form to preserve memory.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct NodeIdSetVecResult {
    pub(crate) node_sets: Vec<NodeIdSet>,
    pub(crate) unshrink_table: Option<Vec<NodeId>>,
}
impl NodeIdSetVecResult {
    pub(crate) fn new(node_sets: Vec<NodeIdSet>, shrink_manager: Option<&ShrinkManager>) -> Self {
        NodeIdSetVecResult {
            node_sets,
            unshrink_table: shrink_manager.map(|m| m.unshrink_table().clone()),
        }
    }
    pub fn unwrap(mut self) -> Vec<NodeIdSet> {
        self.unshrink();
        self.node_sets
    }
    pub fn into_vec_vec(mut self) -> Vec<Vec<NodeId>> {
        self.unshrink();
        self.node_sets
            .iter()
            .map(|node_set| node_set.into_iter().collect())
            .collect()
    }
    pub fn involved_nodes(&self) -> NodeIdSet {
        involved_nodes(&self.node_sets)
    }
    pub fn len(&self) -> usize {
        self.node_sets.len()
    }
    pub fn is_empty(&self) -> bool {
        self.node_sets.is_empty()
    }
    pub fn contains_empty_set(&self) -> bool {
        self.node_sets.contains(&bitset![])
    }
    /// Returns (number_of_sets, number_of_distinct_nodes, <minmaxmean_set_size>, <histogram>)
    pub fn describe(&self) -> (usize, usize, (usize, usize, f64), Vec<usize>) {
        (
            self.node_sets.len(),
            self.involved_nodes().len(),
            self.minmaxmean(),
            self.histogram(),
        )
    }
    /// Returns (min_set_size, max_set_size, mean_set_size)
    pub fn minmaxmean(&self) -> (usize, usize, f64) {
        (self.min(), self.max(), self.mean())
    }
    /// Returns the cardinality of the smallest member set
    pub fn min(&self) -> usize {
        self.node_sets.iter().map(|s| s.len()).min().unwrap_or(0)
    }
    /// Returns the cardinality of the largest member set
    pub fn max(&self) -> usize {
        self.node_sets.iter().map(|s| s.len()).max().unwrap_or(0)
    }
    /// Returns the mean cardinality of all member sets
    pub fn mean(&self) -> f64 {
        if self.node_sets.is_empty() {
            0.0
        } else {
            self.node_sets.iter().map(|s| s.len()).sum::<usize>() as f64
                / (self.node_sets.len() as f64)
        }
    }
    /// Returns [ #members with size 0, #members with size 1, ... , #members with maximum size ]
    pub fn histogram(&self) -> Vec<usize> {
        let max = self.node_sets.iter().map(|s| s.len()).max().unwrap_or(0);
        let mut histogram: Vec<usize> = vec![0; max + 1];
        for node_set in self.node_sets.iter() {
            let size = node_set.len();
            histogram[size] = histogram[size].checked_add(1).unwrap();
        }
        histogram
    }
    /// Merge contained nodes so that all nodes of the same grouping get the same ID.
    /// The remaining node sets might be non-minimal w.r.t. each other, or contain duplicates!
    /// You will usually want to chain this with `.minimal_sets()`.
    pub fn merged_by_group(&self, groupings: &Groupings) -> Self {
        let mut new = self.clone();
        new.unshrink();
        new.node_sets = groupings.merge_node_sets(new.node_sets);
        new
    }
    /// Removes all non-minimal sets and sorts the remaining sets.
    pub fn minimal_sets(&self) -> Self {
        let mut new = self.clone();
        new.node_sets = remove_non_minimal_node_sets(new.node_sets);
        new
    }
    pub fn without_nodes(&self, nodes: &[NodeId]) -> Self {
        let mut new = self.clone();
        new.unshrink();
        let nodes: NodeIdSet = nodes.iter().copied().collect();
        for node_set in new.node_sets.iter_mut() {
            node_set.difference_with(&nodes);
        }
        new
    }
    pub fn without_nodes_pretty(
        &self,
        nodes: &[PublicKey],
        fbas: &Fbas,
        groupings: Option<&Groupings>,
    ) -> Self {
        let nodes_by_id = if let Some(orgs) = groupings {
            from_grouping_names(nodes, fbas, orgs)
        } else {
            from_public_keys(nodes, fbas)
        };
        self.without_nodes(&nodes_by_id)
    }
    fn unshrink(&mut self) {
        if let Some(unshrink_table) = &self.unshrink_table {
            self.node_sets = unshrink_sets(&self.node_sets, unshrink_table);
        }
        self.unshrink_table = None;
    }
}
impl From<Vec<NodeIdSet>> for NodeIdSetVecResult {
    fn from(sets: Vec<NodeIdSet>) -> Self {
        Self::new(sets, None)
    }
}

fn from_public_keys(nodes: &[PublicKey], fbas: &Fbas) -> Vec<NodeId> {
    nodes.iter().filter_map(|pk| fbas.get_node_id(pk)).collect()
}
fn from_grouping_names(nodes: &[PublicKey], fbas: &Fbas, groupings: &Groupings) -> Vec<NodeId> {
    nodes
        .iter()
        .map(|name| match groupings.get_by_name(name) {
            Some(org) => org.validators.clone(),
            None => {
                if let Some(node_id) = fbas.get_node_id(name) {
                    vec![node_id]
                } else {
                    vec![]
                }
            }
        })
        .flatten()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_sets_histogram() {
        let node_sets_result = NodeIdSetVecResult::new(
            vec![
                bitset![0, 1],
                bitset![2, 3],
                bitset![4, 5, 6, 7],
                bitset![1, 4],
            ],
            None,
        );
        let actual = node_sets_result.histogram();
        let expected = vec![0, 0, 3, 0, 1];
        assert_eq!(expected, actual)
    }

    #[test]
    fn node_sets_describe() {
        let node_sets_result = NodeIdSetVecResult::new(
            vec![
                bitset![0, 1],
                bitset![2, 3],
                bitset![4, 5, 6, 7],
                bitset![1, 4],
            ],
            None,
        );
        let actual = node_sets_result.describe();
        let expected = (4, 8, (2, 4, 2.5), vec![0, 0, 3, 0, 1]);
        assert_eq!(expected, actual)
    }

    #[test]
    fn remove_nodes_from_unshrunken_node_set_result() {
        let result = NodeIdSetResult::new(bitset![0, 1], None);
        let expected = bitset![0];
        let actual = result.without_nodes(&[1]).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn remove_nodes_from_shrunken_result() {
        let shrink_manager = ShrinkManager::new(bitset![23, 42]);
        let result = NodeIdSetResult::new(bitset![0, 1], Some(&shrink_manager));
        let expected = bitset![42];
        let actual = result.without_nodes(&[23]).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn remove_nodes_from_unshrunken_vec_result() {
        let result = NodeIdSetVecResult::new(bitsetvec![{0, 1}, {0, 2}, {3}], None);
        let expected = bitsetvec![{0, 1}, {0}, {}];
        let actual = result.without_nodes(&[2, 3]).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn remove_nodes_from_shrunken_vec_result() {
        let shrink_manager = ShrinkManager::new(bitset![23, 42, 7, 1000]);
        let result =
            NodeIdSetVecResult::new(bitsetvec![{0, 1}, {0, 2}, {3}], Some(&shrink_manager));
        let expected = bitsetvec![{7, 23}, {7}, {}];
        let actual = result.without_nodes(&[42, 1000]).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn remove_nodes_by_pretty_name() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "Alex"
            },
            {
                "publicKey": "Bob"
            }
            ]"#,
        );
        let result = NodeIdSetVecResult::new(bitsetvec![{0, 1}, {3}], None);
        let expected = bitsetvec![{ 0 }, { 3 }];
        let actual = result
            .without_nodes_pretty(
                &[String::from("Bob"), String::from("Helen the non-existent")],
                &fbas,
                None,
            )
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn remove_nodes_by_pretty_name_and_org() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "Jim"
            },
            {
                "publicKey": "Jon"
            },
            {
                "publicKey": "Alex"
            },
            {
                "publicKey": "Bob"
            }
            ]"#,
        );
        let organizations = Groupings::organizations_from_json_str(
            r#"[
            {
                "name": "J Mafia",
                "validators": [ "Jim", "Jon" ]
            }
            ]"#,
            &fbas,
        );
        let result = NodeIdSetVecResult::new(bitsetvec![{0, 1}, {0, 2}, {3}], None);
        let expected = bitsetvec![{}, { 2 }, {}];
        let actual = result
            .without_nodes_pretty(
                &[
                    String::from("J Mafia"),
                    String::from("Bob"),
                    String::from("Helen the non-existent"),
                ],
                &fbas,
                Some(&organizations),
            )
            .unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn remove_non_minimal_sets() {
        let result =
            NodeIdSetVecResult::new(bitsetvec![{0, 1}, {0, 2, 3}, {3}, {0, 1, 4, 5}], None);
        let expected = bitsetvec![{3}, {0, 1}];
        let actual = result.minimal_sets().unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn merge_results_by_organization() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "Jim"
            },
            {
                "publicKey": "Jon"
            },
            {
                "publicKey": "Alex"
            },
            {
                "publicKey": "Bob"
            }
            ]"#,
        );
        let organizations = Groupings::organizations_from_json_str(
            r#"[
            {
                "name": "J Mafia",
                "validators": [ "Jim", "Jon" ]
            },
            {
                "name": "B Mafia",
                "validators": [ "Bob" ]
            }
            ]"#,
            &fbas,
        );
        let result = NodeIdSetVecResult::new(bitsetvec![{0, 1}, {0, 2}, {3}], None);
        let expected = bitsetvec![{0}, {0, 2}, {3}];
        let actual = result.merged_by_group(&organizations).unwrap();
        assert_eq!(expected, actual);
    }
    #[test]
    fn merge_results_by_country() {
        let fbas_input = r#"[
            {
                "publicKey": "Jim",
                "geoData": {
                    "countryName": "Oceania,"
                }
            },
            {
                "publicKey": "Jon",
                "geoData": {
                    "countryName": "Oceania"
                }
            },
            {
                "publicKey": "Alex",
                "geoData": {
                    "countryName": "Eastasia"
                }
            },
            {
                "publicKey": "Bob"
            }
            ]"#;
        let fbas = Fbas::from_json_str(&fbas_input);
        let countries = Groupings::countries_from_json_str(&fbas_input, &fbas);
        let result = NodeIdSetVecResult::new(bitsetvec![{0, 1}, {0, 2}, {3}], None);
        let actual = result.merged_by_group(&countries).unwrap();
        let expected = bitsetvec![{0}, {0, 2}, {3}];
        assert_eq!(expected, actual);
    }
}
