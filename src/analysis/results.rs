use super::*;

/// Wraps a node ID set.
#[derive(Debug, Clone)]
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
    pub fn remove_nodes_by_id(&mut self, nodes: impl IntoIterator<Item = NodeId>) {
        for node in nodes.into_iter() {
            self.node_set.remove(node);
        }
    }
    pub fn remove_nodes_by_pretty_name<'a>(
        &mut self,
        nodes: impl IntoIterator<Item = &'a str>,
        fbas: &Fbas,
        organizations: Option<&Organizations>,
    ) {
        let nodes_by_id = if let Some(ref orgs) = organizations {
            from_organization_names(nodes, fbas, orgs)
        } else {
            from_public_keys(nodes, fbas)
        };
        self.remove_nodes_by_id(nodes_by_id)
    }
}

/// Wraps a vector of node ID sets. Node ID sets are stored in shrunken form to preserve memory.
#[derive(Debug, Clone)]
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
    pub fn remove_nodes_by_id(&mut self, nodes: impl IntoIterator<Item = NodeId>) {
        self.unshrink();
        let nodes: NodeIdSet = nodes.into_iter().collect();
        for node_set in self.node_sets.iter_mut() {
            node_set.difference_with(&nodes);
        }
    }
    pub fn remove_nodes_by_pretty_name<'a>(
        &mut self,
        nodes: impl IntoIterator<Item = &'a str>,
        fbas: &Fbas,
        organizations: Option<&Organizations>,
    ) {
        let nodes_by_id = if let Some(ref orgs) = organizations {
            from_organization_names(nodes, fbas, orgs)
        } else {
            from_public_keys(nodes, fbas)
        };
        self.remove_nodes_by_id(nodes_by_id)
    }
    fn unshrink(&mut self) {
        if let Some(unshrink_table) = &self.unshrink_table {
            self.node_sets = unshrink_sets(&self.node_sets, &unshrink_table);
        }
        self.unshrink_table = None;
    }
}

fn from_public_keys<'a>(nodes: impl IntoIterator<Item = &'a str>, fbas: &Fbas) -> Vec<NodeId> {
    nodes
        .into_iter()
        .map(|pk| {
            fbas.get_node_id(pk)
                .unwrap_or_else(|| panic!("Public key {} not found in FBAS!", pk))
        })
        .collect()
}
fn from_organization_names<'a>(
    nodes: impl IntoIterator<Item = &'a str>,
    fbas: &Fbas,
    organizations: &Organizations,
) -> Vec<NodeId> {
    nodes
        .into_iter()
        .map(|name| match organizations.get_by_name(name) {
            Some(org) => org.validators.clone(),
            None => vec![fbas
                .get_node_id(name)
                .unwrap_or_else(|| panic!("Name {} not found!", name))],
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
        let mut result = NodeIdSetResult::new(bitset![0, 1], None);
        let expected = bitset![0];
        result.remove_nodes_by_id(vec![1]);
        assert_eq!(expected, result.unwrap());
    }

    #[test]
    fn remove_nodes_from_shrunken_result() {
        let shrink_manager = ShrinkManager::new(bitset![23, 42]);
        let mut result = NodeIdSetResult::new(bitset![0, 1], Some(&shrink_manager));
        let expected = bitset![42];
        result.remove_nodes_by_id(vec![23]);
        assert_eq!(expected, result.unwrap());
    }

    #[test]
    fn remove_nodes_from_unshrunken_vec_result() {
        let mut result = NodeIdSetVecResult::new(bitsetvec![{0, 1}, {0, 2}, {3}], None);
        let expected = bitsetvec![{0, 1}, {0}, {}];
        result.remove_nodes_by_id(vec![2, 3]);
        assert_eq!(expected, result.unwrap());
    }

    #[test]
    fn remove_nodes_from_shrunken_vec_result() {
        let shrink_manager = ShrinkManager::new(bitset![23, 42, 7, 1000]);
        let mut result =
            NodeIdSetVecResult::new(bitsetvec![{0, 1}, {0, 2}, {3}], Some(&shrink_manager));
        let expected = bitsetvec![{7, 23}, {7}, {}];
        result.remove_nodes_by_id(vec![42, 1000]);
        assert_eq!(expected, result.unwrap());
    }

    #[test]
    fn remove_nodes_by_pretty_name() {
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
        let organizations = Organizations::from_json_str(
            r#"[
            {
                "name": "J Mafia",
                "validators": [ "Jim", "Jon" ]
            }
            ]"#,
            &fbas,
        );
        let mut result = NodeIdSetVecResult::new(bitsetvec![{0, 1}, {0, 2}, {3}], None);
        let expected = bitsetvec![{}, { 2 }, {}];
        result.remove_nodes_by_pretty_name(vec!["J Mafia", "Bob"], &fbas, Some(&organizations));
        assert_eq!(expected, result.unwrap());
    }
}
