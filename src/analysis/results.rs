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
    pub fn unwrap(self) -> Vec<NodeIdSet> {
        if let Some(unshrink_table) = self.unshrink_table {
            unshrink_sets(&self.node_sets, &unshrink_table)
        } else {
            self.node_sets
        }
    }
    pub fn into_vec_vec(self) -> Vec<Vec<NodeId>> {
        self.node_sets
            .iter()
            .map(|node_set| {
                if let Some(unshrink_table) = self.unshrink_table.as_ref() {
                    unshrink_set(node_set, unshrink_table).into_iter().collect()
                } else {
                    node_set.iter().collect()
                }
            })
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
        let min = self.node_sets.iter().map(|s| s.len()).min().unwrap_or(0);
        let max = self.node_sets.iter().map(|s| s.len()).max().unwrap_or(0);
        let mean = if self.node_sets.is_empty() {
            0.0
        } else {
            self.node_sets.iter().map(|s| s.len()).sum::<usize>() as f64
                / (self.node_sets.len() as f64)
        };
        (min, max, mean)
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
}
