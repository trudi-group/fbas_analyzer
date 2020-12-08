use super::*;

/// Only use neighbors perceived as higher-tier as validators, or only nodes perceived as
/// same-tier, if there are no higher-tier neighbors.
pub struct HigherTierNeighborsQsc {
    graph: Graph,
    rank_scores: Vec<RankScore>,
    connected_nodes: NodeIdSet,
    relative_threshold: Option<f64>,
    symmetry_enforcing: bool,
}
impl HigherTierNeighborsQsc {
    pub fn new(graph: Graph, relative_threshold: Option<f64>, symmetry_enforcing: bool) -> Self {
        let rank_scores = graph.get_rank_scores();
        debug!(
            "Non-zero rank scores: {:?}",
            rank_scores
                .iter()
                .copied()
                .enumerate()
                .filter(|&(_, s)| s > 0.0)
                .collect::<Vec<(NodeId, f64)>>()
        );
        let connected_nodes = graph.get_connected_nodes();
        HigherTierNeighborsQsc {
            graph,
            rank_scores,
            connected_nodes,
            relative_threshold,
            symmetry_enforcing,
        }
    }
    pub fn new_67p(graph: Graph, symmetry_enforcing: bool) -> Self {
        Self::new(graph, None, symmetry_enforcing)
    }
    pub fn new_relative(graph: Graph, relative_threshold: f64, symmetry_enforcing: bool) -> Self {
        Self::new(graph, Some(relative_threshold), symmetry_enforcing)
    }
    fn get_neighbors_by_tierness(
        &self,
        node_id: NodeId,
    ) -> (Vec<NodeId>, Vec<NodeId>, Vec<NodeId>) {
        let own_rank_score = self.rank_scores[node_id];
        let neighbors: Vec<NodeId> = self
            .graph
            .outlinks
            .get(node_id)
            .expect("Graph too small for this FBAS!")
            .clone();

        let is_higher_tier = |i: &NodeId| self.rank_scores[*i] >= 2. * own_rank_score;
        let (higher_tier, other_tier): (Vec<NodeId>, Vec<NodeId>) =
            neighbors.into_iter().partition(is_higher_tier);

        let is_lower_tier = |i: &NodeId| own_rank_score >= 2. * self.rank_scores[*i];
        let (lower_tier, same_tier): (Vec<NodeId>, Vec<NodeId>) =
            other_tier.into_iter().partition(is_lower_tier);

        (higher_tier, same_tier, lower_tier)
    }
}
impl QuorumSetConfigurator for HigherTierNeighborsQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        if !self.connected_nodes.contains(node_id) {
            return NoChange;
        }

        let (higher_tier_neighbors, same_tier_neighbors, _) =
            self.get_neighbors_by_tierness(node_id);

        let mut validators = if !higher_tier_neighbors.is_empty() {
            higher_tier_neighbors
        } else {
            same_tier_neighbors
        };

        if self.symmetry_enforcing {
            let mut corrected_validators = NodeIdSet::new();
            for i in validators.drain(..) {
                corrected_validators.insert(i);
                if let Some(other_node) = fbas.nodes.get(i) {
                    let other_validators = other_node.quorum_set.contained_nodes();
                    if other_validators.contains(node_id) {
                        corrected_validators.union_with(&other_validators);
                    }
                }
            }
            validators.extend(corrected_validators.into_iter());
        }

        if !validators.contains(&node_id) {
            // we add nodes to their own quorum sets because
            // 1. nodes in the Stellar network often do it.
            // 2. it makes sense for threshold calculation (for achieving global n=3f+1)
            validators.push(node_id);
        }
        validators.sort_unstable(); // for easier comparability
        let threshold = calculate_threshold(validators.len(), self.relative_threshold);

        let candidate_quorum_set = QuorumSet {
            validators,
            threshold,
            inner_quorum_sets: vec![],
        };
        let existing_quorum_set = &mut fbas.nodes[node_id].quorum_set;

        if *existing_quorum_set == candidate_quorum_set {
            NoChange
        } else {
            *existing_quorum_set = candidate_quorum_set;
            Change
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_neighbors_by_tierness_middle_tier_directed_links() {
        let graph = Graph::new_tiered_full_mesh(&vec![3, 3, 3]);
        let higher_tier_qsc = HigherTierNeighborsQsc::new_67p(graph, false);
        let actual = higher_tier_qsc.get_neighbors_by_tierness(3);
        let expected = (vec![0, 1, 2], vec![4, 5], vec![]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn get_neighbors_by_tierness_top_tier_directed_links() {
        let graph = Graph::new_tiered_full_mesh(&vec![3, 3, 3]);
        let higher_tier_qsc = HigherTierNeighborsQsc::new_67p(graph, false);
        let actual = higher_tier_qsc.get_neighbors_by_tierness(1);
        let expected = (vec![], vec![0, 2], vec![]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn get_neighbors_by_tierness_middle_tier_undirected_links() {
        let mut graph = Graph::new_full_mesh(4);
        graph.outlinks.push(vec![3, 5]);
        graph.outlinks.push(vec![3, 4]);
        graph.outlinks.push(vec![3, 4, 6]);
        graph.outlinks.push(vec![5]);
        graph.outlinks[3].push(4);
        graph.outlinks[3].push(5);
        let higher_tier_qsc = HigherTierNeighborsQsc::new_67p(graph, false);
        let actual = higher_tier_qsc.get_neighbors_by_tierness(4);
        let expected = (vec![3], vec![5], vec![]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn get_neighbors_by_tierness_top_tier_undirected_links() {
        let mut graph = Graph::new_full_mesh(4);
        graph.outlinks.push(vec![3, 5]);
        graph.outlinks.push(vec![3, 4]);
        graph.outlinks.push(vec![3, 4, 6]);
        graph.outlinks.push(vec![5]);
        graph.outlinks[3].push(4);
        graph.outlinks[3].push(5);
        let higher_tier_qsc = HigherTierNeighborsQsc::new_67p(graph, false);
        let actual = higher_tier_qsc.get_neighbors_by_tierness(3);
        let expected = (vec![], vec![0, 1, 2], vec![4, 5]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn higher_tier_qsc_can_be_like_ideal_safe() {
        let n = 10;
        let higher_tier_qsc =
            HigherTierNeighborsQsc::new_67p(Graph::new_tiered_full_mesh(&vec![n]), false);
        let ideal_qsc = IdealQsc::new();

        let actual = simulate!(higher_tier_qsc, n);
        let expected = simulate!(ideal_qsc, n);
        assert_eq!(expected, actual);
    }

    #[test]
    fn higher_tier_qsc_has_few_minimal_quorums() {
        let tier_sizes = vec![3, 10, 20];
        let higher_tier_qsc =
            HigherTierNeighborsQsc::new_67p(Graph::new_tiered_full_mesh(&tier_sizes), false);
        let n = tier_sizes.into_iter().sum();

        let fbas = simulate!(higher_tier_qsc, n);
        let actual = find_minimal_quorums(&fbas);
        let expected = vec![bitset![0, 1, 2]];
        assert_eq!(expected, actual);
    }

    #[test]
    fn higher_tier_qsc_can_make_symmetric_top_tier() {
        let tier_sizes = vec![4, 10, 20];
        let mut graph = Graph::new_tiered_full_mesh(&tier_sizes);
        graph.outlinks[0] = vec![1];
        let n = tier_sizes.into_iter().sum();
        let higher_tier_qsc = HigherTierNeighborsQsc::new_67p(graph, true);

        let fbas = simulate!(higher_tier_qsc, n);
        let analysis = Analysis::new(&fbas);

        let expected = vec![QuorumSet {
            validators: vec![0, 1, 2, 3],
            threshold: 3,
            inner_quorum_sets: vec![],
        }];
        let actual = analysis.symmetric_clusters();
        assert_eq!(expected, actual);
    }
}
