use super::*;

/// Makes non-nested quorum sets containing all immediate graph neighbors
pub struct HigherTiersGraphQsc {
    graph: Graph,
    rank_scores: Vec<RankScore>,
    relative_threshold: Option<f64>,
}
impl HigherTiersGraphQsc {
    pub fn new(graph: Graph, relative_threshold: Option<f64>) -> Self {
        HigherTiersGraphQsc {
            rank_scores: graph.get_rank_scores(),
            graph,
            relative_threshold,
        }
    }
    pub fn new_67p(graph: Graph) -> Self {
        Self::new(graph, None)
    }
    pub fn new_relative(graph: Graph, relative_threshold: f64) -> Self {
        Self::new(graph, Some(relative_threshold))
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
impl QuorumSetConfigurator for HigherTiersGraphQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let existing_quorum_set = &mut fbas.nodes[node_id].quorum_set;
        if *existing_quorum_set == QuorumSet::new() {
            let (higher_tier_neighbors, same_tier_neighbors, _) =
                self.get_neighbors_by_tierness(node_id);
            let mut validators = if higher_tier_neighbors.is_empty() {
                same_tier_neighbors
            } else {
                higher_tier_neighbors
            };

            if !validators.is_empty() && !validators.contains(&node_id) {
                // we add nodes to their own quorum sets because
                // 1. nodes in the Stellar network often do it.
                // 2. it makes sense for threshold calculation (for achieving global n=3f+1)
                validators.push(node_id);
            }
            validators.sort(); // for easier comparability

            let threshold = calculate_threshold(validators.len(), self.relative_threshold);

            existing_quorum_set.validators.extend(validators);
            existing_quorum_set.threshold = threshold;
            Change
        } else {
            NoChange
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_neighbors_by_tierness_middle_tier_directed_links() {
        let graph = Graph::new_tiered_full_mesh(&vec![3, 3, 3]);
        let higher_tier_qsc = HigherTiersGraphQsc::new_67p(graph);
        let actual = higher_tier_qsc.get_neighbors_by_tierness(3);
        let expected = (vec![0, 1, 2], vec![4, 5], vec![]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn get_neighbors_by_tierness_top_tier_directed_links() {
        let graph = Graph::new_tiered_full_mesh(&vec![3, 3, 3]);
        let higher_tier_qsc = HigherTiersGraphQsc::new_67p(graph);
        let actual = higher_tier_qsc.get_neighbors_by_tierness(1);
        let expected = (vec![], vec![0, 2], vec![]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn get_neighbors_by_tierness_middle_tier_undirected_links() {
        let mut graph = Graph::new_full_mesh(4);
        graph.outlinks.push(vec![3, 5]);
        graph.outlinks.push(vec![4]);
        graph.outlinks[3].push(4);
        let higher_tier_qsc = HigherTiersGraphQsc::new_67p(graph);
        let actual = higher_tier_qsc.get_neighbors_by_tierness(4);
        let expected = (vec![3], vec![5], vec![]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn get_neighbors_by_tierness_top_tier_undirected_links() {
        let mut graph = Graph::new_full_mesh(4);
        graph.outlinks.push(vec![3, 5]);
        graph.outlinks.push(vec![4]);
        graph.outlinks[3].push(4);
        let higher_tier_qsc = HigherTiersGraphQsc::new_67p(graph);
        let actual = higher_tier_qsc.get_neighbors_by_tierness(3);
        let expected = (vec![], vec![0, 1, 2], vec![4]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn higher_tier_qsc_can_be_like_ideal_safe() {
        let n = 10;
        let higher_tier_qsc = HigherTiersGraphQsc::new_67p(Graph::new_tiered_full_mesh(&vec![n]));
        let ideal_qsc = IdealQsc::new();

        let actual = simulate!(higher_tier_qsc, n);
        let expected = simulate!(ideal_qsc, n);
        assert_eq!(expected, actual);
    }

    #[test]
    fn higher_tier_qsc_has_few_minimal_quorums() {
        let tier_sizes = vec![3, 10, 20];
        let higher_tier_qsc =
            HigherTiersGraphQsc::new_67p(Graph::new_tiered_full_mesh(&tier_sizes));
        let n = tier_sizes.into_iter().sum();

        let fbas = simulate!(higher_tier_qsc, n);
        let actual = find_minimal_quorums(&fbas);
        let expected = vec![bitset![0, 1, 2]];
        assert_eq!(expected, actual);
    }
}
