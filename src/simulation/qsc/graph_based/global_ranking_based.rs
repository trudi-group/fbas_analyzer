use super::*;

/// Uses all nodes with above-average global rank
pub struct GlobalRankQsc {
    graph: Graph,
    top_tier_nodes: Vec<NodeId>,
    relative_threshold: Option<f64>,
}
impl GlobalRankQsc {
    pub fn new(graph: Graph, relative_threshold: Option<f64>) -> Self {
        GlobalRankQsc {
            top_tier_nodes: Self::get_top_tier_nodes(&graph),
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
    /// Returns all nodes with above-average rank
    fn get_top_tier_nodes(graph: &Graph) -> Vec<NodeId> {
        let n = graph.outlinks.len();
        let rank_scores = graph.get_rank_scores();
        let average_rank_score = rank_scores.iter().sum::<RankScore>() / n as RankScore;
        debug!(
            "rank scores: {:?}; average rank score: {:?}",
            rank_scores, average_rank_score
        );

        (0..n)
            .filter(|&i| rank_scores[i] > average_rank_score)
            .collect()
    }
}
impl QuorumSetConfigurator for GlobalRankQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let existing_quorum_set = &mut fbas.nodes[node_id].quorum_set;
        if *existing_quorum_set == QuorumSet::new_empty()
            && !self.graph.outlinks[node_id].is_empty()
        {
            let validators = self.top_tier_nodes.clone();

            let threshold = calculate_threshold(validators.len(), self.relative_threshold);

            existing_quorum_set.validators = validators;
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
    fn global_rank_qsc_can_be_like_ideal() {
        let n = 10;
        let global_rank_qsc = GlobalRankQsc::new_67p(Graph::new_full_mesh(n));
        let ideal_qsc = IdealQsc::new();

        let actual = simulate!(global_rank_qsc, n);
        let expected = simulate!(ideal_qsc, n);
        assert_eq!(expected, actual);
    }

    #[test]
    fn global_rank_qsc() {
        let graph = Graph::new_tiered_full_mesh(&[2, 3, 1]);
        let n = graph.number_of_nodes();
        let qsc = GlobalRankQsc::new_67p(graph);

        let mut expected = Fbas::new();
        for _ in 0..n {
            expected.add_generic_node(QuorumSet {
                validators: vec![0, 1],
                threshold: 2,
                inner_quorum_sets: vec![],
            });
        }
        let actual = simulate!(qsc, n);
        assert_eq!(expected, actual);
    }
}
