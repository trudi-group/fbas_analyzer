use super::*;

/// Makes 67% quorum sets based on immediate graph neighbors,
/// putting lower quality nodes into a 67% inner quorum set.
/// Node degree used as a proxy for quality.
pub struct QualityAwareGraphQsc {
    graph: Graph,
    quality_scores: Vec<usize>,
}
impl QualityAwareGraphQsc {
    pub fn new(graph: Graph) -> Self {
        let quality_scores = graph.get_in_degrees();
        QualityAwareGraphQsc {
            graph,
            quality_scores,
        }
    }
    fn quality(&self, node_id: NodeId) -> usize {
        self.quality_scores[node_id]
    }
}
impl QuorumSetConfigurator for QualityAwareGraphQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let existing_quorum_set = &mut fbas.nodes[node_id].quorum_set;
        let neighbors = self
            .graph
            .connections
            .get(node_id)
            .expect("Graph too small for this FBAS!")
            .clone();

        let average_quality: usize = (neighbors.iter().map(|&id| self.quality(id)).sum::<usize>()
            as f64
            / neighbors.len() as f64)
            .round() as usize;

        let mut higher_quality_validators: Vec<NodeId> = vec![];
        let mut lower_quality_validators: Vec<NodeId> = vec![];

        if !neighbors.contains(&node_id) {
            // we add nodes to their own quorum sets because
            // 1. nodes in the Stellar network often do it.
            // 2. it makes sense for threshold calculation (for achieving global n=3f+1)
            higher_quality_validators.push(node_id);
        }

        let mut unprocessed = neighbors;
        unprocessed.sort_by_key(|&id| self.quality(id));

        while let Some(next) = unprocessed.pop() {
            if higher_quality_validators.len() < 3
                || self.quality(higher_quality_validators[2]) == self.quality(next)
                || self.quality(next) >= average_quality
            {
                higher_quality_validators.push(next);
            } else {
                lower_quality_validators.push(next);
            }
        }

        let new_quorum_set = if lower_quality_validators.len() > 1 {
            higher_quality_validators.sort(); // for easier comparability
            lower_quality_validators.sort(); // for easier comparability
            let validators = higher_quality_validators;
            let threshold = get_67p_threshold(validators.len() + 1);
            let inner_quorum_sets = vec![QuorumSet {
                threshold: get_67p_threshold(lower_quality_validators.len()),
                validators: lower_quality_validators,
                inner_quorum_sets: vec![],
            }];
            QuorumSet {
                validators,
                threshold,
                inner_quorum_sets,
            }
        } else {
            let mut validators = higher_quality_validators;
            validators.extend(lower_quality_validators.into_iter());
            validators.sort(); // for easier comparability
            let threshold = get_67p_threshold(validators.len());
            let inner_quorum_sets = vec![];
            QuorumSet {
                validators,
                threshold,
                inner_quorum_sets,
            }
        };

        if *existing_quorum_set != new_quorum_set {
            *existing_quorum_set = new_quorum_set;
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
    fn quality_aware_qsc_like_simple_when_nodes_all_same() {
        let n = 30;
        let graph = Graph::new_random_small_world(n, 4, 0.);
        let simple = simulate!(SimpleGraphQsc::new(graph.clone(), 0.67), n);
        let quality_aware = simulate!(QualityAwareGraphQsc::new(graph), n);
        assert_eq!(simple, quality_aware);
    }

    #[test]
    fn quality_aware_qsc_uses_in_degrees_as_quality() {
        let (n, m0, m) = (23, 3, 2);
        let graph = Graph::new_random_scale_free(n, m0, m);
        let qsc = QualityAwareGraphQsc::new(graph.clone());
        let actual = graph.get_in_degrees();
        let expected = qsc.quality_scores;
        assert_eq!(expected, actual);
    }

    #[test]
    fn quality_aware_qsc_no_inner_set_if_few_friends() {
        let n = 8;
        let mut graph = Graph::new_full_mesh(n);
        graph.connections[0] = vec![1, 2, 3];
        let mut qsc = QualityAwareGraphQsc::new(graph.clone());
        qsc.quality_scores[1] = 30;
        qsc.quality_scores[2] = 50;

        let fbas = simulate!(qsc, n);

        let expected = &QuorumSet {
            validators: vec![0, 1, 2, 3],
            threshold: 3,
            inner_quorum_sets: vec![],
        };
        let actual = &fbas.nodes[0].quorum_set;
        assert_eq!(expected, actual);
    }

    #[test]
    fn quality_aware_qsc_no_inner_set_if_same_quality() {
        let n = 8;
        let graph = Graph::new_full_mesh(n);
        let qsc = QualityAwareGraphQsc::new(graph.clone());

        let fbas = simulate!(qsc, n);

        let expected = &QuorumSet {
            validators: vec![0, 1, 2, 3, 4, 5, 6, 7],
            threshold: 6,
            inner_quorum_sets: vec![],
        };
        let actual = &fbas.nodes[0].quorum_set;
        assert_eq!(expected, actual);
    }

    #[test]
    fn quality_aware_qsc_top_2_neighbors_remain_validators() {
        let n = 8;
        let graph = Graph::new_full_mesh(n);
        let mut qsc = QualityAwareGraphQsc::new(graph.clone());
        qsc.quality_scores[1] = 70;
        qsc.quality_scores[2] = 8;
        qsc.quality_scores[3] = 7;
        qsc.quality_scores[4] = 7;
        qsc.quality_scores[5] = 7;
        qsc.quality_scores[6] = 7;
        qsc.quality_scores[7] = 7;

        let fbas = simulate!(qsc, n);

        let expected = &QuorumSet {
            validators: vec![0, 1, 2],
            threshold: 3,
            inner_quorum_sets: vec![QuorumSet {
                validators: vec![3, 4, 5, 6, 7],
                threshold: 4,
                inner_quorum_sets: vec![],
            }],
        };
        let actual = &fbas.nodes[0].quorum_set;
        assert_eq!(expected, actual);
    }

    #[test]
    fn quality_aware_qsc_top_4_neighbors_remain_validators_if_same_quality() {
        let n = 8;
        let graph = Graph::new_full_mesh(n);
        let mut qsc = QualityAwareGraphQsc::new(graph.clone());
        qsc.quality_scores[1] = 79;
        qsc.quality_scores[2] = 8;
        qsc.quality_scores[3] = 8;
        qsc.quality_scores[4] = 8;
        qsc.quality_scores[5] = 7;
        qsc.quality_scores[6] = 7;
        qsc.quality_scores[7] = 7;

        let fbas = simulate!(qsc, n);

        let expected = &QuorumSet {
            validators: vec![0, 1, 2, 3, 4],
            threshold: 5,
            inner_quorum_sets: vec![QuorumSet {
                validators: vec![5, 6, 7],
                threshold: 3,
                inner_quorum_sets: vec![],
            }],
        };
        let actual = &fbas.nodes[0].quorum_set;
        assert_eq!(expected, actual);
    }

    #[test]
    fn quality_aware_qsc_uses_67p_thresholds() {
        let n = 50;
        let graph = Graph::new_random_scale_free(n, 2, 2);
        let qsc = QualityAwareGraphQsc::new(graph);

        let fbas = simulate!(qsc, n);

        for node in fbas.nodes.into_iter() {
            assert_has_67p_threshold!(node.quorum_set);
        }
    }

    #[test]
    fn quality_aware_qsc_uses_all_neighbors_and_node_itself() {
        let n = 50;
        let graph = Graph::new_random_scale_free(n, 2, 2);
        let qsc = QualityAwareGraphQsc::new(graph.clone());

        let fbas = simulate!(qsc, n);

        for (node_id, node) in fbas.nodes.into_iter().enumerate() {
            let mut expected: NodeIdSet = graph.connections[node_id].iter().cloned().collect();
            expected.insert(node_id);
            let actual = node.quorum_set.contained_nodes();
            assert_eq!(expected, actual);
        }
    }
}
