use super::*;

/// Makes non-nested quorum sets containing all immediate graph neighbors
pub struct SimpleGraphQsc {
    graph: Graph,
    relative_threshold: Option<f64>,
}
impl SimpleGraphQsc {
    pub fn new(graph: Graph, relative_threshold: Option<f64>) -> Self {
        SimpleGraphQsc {
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
}
impl QuorumSetConfigurator for SimpleGraphQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let existing_quorum_set = &mut fbas.nodes[node_id].quorum_set;
        if *existing_quorum_set == QuorumSet::new() {
            let mut validators = self
                .graph
                .connections
                .get(node_id)
                .expect("Graph too small for this FBAS!")
                .clone();

            if !validators.contains(&node_id) {
                // we add nodes to their own quorum sets because
                // 1. nodes in the Stellar network often do it.
                // 2. it makes sense for threshold calculation (for achieving global n=3f+1)
                validators.push(node_id);
            }
            validators.sort(); // for easier comparability

            let threshold = if let Some(relative_threshold) = self.relative_threshold {
                (relative_threshold * validators.len() as f64).ceil() as usize
            } else {
                get_67p_threshold(validators.len())
            };

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
    fn simple_qsc_can_be_like_super_safe() {
        let n = 10;
        let simple_qsc = SimpleGraphQsc::new_relative(Graph::new_full_mesh(n), 1.0);
        let super_safe_qsc = SuperSafeQsc::new();

        let actual = simulate!(simple_qsc, n);
        let expected = simulate!(super_safe_qsc, n);
        assert_eq!(expected, actual);
    }

    #[test]
    fn simple_qsc_can_be_like_ideal_safe() {
        let n = 10;
        let simple_qsc = SimpleGraphQsc::new_67p(Graph::new_full_mesh(n));
        let ideal_qsc = IdealQsc::new();

        let actual = simulate!(simple_qsc, n);
        let expected = simulate!(ideal_qsc, n);
        assert_eq!(expected, actual);
    }
}
