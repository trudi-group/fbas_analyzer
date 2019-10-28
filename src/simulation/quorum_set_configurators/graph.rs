use super::*;

/// Makes quorum slices based on his immediate graph neighbors, in total disregard for quorum
/// intersection, which nodes exist, and anything else...
struct SimpleGraphQsc {
    graph: Graph,
    relative_threshold: f64,
}
impl SimpleGraphQsc {
    fn new(graph: Graph, relative_threshold: f64) -> Self {
        SimpleGraphQsc { graph, relative_threshold }
    }
}
impl QuorumSetConfigurator for SimpleGraphQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let existing_quorum_set = &mut fbas.nodes[node_id].quorum_set;
        if *existing_quorum_set == QuorumSet::new() {
            let validators = self.graph.connections[node_id].clone();
            let threshold = (self.relative_threshold * validators.len() as f64).ceil() as usize;

            existing_quorum_set.validators.extend(validators);
            existing_quorum_set.threshold = threshold;
            Change
        } else {
            NoChange
        }
    }
}

struct Graph {
    // connections per node
    connections: Vec<Vec<NodeId>>
}
impl Graph {
    fn new(connections: Vec<Vec<NodeId>>) -> Self {
        Graph{connections}
    }
    /// Build a scale-free graph using the Barabási–Albert (BA) model
    fn new_scale_free() -> Self {
        // TODO
        Self::new(vec![])
    }
    /// Build a graph where every node is connected to every other node (including itself)
    fn new_full_mesh(n: usize) -> Self {
        Self::new((0..n).map(|_| (0..n).collect()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::monitors::*;

    #[test]
    fn full_mesh_can_be_like_super_safe() {
        let n = 3;
        let mut simulator_full_mesh = Simulator::new(
            Fbas::new(),
            Rc::new(SimpleGraphQsc::new(Graph::new_full_mesh(n), 1.0)),
            Rc::new(DummyMonitor),
        );
        let mut simulator_safe = Simulator::new(
            Fbas::new(),
            Rc::new(SuperSafeQsc::new()),
            Rc::new(DummyMonitor),
        );
        simulator_full_mesh.simulate_growth(n);
        simulator_safe.simulate_growth(n);

        assert_eq!(simulator_safe.finalize(), simulator_full_mesh.finalize());
    }
}
