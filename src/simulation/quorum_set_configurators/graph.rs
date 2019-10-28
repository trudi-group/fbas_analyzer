use super::*;

/// Makes quorum slices based on his immediate graph neighbors, in total disregard for quorum
/// intersection, which nodes exist, and anything else...
pub struct SimpleGraphQsc {
    graph: Graph,
    relative_threshold: f64,
}
impl SimpleGraphQsc {
    pub fn new(graph: Graph, relative_threshold: f64) -> Self {
        SimpleGraphQsc {
            graph,
            relative_threshold,
        }
    }
}
impl QuorumSetConfigurator for SimpleGraphQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let existing_quorum_set = &mut fbas.nodes[node_id].quorum_set;
        if *existing_quorum_set == QuorumSet::new() {
            let validators = self
                .graph
                .connections
                .get(node_id)
                .expect("Graph too small for this FBAS!")
                .clone();
            let threshold = (self.relative_threshold * validators.len() as f64).ceil() as usize;

            existing_quorum_set.validators.extend(validators);
            existing_quorum_set.threshold = threshold;
            Change
        } else {
            NoChange
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Graph {
    // connections per node
    connections: Vec<Vec<NodeId>>,
}
impl Graph {
    pub fn new(connections: Vec<Vec<NodeId>>) -> Self {
        Graph { connections }
    }
    /// Build a graph where every node is connected to every other node (including itself)
    pub fn new_full_mesh(n: usize) -> Self {
        Self::new(vec![(0..n).collect(); n])
    }
    /// Build a scale-free graph using the Barabási–Albert (BA) model
    pub fn new_random_scale_free(n: usize, m0: usize, m: usize) -> Self {
        assert!(
            0 < m && m <= m0 && m <= n,
            "Parameters for Barabási–Albert don't make sense."
        );

        let mut rng = thread_rng();

        let mut connections: Vec<Vec<NodeId>> = vec![vec![]; n];

        macro_rules! connect {
            ($a:expr, $b:expr) => {
                let (a, b) = ($a, $b);
                debug_assert_ne!(a, b);
                connections[a].push(b);
                connections[b].push(a);
            };
        }

        // init
        for i in 0..m0 {
            for j in i + 1..m0 {
                connect!(i, j);
            }
        }

        // rest
        for i in m0..n {
            let mut possible_targets: Vec<NodeId> = (0..i).collect();
            for _ in 0..m {
                let j = possible_targets
                    .choose_weighted(&mut rng, |&x| connections[x].len())
                    .unwrap()
                    .to_owned();
                connect!(i, j);
                // remove j from possible targets
                if j == i - 1 {
                    possible_targets.pop();
                } else {
                    possible_targets =
                        [&possible_targets[..j], &possible_targets[j + 1..]].concat();
                }
            }
        }
        Self::new(connections)
    }
    /// Shuffle the node IDs
    pub fn shuffled(self) -> Self {
        let n = self.connections.len();
        let mut rng = thread_rng();

        // mappings
        let mut old_to_new: Vec<NodeId> = (0..n).collect();
        old_to_new.shuffle(&mut rng);
        let mut new_to_old = vec![0; n];
        for (old, &new) in old_to_new.iter().enumerate() {
            new_to_old[new] = old;
        }
        let (new_to_old, old_to_new) = (new_to_old, old_to_new);

        let new_connections = new_to_old
            .iter()
            .map(|&oi| {
                self.connections[oi]
                    .iter()
                    .map(|&oj| old_to_new[oj])
                    .collect()
            })
            .collect();
        Self::new(new_connections)
    }
}

#[cfg(test)]
mod tests {
    use super::monitors::*;
    use super::*;

    #[test]
    fn full_mesh_qsc_can_be_like_super_safe() {
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

    #[test]
    fn scale_free_graph_interconnects_m0_fully() {
        let (n, m0, m) = (23, 8, 2);
        let graph = Graph::new_random_scale_free(n, m0, m);

        assert!((0..m0).all(|i| (0..i)
            .chain(i + 1..m0)
            .all(|j| graph.connections[j].iter().any(|&x| x == i))));
    }

    #[test]
    fn scale_free_graph_has_sane_amount_of_edges_overall() {
        let (n, m0, m) = (23, 3, 2);
        let graph = Graph::new_random_scale_free(n, m0, m);

        let expected = (m0 * (m0 - 1)) / 2 + (n - m0) * m;
        let actual: usize = graph
            .connections
            .into_iter()
            .map(|x| x.len())
            .sum::<usize>()
            / 2;
        assert_eq!(expected, actual);
    }

    #[test]
    fn scale_free_graph_is_undirected() {
        let (n, m0, m) = (23, 3, 2);
        let graph = Graph::new_random_scale_free(n, m0, m);

        assert!((0..n).all(|i| graph.connections[i]
            .iter()
            .all(|&j| graph.connections[j].iter().any(|&x| x == i))));
    }

    #[test]
    fn graph_shuffle_shuffles() {
        let (n, m0, m) = (23, 3, 2);
        let graph = Graph::new_random_scale_free(n, m0, m);
        let shuffled = graph.clone().shuffled();
        assert_ne!(graph, shuffled);
    }

    #[test]
    fn graph_shuffle_preserves_degrees() {
        let (n, m0, m) = (23, 3, 2);
        let graph = Graph::new_random_scale_free(n, m0, m);
        let shuffled = graph.clone().shuffled();

        fn degrees(graph: Graph) -> Vec<usize> {
            let mut result: Vec<usize> = graph.connections.into_iter().map(|x| x.len()).collect();
            result.sort();
            result
        }
        assert_eq!(degrees(graph), degrees(shuffled));
    }
}
