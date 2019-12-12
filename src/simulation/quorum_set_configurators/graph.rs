use super::*;

/// Makes quorum sets based on his immediate graph neighbors, in total disregard for quorum
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

#[derive(Clone, Debug, PartialEq)]
pub struct Graph {
    // connections per node
    connections: Vec<Vec<NodeId>>,
}
impl Graph {
    pub fn new(connections: Vec<Vec<NodeId>>) -> Self {
        Graph { connections }
    }
    /// Build a graph where every node is connected to every other node
    pub fn new_full_mesh(n: usize) -> Self {
        Self::new((0..n).map(|i| (0..i).chain(i + 1..n).collect()).collect())
    }
    /// Build a scale-free graph using the Barabási–Albert (BA) model
    pub fn new_random_scale_free(n: usize, m0: usize, m: usize) -> Self {
        assert!(
            0 < m && m <= m0 && m <= n,
            "Parameters for Barabási–Albert don't make sense."
        );

        let mut connections: Vec<Vec<NodeId>> = vec![vec![]; n];
        let mut rng = thread_rng();

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
        let result = Self::new(connections);
        debug_assert!(result.is_undirected());
        result
    }
    /// Build a small world graph using the Watts-Strogatz model
    /// Not super optimized but OK for networks below 10^5 nodes.
    pub fn new_random_small_world(n: usize, k: usize, beta: f64) -> Self {
        assert!(
            k % 2 == 0,
            "For the Watts-Strogatz model, `k` must be an even number!"
        );
        assert!(n >= 2*k, "Node numbers close to k can lead to infinite loops and are therefore not supported; choose a k <= n/2.");

        let mut matrix = vec![vec![false; n]; n];
        let mut rng = thread_rng();

        // step 1: construct a ring lattice
        for i in 0..n {
            for j in i + 1..=i + k / 2 {
                let j = j % n;
                matrix[i][j] = true;
                matrix[j][i] = true;
            }
        }
        // step 2: rewire with probability beta
        let mut to_be_rewired: VecDeque<usize> = VecDeque::with_capacity(k);
        for i in 0..n {
            for j in i + 1..=i + k / 2 {
                let j = j % n;
                if matrix[i][j] && rng.gen_bool(beta) {
                    to_be_rewired.push_back(j);
                }
            }
            for j in to_be_rewired.drain(..) {
                // find new j
                let mut newj = i;
                while newj == i || matrix[i][newj] {
                    newj = rng.gen_range(0, n);
                }
                // rewire
                matrix[i][j] = false;
                matrix[j][i] = false;
                matrix[i][newj] = true;
                matrix[newj][i] = true;
            }
        }
        // transform to data format used here
        let mut connections = vec![vec![]; n];
        for i in 0..n {
            for j in 0..n {
                if matrix[i][j] {
                    connections[i].push(j);
                }
            }
        }
        let result = Self::new(connections);
        debug_assert!(result.is_undirected());
        result
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
    pub fn is_undirected(&self) -> bool {
        self.connections.iter().enumerate().all(|(i, cons_i)| {
            cons_i
                .iter()
                .map(|&j| &self.connections[j])
                .all(|cons_j| cons_j.iter().any(|&x| x == i))
        })
    }
    pub fn get_in_degrees(&self) -> Vec<usize> {
        let mut result: Vec<usize> = vec![0; self.connections.len()];
        for connections in self.connections.iter() {
            for &in_node in connections.iter() {
                result[in_node] = result[in_node].checked_add(1).unwrap();
            }
        }
        result
    }
    pub fn get_out_degrees(&self) -> Vec<usize> {
        self.connections.iter().map(|x| x.len()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::monitors::*;
    use super::*;

    #[test]
    fn full_mesh() {
        let expected = Graph {
            connections: vec![vec![1, 2, 3], vec![0, 2, 3], vec![0, 1, 3], vec![0, 1, 2]],
        };
        let actual = Graph::new_full_mesh(4);
        assert_eq!(expected, actual);
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
    fn small_world_graph_has_sane_amount_of_edges_overall() {
        let (n, k, beta) = (100, 10, 0.05);
        let graph = Graph::new_random_small_world(n, k, beta);

        let expected = n * k / 2;
        let actual: usize = graph
            .connections
            .into_iter()
            .map(|x| x.len())
            .sum::<usize>()
            / 2;
        assert_eq!(expected, actual);
    }

    #[test]
    fn small_world_graph_is_random() {
        let (n, k, beta) = (100, 10, 0.05);
        let graph1 = Graph::new_random_small_world(n, k, beta);
        let graph2 = Graph::new_random_small_world(n, k, beta);
        assert_ne!(graph1, graph2);
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

    #[test]
    fn node_degrees_undirected() {
        let (n, m0, m) = (23, 3, 2);
        let graph = Graph::new_random_scale_free(n, m0, m);
        assert!(graph.is_undirected());

        let expected: Vec<usize> = graph.connections.iter().map(|x| x.len()).collect();

        assert_eq!(expected, graph.get_in_degrees());
        assert_eq!(expected, graph.get_out_degrees());
    }

    #[test]
    fn node_degrees_directed() {
        // TODO
    }

    macro_rules! assert_has_67p_threshold {
        ($qset:expr) => {
            assert!(
                3 * $qset.threshold
                    >= 2 * ($qset.validators.len() + $qset.inner_quorum_sets.len()) + 1,
                "Not a 67% threshold!"
            )
        };
    }
    macro_rules! assert_eq_when_sorted {
        ($left:expr, $right:expr) => (
            let mut left = $left.clone();
            let mut right = $right.clone();
            left.sort();
            right.sort();
            assert_eq!(left, right)
        );
        ($left:expr, $right:expr, $($arg:tt)+) => (
            let mut left = $left.clone();
            let mut right = $right.clone();
            left.sort();
            right.sort();
            assert_eq!(left, right, $($arg)*)
        );
    }

    macro_rules! simulate {
        ($qsc:expr, $n:expr) => {{
            let mut simulator = Simulator::new(Fbas::new(), Rc::new($qsc), Rc::new(DummyMonitor));
            simulator.simulate_growth($n);
            simulator.finalize()
        }};
    }

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
        graph.connections[0] = vec![1, 2, 3, 4];
        let mut qsc = QualityAwareGraphQsc::new(graph.clone());
        qsc.quality_scores[1] = 30;
        qsc.quality_scores[2] = 50;

        let fbas = simulate!(qsc, n);

        let expected = &QuorumSet {
            validators: vec![1, 2, 3, 4],
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
            validators: vec![1, 2, 3, 4, 5, 6, 7],
            threshold: 5,
            inner_quorum_sets: vec![],
        };
        let actual = &fbas.nodes[0].quorum_set;
        assert_eq!(expected, actual);
    }

    #[test]
    fn quality_aware_qsc_top_3_neighbors_remain_validators() {
        let n = 8;
        let graph = Graph::new_full_mesh(n);
        let mut qsc = QualityAwareGraphQsc::new(graph.clone());
        qsc.quality_scores[1] = 70;
        qsc.quality_scores[2] = 8;
        qsc.quality_scores[3] = 8;
        qsc.quality_scores[4] = 7;
        qsc.quality_scores[5] = 7;
        qsc.quality_scores[6] = 7;
        qsc.quality_scores[7] = 7;

        let fbas = simulate!(qsc, n);

        let expected = &QuorumSet {
            validators: vec![1, 2, 3],
            threshold: 3,
            inner_quorum_sets: vec![QuorumSet {
                validators: vec![4, 5, 6, 7],
                threshold: 3,
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
            validators: vec![1, 2, 3, 4],
            threshold: 4,
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
    fn quality_aware_qsc_make_inner_qorum_set_for_lower_quality() {
        // FIXME way too long and ugly
        let n = 50;
        let graph = Graph::new_random_scale_free(n, 2, 2);
        let degrees = graph.get_in_degrees();

        let mut simulator = Simulator::new(
            Fbas::new(),
            Rc::new(QualityAwareGraphQsc::new(graph.clone())),
            Rc::new(DummyMonitor),
        );
        simulator.simulate_growth(n);
        let fbas = simulator.finalize();

        for (node_id, node) in fbas.nodes.into_iter().enumerate() {
            let qset = node.quorum_set;
            assert_has_67p_threshold!(qset);

            if degrees[node_id] > 4 {
                let mut neighbor_degrees: Vec<usize> = (&graph).connections[node_id]
                    .iter()
                    .map(|&nid| degrees[nid])
                    .collect();
                neighbor_degrees.sort();
                neighbor_degrees.reverse();

                if qset.inner_quorum_sets.is_empty() {
                    assert!(
                        neighbor_degrees[2] <= neighbor_degrees[neighbor_degrees.len() - 2],
                        "Should have put lower-degree nodes in inner quorum set."
                    );
                } else {
                    assert_eq!(
                        qset.inner_quorum_sets.len(),
                        1,
                        "Why more than one inner quorum set?"
                    );
                    let iqset = &qset.inner_quorum_sets[0];
                    assert_has_67p_threshold!(iqset);
                    assert!(
                        iqset.inner_quorum_sets.is_empty(),
                        "Why a second nested quroum set?"
                    );

                    let average_neighbor_degree = (neighbor_degrees.iter().sum::<usize>() as f64
                        / neighbor_degrees.len() as f64)
                        .round() as usize;
                    let cut_off_degree = cmp::min(average_neighbor_degree, neighbor_degrees[2]);

                    for &validator_id in qset.validators.iter() {
                        assert!(
                            degrees[validator_id] >= cut_off_degree,
                            format!(
                                "Quality threshold too low? {:?} vs {:?}; average is {:?}",
                                degrees[validator_id], cut_off_degree, average_neighbor_degree
                            )
                        );
                    }
                    for &ivalidator_id in iqset.validators.iter() {
                        assert!(
                            degrees[ivalidator_id] < cut_off_degree,
                            format!(
                                "Quality threshold too high? {:?} vs {:?}; average is {:?}",
                                degrees[ivalidator_id], cut_off_degree, average_neighbor_degree
                            )
                        );
                    }
                }
            } else {
                assert!(
                    qset.inner_quorum_sets.is_empty(),
                    "Too few nodes for using an inner quorum set."
                );
                assert_eq_when_sorted!(qset.validators, (&graph).connections[node_id]);
            }
        }
    }
}
