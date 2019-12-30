use super::*;

mod simple;
pub use simple::*;
mod quality;
pub use quality::*;

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
                possible_targets = possible_targets.into_iter().filter(|&x| x != j).collect();
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

        // List of free end nodes per node
        let mut target_nodes: Vec<Vec<NodeId>> = vec![Vec::with_capacity(n - k); n];
        for i in 0..n {
            for j in 0..n {
                if i != j && !matrix[i][j] && !matrix[j][i] {
                    target_nodes[i].push(j);
                }
            }
        }

        // step 2: rewire with progability beta
        let mut to_be_rewired: VecDeque<usize> = VecDeque::with_capacity(k);
        for i in 0..n {
            for j in i + 1..=i + k / 2 {
                let j = j % n;
                if matrix[i][j] && rng.gen_bool(beta) {
                    to_be_rewired.push_back(j);
                }
            }

            for j in to_be_rewired.drain(..) {
                let chosen_node = target_nodes[i].choose(&mut rng);
                match chosen_node {
                    Some(node) => {
                        let newj: usize = *node;
                        let mut index = target_nodes[i].iter().position(|x| *x == *node).unwrap();
                        target_nodes[i].remove(index);
                        if !target_nodes[newj].is_empty() { //remove i from newj's list
                            index = target_nodes[newj].iter().position(|x| *x == i).unwrap();
                            target_nodes[newj].remove(index);
                        }
                        //rewire
                        matrix[i][j] = false;
                        matrix[j][i] = false;
                        matrix[i][newj] = true;
                        matrix[newj][i] = true
                    }
                    None => continue,
                }
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
    fn scale_free_graph_doesnt_panic_on_exotic_m_values() {
        let (n, m0, m) = (40, 4, 4);
        Graph::new_random_scale_free(n, m0, m);
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

    #[test]
    fn small_world_graph_with_big_ks_has_same_number_of_edges() {
        let nodes = vec![10, 51, 100];
        let mean_k = vec![8, 50, 98];
        for (n, k) in nodes.iter().zip(mean_k.iter()) {
            let graph = Graph::new_random_small_world(*n, *k, 0.05);

            let expected = n * k / 2;
            let actual: usize = graph
                .connections
                .into_iter()
                .map(|x| x.len())
                .sum::<usize>()
                / 2;
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn small_world_graph_with_big_ks_is_random() {
        let (n, k, beta) = (100, 10, 0.05);
        let graph1 = Graph::new_random_small_world(n, k, beta);
        let graph2 = Graph::new_random_small_world(n, k, beta);
        assert_ne!(graph1, graph2);
    }
}
