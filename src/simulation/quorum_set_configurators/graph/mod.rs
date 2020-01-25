use super::*;

mod simple;
pub use simple::*;
mod quality;
pub use quality::*;
mod tier_based;
pub use tier_based::*;
mod rank_based;
pub use rank_based::*;

#[derive(Clone, Debug, PartialEq)]
pub struct Graph {
    // outgoing edges per node
    outlinks: Vec<Vec<NodeId>>,
}
impl Graph {
    pub fn new(outlinks: Vec<Vec<NodeId>>) -> Self {
        info!("New graph with {} nodes.", outlinks.len());
        Graph { outlinks }
    }
    /// Build a graph where every node is connected to every other node (i.e., a complete graph).
    pub fn new_full_mesh(n: usize) -> Self {
        Self::new((0..n).map(|i| (0..i).chain(i + 1..n).collect()).collect())
    }
    /// Build a graph consisting of tiers. Each tier is (conceptually) a fully meshed (i.e.,
    /// complete) subraph. Each node is connected via a directed edge to each node from the next
    /// "highest" tier, i.e., the tier with the next lowest index.
    #[allow(clippy::needless_range_loop)]
    pub fn new_tiered_full_mesh(tier_sizes: &[usize]) -> Self {
        let mut outlinks: Vec<Vec<NodeId>> = vec![vec![]; tier_sizes.iter().sum()];
        let mut higher_tier_node_ids = vec![];
        for tier_size in tier_sizes.iter() {
            let i0: usize = higher_tier_node_ids.last().map_or(0, |x| x + 1);
            let n = i0.checked_add(*tier_size).unwrap();
            for i in i0..n {
                outlinks[i].extend_from_slice(&higher_tier_node_ids);
                outlinks[i].extend((i0..i).chain(i + 1..n));
            }
            higher_tier_node_ids = (i0..n).collect();
        }
        Self::new(outlinks)
    }
    /// Build a scale-free graph using the Barabási–Albert (BA) model
    pub fn new_random_scale_free(n: usize, m0: usize, m: usize) -> Self {
        assert!(
            0 < m && m <= m0 && m <= n,
            "Parameters for Barabási–Albert don't make sense."
        );

        let mut outlinks: Vec<Vec<NodeId>> = vec![vec![]; n];
        let mut rng = thread_rng();

        macro_rules! connect {
            ($a:expr, $b:expr) => {
                let (a, b) = ($a, $b);
                debug_assert_ne!(a, b);
                outlinks[a].push(b);
                outlinks[b].push(a);
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
                    .choose_weighted(&mut rng, |&x| outlinks[x].len())
                    .unwrap()
                    .to_owned();
                connect!(i, j);
                // remove j from possible targets
                possible_targets = possible_targets.into_iter().filter(|&x| x != j).collect();
            }
        }
        let result = Self::new(outlinks);
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
        let mut possible_targets: Vec<Vec<NodeId>> = vec![Vec::with_capacity(n - k); n];
        for i in 0..n {
            for j in 0..n {
                if i != j && !matrix[i][j] && !matrix[j][i] {
                    possible_targets[i].push(j);
                }
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
                let chosen_node = possible_targets[i].choose(&mut rng);
                if let Some(&newj) = chosen_node {
                    //rewire
                    matrix[i][j] = false;
                    matrix[j][i] = false;
                    matrix[i][newj] = true;
                    matrix[newj][i] = true;
                    possible_targets[i].push(j);
                    possible_targets[j].push(i);
                    possible_targets[i].retain(|&x| x != newj);
                    possible_targets[newj].retain(|&x| x != i);
                }
            }
        }

        // transform to data format used here
        let mut outlinks = vec![vec![]; n];
        for i in 0..n {
            for j in 0..n {
                if matrix[i][j] {
                    outlinks[i].push(j);
                }
            }
        }
        let result = Self::new(outlinks);
        debug_assert!(result.is_undirected());
        result
    }
    /// Shuffle the node IDs
    pub fn shuffled(self) -> Self {
        let n = self.outlinks.len();
        let mut rng = thread_rng();

        // mappings
        let mut old_to_new: Vec<NodeId> = (0..n).collect();
        old_to_new.shuffle(&mut rng);
        let mut new_to_old = vec![0; n];
        for (old, &new) in old_to_new.iter().enumerate() {
            new_to_old[new] = old;
        }
        let (new_to_old, old_to_new) = (new_to_old, old_to_new);

        let new_outlinks = new_to_old
            .iter()
            .map(|&oi| self.outlinks[oi].iter().map(|&oj| old_to_new[oj]).collect())
            .collect();
        Self::new(new_outlinks)
    }
    pub fn is_undirected(&self) -> bool {
        self.outlinks.iter().enumerate().all(|(i, cons_i)| {
            cons_i
                .iter()
                .map(|&j| &self.outlinks[j])
                .all(|cons_j| cons_j.iter().any(|&x| x == i))
        })
    }
    pub fn number_of_nodes(&self) -> usize {
        self.outlinks.len()
    }
    pub fn get_in_degrees(&self) -> Vec<usize> {
        let mut result: Vec<usize> = vec![0; self.outlinks.len()];
        for outlink in self.outlinks.iter() {
            for &in_node in outlink.iter() {
                result[in_node] = result[in_node].checked_add(1).unwrap();
            }
        }
        result
    }
    pub fn get_out_degrees(&self) -> Vec<usize> {
        self.outlinks.iter().map(|x| x.len()).collect()
    }
    /// Simplified page rank (no dampening, fixed maximum number of runs, fixed epsilon)
    #[allow(clippy::needless_range_loop)]
    pub fn get_rank_scores(&self) -> Vec<RankScore> {
        let n = self.number_of_nodes();
        let max_runs = 1000;
        let epsilon = 0.00001;

        let starting_score = 1. / n as RankScore;
        let mut scores: Vec<RankScore> = vec![starting_score; n];
        let mut last_scores: Vec<RankScore>;

        for _ in 0..max_runs {
            last_scores = scores;
            scores = vec![0.; n];

            for i in 0..n {
                let l = self.outlinks[i].len() as RankScore;
                for j in self.outlinks[i].iter().copied() {
                    scores[j] += last_scores[i] / l;
                }
            }
            if scores
                .iter()
                .zip(last_scores.iter())
                .all(|(&x, &y)| (x - y).abs() < epsilon)
            {
                break;
            }
        }
        scores
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_mesh() {
        let expected = Graph {
            outlinks: vec![vec![1, 2, 3], vec![0, 2, 3], vec![0, 1, 3], vec![0, 1, 2]],
        };
        let actual = Graph::new_full_mesh(4);
        assert_eq!(expected, actual);
    }

    #[test]
    fn tiered_full_mesh() {
        let expected = Graph {
            outlinks: vec![
                vec![1],
                vec![0],
                vec![0, 1, 3, 4],
                vec![0, 1, 2, 4],
                vec![0, 1, 2, 3],
                vec![2, 3, 4],
            ],
        };
        let actual = Graph::new_tiered_full_mesh(&vec![2, 3, 1]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn scale_free_graph_interconnects_m0_fully() {
        let (n, m0, m) = (23, 8, 2);
        let graph = Graph::new_random_scale_free(n, m0, m);

        assert!((0..m0).all(|i| (0..i)
            .chain(i + 1..m0)
            .all(|j| graph.outlinks[j].iter().any(|&x| x == i))));
    }

    #[test]
    fn scale_free_graph_has_sane_number_of_edges() {
        let (n, m0, m) = (23, 3, 2);
        let graph = Graph::new_random_scale_free(n, m0, m);

        let expected = (m0 * (m0 - 1)) / 2 + (n - m0) * m;
        let actual: usize = graph.outlinks.into_iter().map(|x| x.len()).sum::<usize>() / 2;
        assert_eq!(expected, actual);
    }

    #[test]
    fn scale_free_graph_doesnt_panic_on_exotic_m_values() {
        let (n, m0, m) = (40, 4, 4);
        Graph::new_random_scale_free(n, m0, m);
    }

    #[test]
    fn small_world_graph_has_sane_number_of_edges() {
        let (n, k, beta) = (100, 10, 0.05);
        let graph = Graph::new_random_small_world(n, k, beta);

        let expected = n * k / 2;
        let actual: usize = graph.outlinks.into_iter().map(|x| x.len()).sum::<usize>() / 2;
        assert_eq!(expected, actual);
    }

    #[test]
    fn small_world_graph_with_big_k_has_sane_number_of_edges() {
        let (n, k, beta) = (80, 78, 0.05);
        let graph = Graph::new_random_small_world(n, k, beta);

        let expected = n * k / 2;
        let actual: usize = graph.outlinks.into_iter().map(|x| x.len()).sum::<usize>() / 2;
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
    fn small_world_graph_with_big_k_is_random() {
        let (n, k, beta) = (120, 110, 0.05);
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
            let mut result: Vec<usize> = graph.outlinks.into_iter().map(|x| x.len()).collect();
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

        let expected: Vec<usize> = graph.outlinks.iter().map(|x| x.len()).collect();

        assert_eq!(expected, graph.get_in_degrees());
        assert_eq!(expected, graph.get_out_degrees());
    }

    #[test]
    fn node_out_degrees_directed() {
        let graph = Graph::new_tiered_full_mesh(&vec![2, 3, 1]);
        let actual = graph.get_out_degrees();
        let expected = vec![1, 1, 4, 4, 4, 3];
        assert_eq!(expected, actual);
    }

    #[test]
    fn node_in_degrees_directed() {
        let graph = Graph::new_tiered_full_mesh(&vec![2, 3, 1]);
        let actual = graph.get_in_degrees();
        let expected = vec![4, 4, 3, 3, 3, 0];
        assert_eq!(expected, actual);
    }

    #[test]
    fn node_rank_scores() {
        let graph = Graph::new_tiered_full_mesh(&vec![2, 3, 1]);
        let actual: Vec<RankScore> = graph
            .get_rank_scores()
            .into_iter()
            .map(|x| (x * 2.).round() as RankScore / 2.) // rounding
            .collect();
        let expected = vec![0.5, 0.5, 0., 0., 0., 0.];
        assert_eq!(expected, actual);
    }
}
