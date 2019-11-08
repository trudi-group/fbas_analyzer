use super::*;

pub struct RandomQsc {
    desired_quorum_set_size: usize,
    desired_threshold: usize,
    weights: Vec<usize>,
}
impl RandomQsc {
    pub fn new(
        desired_quorum_set_size: usize,
        desired_threshold: usize,
        weights: Vec<usize>,
    ) -> Self {
        if desired_threshold > desired_quorum_set_size {
            warn!(
                "Desired threshold higher than desired quorum set size; \
                 will be set to equal quorum set size."
            );
        }
        RandomQsc {
            desired_quorum_set_size,
            desired_threshold,
            weights,
        }
    }
    pub fn new_simple(desired_quorum_set_size: usize, desired_threshold: usize) -> Self {
        Self::new(desired_quorum_set_size, desired_threshold, vec![])
    }
}
impl QuorumSetConfigurator for RandomQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let n = fbas.nodes.len();
        let existing_quorum_set = &mut fbas.nodes[node_id].quorum_set;
        let current_quorum_set_size = existing_quorum_set.validators.len();

        if current_quorum_set_size < self.desired_quorum_set_size {
            let target_quorum_set_size = cmp::min(self.desired_quorum_set_size, n);
            let threshold = cmp::min(target_quorum_set_size, self.desired_threshold);

            let used_nodes: BitSet<NodeId> =
                existing_quorum_set.validators.iter().copied().collect();
            let mut available_nodes: Vec<NodeId> =
                (0..n).filter(|&x| !used_nodes.contains(x)).collect();

            let mut rng = thread_rng();
            for _ in current_quorum_set_size..target_quorum_set_size {
                let &chosen_node = available_nodes
                    .choose_weighted(&mut rng, |&node_id| {
                        *self.weights.get(node_id).unwrap_or(&1)
                    })
                    .unwrap();
                let chosen_idx = available_nodes.binary_search(&chosen_node).unwrap();
                available_nodes.remove(chosen_idx);
                existing_quorum_set.validators.push(chosen_node);
            }
            existing_quorum_set.threshold = threshold;

            Change
        } else {
            NoChange
        }
    }
}

#[cfg(test)]
mod tests {
    use super::monitors::*;
    use super::*;

    #[test]
    fn simple_random_qsc_makes_a_quorum() {
        let mut simulator = Simulator::new(
            Fbas::new(),
            Rc::new(RandomQsc::new_simple(2, 1)),
            Rc::new(DummyMonitor),
        );
        simulator.simulate_growth(3);
        assert!(simulator.fbas.is_quorum(&bitset![0, 1, 2]));
    }

    #[test]
    fn simple_random_qsc_adapts_until_satisfied() {
        let mut simulator_random = Simulator::new(
            Fbas::new(),
            Rc::new(RandomQsc::new_simple(5, 3)),
            Rc::new(DummyMonitor),
        );
        let mut simulator_safe = Simulator::new(
            Fbas::new(),
            Rc::new(SuperSafeQsc::new()),
            Rc::new(DummyMonitor),
        );
        simulator_random.simulate_growth(2);
        simulator_safe.simulate_growth(2);

        assert!(simulator_random.fbas.is_quorum(&bitset![0, 1]));

        simulator_random.simulate_growth(10);
        simulator_safe.simulate_growth(10);

        assert_ne!(simulator_safe.fbas, simulator_random.fbas);
        assert!(!simulator_random.fbas.is_quorum(&bitset![0, 1]));
    }

    #[test]
    fn simple_random_qsc_is_random() {
        let mut simulator_random_1 = Simulator::new(
            Fbas::new(),
            Rc::new(RandomQsc::new_simple(5, 3)),
            Rc::new(DummyMonitor),
        );
        let mut simulator_random_2 = simulator_random_1.clone();
        simulator_random_1.simulate_growth(23);
        simulator_random_2.simulate_growth(23);

        assert_ne!(simulator_random_1.fbas, simulator_random_2.fbas);
    }

    #[test]
    fn random_qsc_honors_weights() {
        let mut simulator = Simulator::new(
            Fbas::new_generic_unconfigured(10),
            Rc::new(RandomQsc::new(5, 3, vec![0, 0, 0, 0, 0, 1, 1, 1, 1, 1])),
            Rc::new(DummyMonitor),
        );
        simulator.simulate_global_reevaluation(2);
        assert!(!simulator.fbas.is_quorum(&bitset![0, 1, 2, 3, 4, 5, 6]));
        assert!(simulator.fbas.is_quorum(&bitset![7, 8, 9]));
    }
}
