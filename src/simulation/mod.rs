use super::*;

use rand::seq::SliceRandom;
use rand::thread_rng;

pub mod monitors;
pub mod quorum_set_configurators;

pub struct Simulator {
    fbas: Fbas,
    qsc: Box<dyn QuorumSetConfigurator>,
    monitor: Box<dyn SimulationMonitor>,
}
impl Simulator {
    pub fn new(
        fbas: Fbas,
        qsc: impl QuorumSetConfigurator + 'static,
        monitor: impl SimulationMonitor + 'static,
    ) -> Self {
        Simulator {
            fbas,
            qsc: Box::new(qsc),
            monitor: Box::new(monitor),
        }
    }
    /// Get the contained FBAS, effectively ending the simulation
    pub fn finalize(self) -> Fbas {
        self.fbas
    }
    /// Add `nodes_to_spawn` new nodes, setting their quorum sets using `qsc`.
    /// Also lets all nodes reevaluate their quorum sets after each new node is added.
    pub fn simulate_growth(&mut self, nodes_to_spawn: usize) {
        let n = self.fbas.nodes.len();
        for i in n..(n + nodes_to_spawn) {
            let public_key = generate_generic_node_name(i);
            let quorum_set = self.qsc.build_new(&self.fbas);
            self.fbas.add_node(Node {
                public_key,
                quorum_set,
            });
            self.simulate_global_reevaluation(i + 1);
        }
    }
    /// Make all nodes reevaluate and update their quorum sets using `qsc`, up to
    /// `maximum_number_of_rounds` or until the global configuration has stabilizied (no more
    /// changes happen).
    ///
    /// Returns the number of reevaluation rounds made.
    pub fn simulate_global_reevaluation(&mut self, maximum_number_of_rounds: usize) -> usize {
        let mut stable = false;
        let mut next_round_number = 0;

        // Visit nodes in random order each time
        let mut order: Vec<NodeId> = (0..self.fbas.nodes.len()).collect();
        let mut rng = thread_rng();

        while !stable && next_round_number < maximum_number_of_rounds {
            order.shuffle(&mut rng);
            stable = !self.simulate_global_reevaluation_round(&order).had_change();
            next_round_number += 1;
        }
        next_round_number
    }
    /// Make *all* nodes reevaluate their quorum sets *once*, using `qsc`.
    fn simulate_global_reevaluation_round(&mut self, order: &[NodeId]) -> ChangeEffect {
        let mut any_change = NoChange;
        for node_id in order {
            let change = self.qsc.change_existing(*node_id, &mut self.fbas);
            any_change.update(change);
        }
        any_change
    }
}

impl Fbas {
    /// Creates a generate generic dummy network of size `n`, full of nodes with empty quorum sets
    pub fn new_generic(n: usize) -> Self {
        let mut fbas = Fbas::new();
        for i in 0..n {
            fbas.add_node(Node {
                public_key: generate_generic_node_name(i),
                quorum_set: Default::default(),
            });
        }
        fbas
    }
}

pub trait QuorumSetConfigurator {
    fn build_new(&self, fbas: &Fbas) -> QuorumSet;
    fn change_existing(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let candidate = self.build_new(fbas);
        let existing = &mut fbas.nodes[node_id].quorum_set;
        if candidate == *existing {
            NoChange
        } else {
            *existing = candidate;
            Change
        }
    }
}

pub trait SimulationMonitor {
    fn register_event(&mut self, event: Event);
}

#[derive(PartialEq)]
pub enum Event {
    AddNode(NodeId),
    StartGlobalReevaluation,
    StartGolvalReevaluationRound,
    EndGlobalReevaluation,
    QuorumSetChange(NodeId, ChangeEffect),
}
use Event::*;

#[derive(PartialEq)]
pub enum ChangeEffect {
    Change,
    NoChange,
}
impl ChangeEffect {
    fn had_change(&self) -> bool {
        *self == Change
    }
    fn update(&mut self, other: ChangeEffect) {
        if *self == ChangeEffect::NoChange {
            *self = other;
        }
    }
}
use ChangeEffect::*;

fn generate_generic_node_name(node_id: NodeId) -> String {
    format!("n{}", node_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simulate_growth_1_to_3_node_fbas() {
        let mut simulator = Simulator::new(
            Fbas::new(),
            quorum_set_configurators::DummyQsc,
            monitors::DummyMonitor,
        );
        simulator.simulate_growth(1);
        assert_eq!(
            simulator.fbas.nodes,
            vec![Node::new(generate_generic_node_name(0)),]
        );
        simulator.simulate_growth(2);
        assert_eq!(
            simulator.fbas.nodes,
            vec![
                Node::new(generate_generic_node_name(0)),
                Node::new(generate_generic_node_name(1)),
                Node::new(generate_generic_node_name(2)),
            ]
        );
    }

    #[test]
    fn simulate_global_reevaluation_round_can_make_all_nodes_super_safe() {
        let mut simulator = Simulator::new(
            Fbas::new_generic(8),
            quorum_set_configurators::SuperSafeQsc,
            monitors::DummyMonitor,
        );
        simulator.simulate_global_reevaluation_round(&[0, 1, 2, 3, 4, 5, 6, 7]);

        let expected_quorum_set = QuorumSet {
            threshold: 8,
            validators: vec![0, 1, 2, 3, 4, 5, 6, 7],
            inner_quorum_sets: vec![],
        };
        let expeted: Vec<QuorumSet> = (0..8)
            .into_iter()
            .map(|_| expected_quorum_set.clone())
            .collect();
        let actual: Vec<QuorumSet> = simulator
            .fbas
            .nodes
            .into_iter()
            .map(|node| node.quorum_set)
            .collect();
        assert_eq!(expeted, actual);
    }

    #[test]
    fn simulate_global_reevaluation_stops_once_stable() {
        let mut simulator = Simulator::new(
            Fbas::new_generic(8),
            quorum_set_configurators::SuperSafeQsc,
            monitors::DummyMonitor,
        );

        let number_of_rounds = simulator.simulate_global_reevaluation(1000000);
        assert_eq!(number_of_rounds, 2);
    }

    // #[test]
    // fn simulate_global_reevaluation_visits_in_random_order() {
    // TODO
    //     let mut fbas = Fbas::new_generate_generic(8);
    //     let qsc = quorum_set_configurators::SuperSafeQsc;
    // }
}
