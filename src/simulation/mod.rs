use super::*;

use std::rc::Rc;

use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};

pub mod monitors;
pub mod quorum_set_configurators;

#[derive(Clone)]
pub struct Simulator {
    fbas: Fbas,
    qsc: Rc<dyn QuorumSetConfigurator>,
    monitor: Rc<dyn SimulationMonitor>,
}
impl Simulator {
    pub fn new(
        fbas: Fbas,
        qsc: Rc<dyn QuorumSetConfigurator>,
        monitor: Rc<dyn SimulationMonitor>,
    ) -> Self {
        Simulator { fbas, qsc, monitor }
    }
    /// Get the contained FBAS, effectively ending the simulation
    pub fn finalize(self) -> Fbas {
        self.fbas
    }
    /// Add `nodes_to_spawn` new nodes, setting their quorum sets using `qsc`.
    /// Also lets all nodes reevaluate their quorum sets after each new node is added.
    pub fn simulate_growth(&mut self, nodes_to_spawn: usize) {
        for _ in 0..nodes_to_spawn {
            let node_id = self.fbas.add_generic_node(QuorumSet::new());
            self.qsc.configure(node_id, &mut self.fbas);
            self.monitor.register_event(AddNode(node_id));
            self.simulate_global_reevaluation(self.fbas.number_of_nodes());
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

        self.monitor.register_event(StartGlobalReevaluation);

        while !stable && next_round_number < maximum_number_of_rounds {
            order.shuffle(&mut rng);
            stable = !self.simulate_global_reevaluation_round(&order).had_change();
            next_round_number += 1;
        }
        let number_of_rounds = next_round_number;
        self.monitor
            .register_event(FinishGlobalReevaluation(number_of_rounds));
        number_of_rounds
    }
    /// Make *all* nodes reevaluate their quorum sets *once*, using `qsc`.
    fn simulate_global_reevaluation_round(&mut self, order: &[NodeId]) -> ChangeEffect {
        self.monitor.register_event(StartGlobalReevaluationRound);
        let mut any_change = NoChange;
        for &node_id in order {
            let change = self.qsc.configure(node_id, &mut self.fbas);
            any_change.update(change);
            self.monitor
                .register_event(QuorumSetChange(node_id, change));
        }
        any_change
    }
}

pub trait QuorumSetConfigurator {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect;
}

pub trait SimulationMonitor {
    fn register_event(&self, event: Event);
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Event {
    AddNode(NodeId),
    StartGlobalReevaluation,
    StartGlobalReevaluationRound,
    FinishGlobalReevaluation(usize),
    QuorumSetChange(NodeId, ChangeEffect),
}
use Event::*;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum ChangeEffect {
    Change,
    NoChange,
}
impl ChangeEffect {
    fn had_change(self) -> bool {
        self == Change
    }
    fn update(&mut self, other: ChangeEffect) {
        if *self == ChangeEffect::NoChange {
            *self = other;
        }
    }
}
use ChangeEffect::*;

impl Fbas {
    /// FBAS of `n` nodes with empty quorum sets
    pub fn new_generic_unconfigured(n: usize) -> Self {
        let mut fbas = Fbas::new();
        for _ in 0..n {
            fbas.add_generic_node(QuorumSet::new());
        }
        fbas
    }
    /// Add a node with generic "`public_key`"
    pub fn add_generic_node(&mut self, quorum_set: QuorumSet) -> NodeId {
        let node_id = self.nodes.len();
        self.add_node(Node {
            public_key: generate_generic_node_name(node_id),
            quorum_set,
        });
        node_id
    }
}
fn generate_generic_node_name(node_id: NodeId) -> String {
    format!("n{}", node_id)
}

#[cfg(test)]
mod tests {
    use super::monitors::*;
    use super::quorum_set_configurators::*;
    use super::*;

    #[test]
    fn growth_with_interruptions() {
        let mut simulator = Simulator::new(Fbas::new(), Rc::new(DummyQsc), Rc::new(DummyMonitor));
        simulator.simulate_growth(3);
        assert_eq!(simulator.fbas, Fbas::new_generic_unconfigured(3));
        simulator.simulate_growth(5);
        assert_eq!(simulator.finalize(), Fbas::new_generic_unconfigured(8));
    }

    #[test]
    fn monitoring_works() {
        let monitor = Rc::new(DebugMonitor::new());
        let mut simulator = Simulator::new(
            Fbas::new(),
            Rc::new(DummyQsc),
            Rc::clone(&monitor) as Rc<dyn SimulationMonitor>,
        );
        assert!(monitor.events_ref().is_empty());
        simulator.simulate_growth(1);
        assert!(!monitor.events_ref().is_empty());
    }

    #[test]
    fn global_reevaluation_round_can_make_all_nodes_super_safe() {
        let mut simulator = Simulator::new(
            Fbas::new_generic_unconfigured(8),
            Rc::new(SuperSafeQsc),
            Rc::new(DummyMonitor),
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
    fn global_reevaluation_stops_once_stable() {
        let mut simulator = Simulator::new(
            Fbas::new_generic_unconfigured(8),
            Rc::new(SuperSafeQsc),
            Rc::new(DummyMonitor),
        );
        let number_of_rounds = simulator.simulate_global_reevaluation(1000000);
        assert_eq!(number_of_rounds, 2);
    }

    #[test]
    fn global_reevaluation_visits_in_random_order() {
        let monitor = Rc::new(DebugMonitor::new());
        let mut simulator = Simulator::new(
            Fbas::new_generic_unconfigured(128),
            Rc::new(SuperSafeQsc),
            Rc::clone(&monitor) as Rc<dyn SimulationMonitor>,
        );
        simulator.simulate_global_reevaluation(2);

        let events: Vec<Event> = monitor.events_clone();
        let rounds = events
            .split(|&event| event == StartGlobalReevaluationRound)
            .skip(1);

        let orderings: Vec<Vec<NodeId>> = rounds
            .map(|round| {
                round
                    .into_iter()
                    .filter_map(|&event| match event {
                        QuorumSetChange(id, _) => Some(id),
                        _ => None,
                    })
                    .collect()
            })
            .collect();
        assert_eq!(orderings.len(), 2);
        assert_ne!(orderings[0], orderings[1]);
    }
}
