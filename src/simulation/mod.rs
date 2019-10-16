use super::*;

pub mod quorum_set_configurators;

impl Fbas {
    /// Add `nodes_to_spawn` new nodes, setting their quorum sets using `qsc`.
    /// Also lets all nodes reevaluate their quorum sets after each new node is added.
    pub fn simulate_growth(&mut self, nodes_to_spawn: usize, qsc: &impl QuorumSetConfigurator) {
        let n = self.nodes.len();
        for i in n..(n + nodes_to_spawn) {
            let public_key = generate_generic_node_name(i);
            let quorum_set = qsc.build_new(self);
            self.add_node(Node {
                public_key,
                quorum_set,
            });
            self.simulate_global_reevaluation(i + 1, qsc);
        }
    }
    /// Make all nodes reevaluate and update their quorum sets using `qsc`, up to
    /// `maximum_number_of_rounds` or until the global configuration has stabilizied (no more
    /// changes happen).
    pub fn simulate_global_reevaluation(
        &mut self,
        maximum_number_of_rounds: usize,
        qsc: &impl QuorumSetConfigurator,
    ) {
        let mut stable = false;
        let mut next_round_number = 0;
        while !stable && next_round_number < maximum_number_of_rounds {

            stable = !self.simulate_global_reevaluation_round(qsc).had_change();
            next_round_number += 1;
        }
    }
    /// Make *all* nodes reevaluate their quorum sets *once*, using `qsc`.
    fn simulate_global_reevaluation_round(&mut self, qsc: &impl QuorumSetConfigurator, ) -> ChangeEffect {
        let mut any_change = NoChange;
        for node_id in 0..self.nodes.len() {
            let change = qsc.change_existing(node_id, self);
            any_change.update(change);
        }
        any_change
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

fn generate_generic_node_name(node_id: NodeId) -> String {
    format!("n{}", node_id)
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simulate_growth_1_to_3_node_fbas() {
        let mut fbas = Fbas::new();
        let qsc = quorum_set_configurators::DummyQsc;
        fbas.simulate_growth(1, &qsc);
        assert_eq!(fbas.nodes, vec![Node::new(generate_generic_node_name(0)),]);
        fbas.simulate_growth(2, &qsc);
        assert_eq!(
            fbas.nodes,
            vec![
                Node::new(generate_generic_node_name(0)),
                Node::new(generate_generic_node_name(1)),
                Node::new(generate_generic_node_name(2)),
            ]
        );
    }

    #[test]
    fn simulate_global_reevaluation_round_can_make_all_nodes_super_safe() {
        let mut fbas = Fbas::new();
        for i in 0..8 {
            fbas.add_node(Node {
                public_key:generate_generic_node_name(i),
                quorum_set: Default::default(),
            });
        }
        let qsc = quorum_set_configurators::SuperSafeQsc;
        fbas.simulate_global_reevaluation_round(&qsc);

        let expected_quorum_set = QuorumSet {
            threshold: 8,
            validators: vec![0,1,2,3,4,5,6,7],
            inner_quorum_sets: vec![],
        };
        let expeted: Vec<QuorumSet> = (0..8).into_iter().map(|_| expected_quorum_set.clone()).collect();
        let actual: Vec<QuorumSet> = fbas.nodes.into_iter().map(|node| node.quorum_set).collect();
        assert_eq!(expeted, actual);
    }

    // #[test]
    // fn simulate_global_reevaluation_stops_once_stable() {
       // todo need to count steps 
    // }
}
