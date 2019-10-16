use super::*;

pub mod quorum_set_configurators;

impl Fbas {
    /// Add `nodes_to_spawn` new nodes, setting their quorum sets using `qsc`
    pub fn simulate_growth(&mut self, nodes_to_spawn: usize, qsc: &impl QuorumSetConfigurator) {
        let n = self.nodes.len();
        for i in n..(n + nodes_to_spawn) {
            let public_key = generate_generic_node_name(i);
            let quorum_set = qsc.build_new(self);
            self.add_node(Node {
                public_key,
                quorum_set,
            });
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
    #[allow(unused_variables)]
    fn change_existing(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        NoChange
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
}
