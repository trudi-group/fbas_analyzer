use super::*;

pub mod quorum_set_configurators;

impl Fbas {
    /// Simulate the adding of new nodes
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
}

fn generate_generic_node_name(node_id: NodeId) -> String {
    format!("n{}", node_id)
}

pub trait QuorumSetConfigurator {
    fn build_new(&self, fbas: &Fbas) -> QuorumSet;
}

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
