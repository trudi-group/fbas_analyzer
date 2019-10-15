use super::*;
use log::info;

impl Fbas {
    pub fn evolve(&mut self, nodes_to_spawn: usize, qsc: &impl QuorumSetConfigurator) {
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

/// Creates empty quorum sets
struct DummyQsc;
impl QuorumSetConfigurator for DummyQsc {
    fn build_new(&self, fbas: &Fbas) -> QuorumSet {
        QuorumSet::new()
    }
}

/// Creates threshold=1 quorum sets containing all nodes in the FBAS
struct SuperLiveQsc;
impl QuorumSetConfigurator for SuperLiveQsc {
    /// Also includes the "next" node (most likely the one currently being created). This solves
    /// the bootstrapping problem that the first node otherwise doesn't have a valid quorum.
    fn build_new(&self, fbas: &Fbas) -> QuorumSet {
        let threshold = 1;
        let validators = (0..fbas.nodes.len() + 1).collect();
        let inner_quorum_sets = vec![];
        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
    }
}

/// Creates threshold=n quorum sets containing all n nodes in the FBAS
struct SuperSafeQsc;
impl QuorumSetConfigurator for SuperSafeQsc {
    /// Also counts the "next" node (most likely the one currently being created). This solves
    /// the bootstrapping problem that the first node otherwise doesn't have a valid quorum.
    fn build_new(&self, fbas: &Fbas) -> QuorumSet {
        let n = fbas.nodes.len() + 1;
        let threshold = n;
        let validators = (0..n).collect();
        let inner_quorum_sets = vec![];
        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evolve_1_to_3_node_fbas() {
        let mut fbas = Fbas::new();
        let qsc = DummyQsc {};
        fbas.evolve(1, &qsc);
        assert_eq!(fbas.nodes, vec![Node::new(generate_generic_node_name(0)),]);
        fbas.evolve(2, &qsc);
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
    fn super_live_fbas_has_quorums() {
        let mut fbas = Fbas::new();
        let qsc = SuperLiveQsc {};
        fbas.evolve(3, &qsc);
        assert!(fbas.is_quorum(&bitset![0]));
        assert!(fbas.is_quorum(&bitset![1]));
        assert!(fbas.is_quorum(&bitset![2]));
        assert!(fbas.is_quorum(&bitset![0, 1, 2]));
    }

    #[test]
    fn super_safe_fbas_has_a_quorum() {
        let mut fbas = Fbas::new();
        let qsc = SuperSafeQsc {};
        fbas.evolve(3, &qsc);
        assert!(fbas.is_quorum(&bitset![0, 1, 2]));
    }

    #[test]
    fn super_live_fbas_has_no_quorum_intersection() {
        let mut fbas = Fbas::new();
        let qsc = SuperLiveQsc {};
        fbas.evolve(3, &qsc);
        assert!(!fbas.has_quorum_intersection());
    }

    #[test]
    fn super_safe_fbas_has_quorum_intersection() {
        let mut fbas = Fbas::new();
        let qsc = SuperSafeQsc {};
        fbas.evolve(8, &qsc);
        assert!(fbas.has_quorum_intersection());
    }
}
