use super::*;
use log::info;

// impl Fbas {
//     fn evolve(target_number_of_nodes: usize) -> Self {
//     }
// }

impl Node {
    fn new_generic(id: NodeId) -> Self {
        let public_key = format!("node {}", id);
        let quorum_set = Default::default();
        Node {
            public_key,
            quorum_set,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_generic_node() {
        let node = Node::new_generic(42);
        assert_eq!(node.public_key, "node 42");
        assert_eq!(
            node.quorum_set,
            QuorumSet {
                threshold: 0,
                validators: vec![],
                inner_quorum_sets: vec![]
            }
        );
    }
}
