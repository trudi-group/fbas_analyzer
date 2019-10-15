use super::*;
use log::info;

impl Fbas {
    pub fn evolve(&mut self, nodes_to_spawn: usize) {
        let n = self.nodes.len();
        for i in n..(n + nodes_to_spawn) {
            self.add_node(Node::new_generic(i));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evolve_1_node_fbas() {
        let mut fbas = Fbas::new();
        fbas.evolve(1);
        assert_eq!(fbas.nodes, vec![Node::new_generic(0)]);
    }

    #[test]
    fn evolve_1_to_3_node_fbas() {
        let mut fbas = Fbas::new();
        fbas.evolve(1);
        assert_eq!(fbas.nodes, vec![Node::new_generic(0)]);
        fbas.evolve(2);
        assert_eq!(
            fbas.nodes,
            vec![
                Node::new_generic(0),
                Node::new_generic(1),
                Node::new_generic(2),
            ]
        );
    }
}
