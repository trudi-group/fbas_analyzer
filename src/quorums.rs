use super::*;
use bit_set::BitSet;

impl Node {
    pub fn is_quorum(&self, node_set: &BitSet) -> bool {
        self.quorum_set.is_quorum(node_set)
    }
}
impl QuorumSet {
    pub fn is_quorum(&self, node_set: &BitSet) -> bool {
        let mut remaining_threshold = self.threshold;
        let mut remaining_chances = self.validators.len() + self.inner_quorum_sets.len();

        for validator in self.validators.iter() {
            if node_set.contains(*validator) {
                remaining_threshold -= 1;
            }
            remaining_chances -= 1;
            if remaining_threshold == 0 {
                return true;
            } else if remaining_chances < remaining_threshold {
                return false;
            }
        }
        for inner_quorum_set in self.inner_quorum_sets.iter() {
            if remaining_threshold == 0 {
                return true;
            } else if remaining_chances < remaining_threshold {
                return false;
            } else if inner_quorum_set.is_quorum(node_set) {
                remaining_threshold -= 1;
            }
            remaining_chances -= 1;
        }
        return false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node(validators: &[NodeID], threshold: usize) -> Node {
        Node {
            public_key: Default::default(),
            quorum_set: QuorumSet {
                threshold: threshold,
                validators: validators.iter().copied().collect(),
                inner_quorum_sets: vec![],
            },
        }
    }

    #[test]
    fn is_quorum_if_not_quorum() {
        let node = test_node(&[0, 1, 2], 3);
        let node_set = &[1, 2, 3].iter().copied().collect();
        assert!(!node.is_quorum(&node_set));
    }

    #[test]
    fn is_quorum_if_quorum() {
        let node = test_node(&[0, 1, 2], 2);
        let node_set = &[1, 2, 3].iter().copied().collect();
        assert!(node.is_quorum(&node_set));
    }
}
