use super::*;

impl Fbas {
    /// This corresponds to the *delete* operation from Mazières's original FBAS/SCP paper.
    /// For keeping the IDs correct, it doesn't delete the node entirely but only makes it
    /// unsatisfiable and redacts it from all quorum sets.
    pub fn assume_faulty(&mut self, nodes: &NodeIdSet) {
        for node_id in nodes.iter() {
            self.nodes[node_id].quorum_set = QuorumSet::new_unsatisfiable();
        }
        for node in self.nodes.iter_mut() {
            node.assume_faulty(nodes);
        }
    }
}
impl Node {
    /// This corresponds to the *delete* operation from Mazières's original FBAS/SCP paper.
    pub fn assume_faulty(&mut self, nodes: &NodeIdSet) {
        self.quorum_set.assume_faulty(nodes);
    }
}
impl QuorumSet {
    /// This corresponds to the *delete* operation from Mazières's original FBAS/SCP paper.
    pub fn assume_faulty(&mut self, nodes: &NodeIdSet) {
        let n_validators_before = self.validators.len();
        self.validators = self
            .validators
            .iter()
            .copied()
            .filter(|&x| !nodes.contains(x))
            .collect();
        let n_validator_deletions = n_validators_before - self.validators.len();

        for iqs in self.inner_quorum_sets.iter_mut() {
            iqs.assume_faulty(nodes);
        }
        self.threshold = self.threshold.saturating_sub(n_validator_deletions);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assume_faulty_makes_nodes_unsatisfiable() {
        let mut fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"], "innerQuorumSets": [] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"], "innerQuorumSets": [] }
            }
        ]"#,
        );
        fbas.assume_faulty(&bitset! {0, 1});
        assert!(!fbas.nodes[0].quorum_set.is_satisfiable());
        assert!(!fbas.nodes[1].quorum_set.is_satisfiable());
    }

    #[test]
    fn assume_faulty_removes_from_quorum_sets() {
        let mut fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"], "innerQuorumSets": [] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"], "innerQuorumSets": [] }
            }
        ]"#,
        );
        fbas.assume_faulty(&bitset! {1});
        let actual = fbas.nodes[0].quorum_set.clone();
        let expected = QuorumSet::new(vec![0], vec![], 1);
        assert_eq!(expected, actual);
    }

    #[test]
    fn assume_faulty_works_on_a_more_complex_fbas() {
        let mut fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1", "n2"], "innerQuorumSets": [] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 3, "validators": ["n0", "n1"], "innerQuorumSets": [
                    { "threshold": 2, "validators": ["n2", "n3"] }
                ]}
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 4, "validators": ["n0", "n1", "n2", "n3"], "innerQuorumSets": [] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 4, "validators": ["n1", "n2", "n3", "n4"], "innerQuorumSets": [] }
            }
        ]"#,
        );
        fbas.assume_faulty(&bitset! {0, 2});
        let actual = fbas;
        let expected = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0"
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n1"], "innerQuorumSets": [
                    { "threshold": 1, "validators": ["n3"] }
                ]}
            },
            {
                "publicKey": "n2"
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 3, "validators": ["n1", "n3", "n4"], "innerQuorumSets": [] }
            }
        ]"#,
        );
        assert_eq!(expected, actual);
    }
}
