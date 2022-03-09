use super::*;

impl<'fbas> Groupings<'fbas> {
    /// Merge a node ID so that all nodes by the same grouping get the same ID.
    pub fn merge_node(&self, node_id: NodeId) -> NodeId {
        self.merged_ids[node_id]
    }
    /// Merge a node ID set so that all nodes by the same grouping get the same ID.
    pub fn merge_node_set(&self, node_set: NodeIdSet) -> NodeIdSet {
        node_set.into_iter().map(|x| self.merge_node(x)).collect()
    }
    /// Merge a list of node ID sets so that all nodes by the same grouping get the same ID.
    pub fn merge_node_sets(&self, node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
        node_sets
            .into_iter()
            .map(|x| self.merge_node_set(x))
            .collect()
    }
    /// Merge a list of node ID sets so that all nodes by the same grouping get the same ID and
    /// the returned node sets are all minimal w.r.t. each other (none is a superset of another).
    pub fn merge_minimal_node_sets(&self, node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
        remove_non_minimal_node_sets(self.merge_node_sets(node_sets))
    }
    /// Merge a quorum set so that all nodes by the same grouping get the same ID and
    /// validator lists consisting of only of one grouping are collapsed into one validator.
    pub fn merge_quorum_set(&self, quorum_set: QuorumSet) -> QuorumSet {
        let mut threshold = quorum_set.threshold;
        let mut validators: Vec<NodeId> = quorum_set
            .validators
            .iter()
            .map(|&x| self.merge_node(x))
            .collect();

        let (new_validator_candidates, inner_quorum_sets): (Vec<QuorumSet>, Vec<QuorumSet>) =
            quorum_set
                .inner_quorum_sets
                .into_iter()
                .map(|q| self.merge_quorum_set(q))
                .partition(|q| q.validators.len() == 1);

        validators.extend(
            new_validator_candidates
                .into_iter()
                .map(|q| q.validators[0]),
        );
        if !validators.is_empty() && validators.iter().all(|&v| v == validators[0]) {
            validators = vec![validators[0]];
            threshold = 1;
        }
        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
    }
    /// calls `merge_quorum_set` on each vector element
    pub fn merge_quorum_sets(&self, quorum_set: Vec<QuorumSet>) -> Vec<QuorumSet> {
        quorum_set
            .into_iter()
            .map(|q| self.merge_quorum_set(q))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_node_sets_by_organization() {
        let fbas_input = r#"[
            {
                "publicKey": "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH"
            },
            {
                "publicKey": "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ"
            },
            {
                "publicKey": "GCWJKM4EGTGJUVSWUJDPCQEOEP5LHSOFKSA4HALBTOO4T4H3HCHOM6UX"
            }]"#;
        let organizations_input = r#"[
            {
                "id": "266107f8966d45eedce41fee2581326d",
                "name": "Stellar Development Foundation",
                "validators": [
                    "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK",
                    "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                    "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ"
                ]
            }]"#;
        let fbas = Fbas::from_json_str(fbas_input);
        let organizations = Groupings::organizations_from_json_str(organizations_input, &fbas);

        let node_sets = vec![bitset![0], bitset![1, 2]];

        let expected = vec![bitset![0], bitset![0, 2]];
        let actual = organizations.merge_node_sets(node_sets);

        assert_eq!(expected, actual);
    }

    #[test]
    fn merge_node_sets_by_isp() {
        let fbas_input = r#"[
            {
                "publicKey": "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                "isp": "Hetzner Gmbh"
            },
            {
                "publicKey": "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ",
                "isp": "Hetzner Gmbh"
            },
            {
                "publicKey": "GCWJKM4EGTGJUVSWUJDPCQEOEP5LHSOFKSA4HALBTOO4T4H3HCHOM6UX",
                "isp": "Microsoft"
            }]"#;
        let fbas = Fbas::from_json_str(fbas_input);
        let isps = Groupings::isps_from_json_str(fbas_input, &fbas);

        let node_sets = vec![bitset![0], bitset![1, 2]];

        let expected = vec![bitset![0], bitset![0, 2]];
        let actual = isps.merge_node_sets(node_sets);

        assert_eq!(expected, actual);
    }
}
