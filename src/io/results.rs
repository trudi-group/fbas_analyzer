use super::*;
use std::convert::TryInto;

#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PrettyQuorumSet {
    pub threshold: u64,
    pub validators: Vec<PublicKey>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inner_quorum_sets: Vec<PrettyQuorumSet>,
}

macro_rules! json_format_single_line {
    ($x:expr) => {
        serde_json::to_string(&$x).expect("Error formatting as JSON")
    };
}
macro_rules! json_format_pretty {
    ($x:expr) => {
        serde_json::to_string_pretty(&$x).expect("Error formatting as JSON")
    };
}

pub trait AnalysisResult: Sized + Serialize {
    fn into_id_string(self) -> String;
    fn into_pretty_string(self, _: &Fbas, _: Option<&Groupings>) -> String {
        self.into_id_string()
    }
    fn into_describe_string(self) -> String;
}

// semantically strange, but for convenience
impl AnalysisResult for bool {
    fn into_id_string(self) -> String {
        self.to_string()
    }
    fn into_describe_string(self) -> String {
        self.to_string()
    }
}

// semantically strange, but for convenience
impl AnalysisResult for usize {
    fn into_id_string(self) -> String {
        self.to_string()
    }
    fn into_describe_string(self) -> String {
        self.to_string()
    }
}

impl AnalysisResult for QuorumSet {
    fn into_id_string(self) -> String {
        json_format_single_line!(self)
    }
    fn into_pretty_string(self, fbas: &Fbas, groupings: Option<&Groupings>) -> String {
        json_format_pretty!(self.into_pretty_quorum_set(fbas, groupings))
    }
    fn into_describe_string(self) -> String {
        self.into_id_string()
    }
}

impl AnalysisResult for Vec<QuorumSet> {
    fn into_id_string(self) -> String {
        json_format_single_line!(self)
    }
    fn into_pretty_string(self, fbas: &Fbas, groupings: Option<&Groupings>) -> String {
        let pretty_self: Vec<PrettyQuorumSet> = self
            .into_iter()
            .map(|q| q.into_pretty_quorum_set(fbas, groupings))
            .collect();
        json_format_pretty!(pretty_self)
    }
    fn into_describe_string(self) -> String {
        self.into_id_string()
    }
}

impl AnalysisResult for NodeIdSetResult {
    fn into_id_string(self) -> String {
        json_format_single_line!(self.into_vec())
    }
    fn into_pretty_string(self, fbas: &Fbas, groupings: Option<&Groupings>) -> String {
        json_format_single_line!(self.into_pretty_vec(fbas, groupings))
    }
    fn into_describe_string(self) -> String {
        self.len().to_string()
    }
}
impl Serialize for NodeIdSetResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.clone().into_vec().serialize(serializer)
    }
}

impl AnalysisResult for NodeIdSetVecResult {
    fn into_id_string(self) -> String {
        json_format_single_line!(self.into_vec_vec())
    }
    fn into_pretty_string(self, fbas: &Fbas, groupings: Option<&Groupings>) -> String {
        json_format_single_line!(self.into_pretty_vec_vec(fbas, groupings))
    }
    fn into_describe_string(self) -> String {
        json_format_single_line!(self.describe())
    }
}
impl Serialize for NodeIdSetVecResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.clone().into_vec_vec().serialize(serializer)
    }
}

impl QuorumSet {
    pub fn into_pretty_quorum_set(
        self,
        fbas: &Fbas,
        groupings: Option<&Groupings>,
    ) -> PrettyQuorumSet {
        let quorum_set = if let Some(groupings) = groupings {
            groupings.merge_quorum_set(self)
        } else {
            self
        };
        let QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        } = quorum_set;
        let validators = if let Some(orgs) = groupings {
            to_grouping_names(validators, fbas, orgs)
        } else {
            to_public_keys(validators, fbas)
        };
        let inner_quorum_sets = inner_quorum_sets
            .into_iter()
            .map(|q| q.into_pretty_quorum_set(fbas, groupings))
            .collect();
        PrettyQuorumSet {
            threshold: threshold
                .try_into()
                .expect("Error converting threshold from usize to u64."),
            validators,
            inner_quorum_sets,
        }
    }
}

impl NodeIdSetResult {
    /// Transforms result into a vector of public keys and/or grouping names.
    /// The passed FBAS should be the same as the one used for analysis, otherwise the IDs might
    /// not match. Preserves the original node ID-based ordering.
    pub fn into_pretty_vec(self, fbas: &Fbas, groupings: Option<&Groupings>) -> Vec<PublicKey> {
        if let Some(orgs) = groupings {
            to_grouping_names(&self.unwrap(), fbas, orgs)
        } else {
            to_public_keys(&self.unwrap(), fbas)
        }
    }
}

impl NodeIdSetVecResult {
    /// Transforms result into a vector of vectors of public keys and/or grouping names.
    /// The passed FBAS should be the same as the one used for analysis, otherwise the IDs might
    /// not not match. Preserves the original (typically node ID-based) ordering.
    pub fn into_pretty_vec_vec(
        self,
        fbas: &Fbas,
        groupings: Option<&Groupings>,
    ) -> Vec<Vec<PublicKey>> {
        self.shrunken_node_sets
            .iter()
            .map(|node_set| {
                if let Some(unshrink_table) = self.unshrink_table.as_ref() {
                    NodeIdSetResult {
                        node_set: unshrink_set(node_set, unshrink_table),
                    }
                } else {
                    NodeIdSetResult {
                        node_set: node_set.clone(),
                    }
                }
                .into_pretty_vec(fbas, groupings)
            })
            .collect()
    }
}

fn to_public_keys(nodes: impl IntoIterator<Item = NodeId>, fbas: &Fbas) -> Vec<PublicKey> {
    nodes
        .into_iter()
        .map(|id| &fbas.nodes[id].public_key)
        .cloned()
        .collect()
}
fn to_grouping_names(
    nodes: impl IntoIterator<Item = NodeId>,
    fbas: &Fbas,
    groupings: &Groupings,
) -> Vec<PublicKey> {
    nodes
        .into_iter()
        .map(|id| match &groupings.get_by_member(id) {
            Some(org) => &org.name,
            None => &fbas.nodes[id].public_key,
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_contains {
        ($actual_collection:expr, $expected_element:expr) => {
            assert!(
                $actual_collection.contains($expected_element),
                "{:?} does not contain {:?}",
                $actual_collection,
                $expected_element
            );
        };
    }

    macro_rules! assert_eq_ex_whitespace {
        ($actual:expr, $expected:expr) => {
            let mut actual = String::from($actual);
            let mut expected = String::from($expected);
            actual.retain(|c| !c.is_whitespace());
            expected.retain(|c| !c.is_whitespace());
            assert_eq!(expected, actual);
        };
    }

    #[test]
    #[ignore]
    fn results_output_correctly() {
        let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let groupings = None;
        let analysis = Analysis::new(&fbas);

        // all in one test to share the analysis (it is not *that* fast)
        // values found with fbas_analyzer v0.1 + some python and jq
        let qi = analysis.has_quorum_intersection();
        assert_eq!(qi.clone().into_id_string(), "true");
        assert_eq!(
            qi.clone().into_pretty_string(&fbas, groupings.as_ref()),
            "true"
        );
        assert_eq!(qi.clone().into_describe_string(), "true");

        let tt = analysis.top_tier();
        assert_eq!(
            tt.clone().into_id_string(),
            "[1,4,8,23,29,36,37,43,44,52,56,69,86,105,167,168,171]"
        );
        assert_eq!(
            tt.clone().into_pretty_string(&fbas, groupings.as_ref()),
            r#"["GDXQB3OMMQ6MGG43PWFBZWBFKBBDUZIVSUDAZZTRAWQZKES2CDSE5HKJ","GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ","GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH","GADLA6BJK6VK33EM2IDQM37L5KGVCY5MSHSHVJA4SCNGNUIEOTCR6J5T","GC5SXLNAM3C4NMGK2PXK4R34B5GNZ47FYQ24ZIBFDFOCU6D4KBN4POAE","GDKWELGJURRKXECG3HHFHXMRX64YWQPUHKCVRESOX3E5PM6DM4YXLZJM","GA7TEPCBDQKI7JQLQ34ZURRMK44DVYCIGVXQQWNSWAEQR6KB4FMCBT7J","GD5QWEVV4GZZTQP46BRXV5CUMMMLP4JTGFD7FWYJJWRL54CELY6JGQ63","GA35T3723UP2XJLC2H7MNL6VMKZZIFL2VW7XHMFFJKKIA2FJCYTLKFBW","GCFONE23AB7Y6C5YZOMKUKGETPIAJA4QOYLS5VNS4JHBGKRZCPYHDLW7","GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK","GAZ437J46SCFPZEDLVGDMKZPLFO77XJ4QVAURSJVRZK2T5S7XUFHXI2Z","GA5STBMV6QDXFDGD62MEHLLHZTPDI77U3PFOD2SELU5RJDHQWBR5NNK7","GBJQUIXUO4XSNPAUT6ODLZUJRV2NPXYASKUBY4G5MYP3M47PCVI55MNT","GAK6Z5UVGUVSEK6PEOCAYJISTT5EJBB34PN3NOLEQG2SUKXRVV2F6HZY","GD6SZQV3WEJUH352NTVLKEV2JM2RH266VPEM7EH5QLLI7ZZAALMLNUVN","GCWJKM4EGTGJUVSWUJDPCQEOEP5LHSOFKSA4HALBTOO4T4H3HCHOM6UX"]"#
        );
        assert_eq!(tt.clone().into_describe_string(), "17");

        let mq = analysis.minimal_quorums();
        assert_eq!(mq.len(), 1161);
        assert_contains!(mq.clone().into_id_string(), "[4,8,23,29,36,44,69,105]");
        assert_contains!(mq.clone().into_id_string(), "[1,4,29,36,37,43,56,105,171]");
        assert_contains!(
            mq.clone().into_pretty_string(&fbas, groupings.as_ref()),
            // [4,8,23,29,36,44,69,105]
            r#"["GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ","GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH","GADLA6BJK6VK33EM2IDQM37L5KGVCY5MSHSHVJA4SCNGNUIEOTCR6J5T","GC5SXLNAM3C4NMGK2PXK4R34B5GNZ47FYQ24ZIBFDFOCU6D4KBN4POAE","GDKWELGJURRKXECG3HHFHXMRX64YWQPUHKCVRESOX3E5PM6DM4YXLZJM","GA35T3723UP2XJLC2H7MNL6VMKZZIFL2VW7XHMFFJKKIA2FJCYTLKFBW","GAZ437J46SCFPZEDLVGDMKZPLFO77XJ4QVAURSJVRZK2T5S7XUFHXI2Z","GBJQUIXUO4XSNPAUT6ODLZUJRV2NPXYASKUBY4G5MYP3M47PCVI55MNT"]"#
        );
        assert_contains!(
            mq.clone().into_pretty_string(&fbas, groupings.as_ref()),
            // [1,4,29,36,37,43,56,105,171]"
            r#"["GDXQB3OMMQ6MGG43PWFBZWBFKBBDUZIVSUDAZZTRAWQZKES2CDSE5HKJ","GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ","GC5SXLNAM3C4NMGK2PXK4R34B5GNZ47FYQ24ZIBFDFOCU6D4KBN4POAE","GDKWELGJURRKXECG3HHFHXMRX64YWQPUHKCVRESOX3E5PM6DM4YXLZJM","GA7TEPCBDQKI7JQLQ34ZURRMK44DVYCIGVXQQWNSWAEQR6KB4FMCBT7J","GD5QWEVV4GZZTQP46BRXV5CUMMMLP4JTGFD7FWYJJWRL54CELY6JGQ63","GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK","GBJQUIXUO4XSNPAUT6ODLZUJRV2NPXYASKUBY4G5MYP3M47PCVI55MNT","GCWJKM4EGTGJUVSWUJDPCQEOEP5LHSOFKSA4HALBTOO4T4H3HCHOM6UX"]"#
        );
        assert_eq!(
            mq.clone().into_describe_string(),
            "[1161,17,[8,9,8.930232558139535],[0,0,0,0,0,0,0,0,81,1080]]"
        );
    }

    #[test]
    #[ignore]
    fn merge_by_organization_results_output_correctly() {
        let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let organizations = Groupings::organizations_from_json_file(
            Path::new("test_data/stellarbeat_organizations_2019-09-17.json"),
            &fbas,
        );
        let analysis = Analysis::new(&fbas);

        // all in one test to share the analysis (it is not *that* fast)
        // values found with v0.1 of fbas_analyzer
        let qi = analysis.has_quorum_intersection();
        assert_eq!(qi.clone().into_id_string(), "true");
        assert_eq!(
            qi.clone().into_pretty_string(&fbas, Some(&organizations)),
            "true"
        );
        assert_eq!(qi.clone().into_describe_string(), "true");

        let tt = analysis.top_tier().merged_by_group(&organizations);
        assert_eq!(tt.clone().into_id_string(), "[56,86,167,168,171]");
        assert_eq!(
            tt.clone().into_pretty_string(&fbas, Some(&organizations)),
            r#"["Stellar Development Foundation","LOBSTR","SatoshiPay","COINQVEST Limited","Keybase"]"#
        );
        assert_eq!(tt.clone().into_describe_string(), "5");

        let mq = analysis
            .minimal_quorums()
            .merged_by_group(&organizations)
            .minimal_sets();
        assert_eq!(
            mq.clone().into_id_string(),
            "[[56,86,167,168],[56,86,167,171],[56,86,168,171],[56,167,168,171],[86,167,168,171]]"
        );
        assert_contains!(
            mq.clone().into_pretty_string(&fbas, Some(&organizations)),
            // [1,23,29,36]
            r#"["LOBSTR","SatoshiPay","COINQVEST Limited","Keybase"]"#
        );
        assert_eq!(
            mq.clone().into_describe_string(),
            "[5,5,[4,4,4.0],[0,0,0,0,5]]"
        );
    }

    #[test]
    fn symmetric_clusters_id_output_correctly() {
        let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let analysis = Analysis::new(&fbas);

        let clusters = analysis.symmetric_clusters();

        let expected = r#"[{"threshold":4,"innerQuorumSets":[{"threshold":2,"validators":[4,8,56]},{"threshold":2,"validators":[23,69,168]},{"threshold":2,"validators":[29,105,167]},{"threshold":2,"validators":[36,44,171]},{"threshold":3,"validators":[1,37,43,52,86]}]}]"#;
        let actual = clusters.into_id_string();
        assert_eq!(expected, actual);
    }

    #[test]
    fn symmetric_clusters_by_organization_pretty_output_correctly() {
        let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let organizations = Groupings::organizations_from_json_file(
            Path::new("test_data/stellarbeat_organizations_2019-09-17.json"),
            &fbas,
        );
        let analysis = Analysis::new(&fbas);

        let clusters = analysis.symmetric_clusters();

        let clusters = organizations.merge_quorum_sets(clusters);

        let expected = r#"[{"threshold":4,"validators":["Stellar Development Foundation","COINQVEST Limited","SatoshiPay","Keybase","LOBSTR"]}]"#;
        let actual = clusters.into_pretty_string(&fbas, Some(&organizations));

        assert_eq_ex_whitespace!(expected, actual);
    }

    #[test]
    fn into_pretty_vec_keeps_original_order() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "Jim"
            },
            {
                "publicKey": "Jon"
            },
            {
                "publicKey": "Alex"
            },
            {
                "publicKey": "Bob"
            }
        ]"#,
        );
        let result = NodeIdSetResult::new(bitset![0, 3], None);
        let expected = vec!["Jim", "Bob"];
        let actual = result.into_pretty_vec(&fbas, None);
        assert_eq!(expected, actual);
    }

    #[test]
    fn into_pretty_vec_vec_keeps_original_order() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "Jim"
            },
            {
                "publicKey": "Jon"
            },
            {
                "publicKey": "Alex"
            },
            {
                "publicKey": "Bob"
            }
        ]"#,
        );
        let result = NodeIdSetVecResult::new(bitsetvec![{0, 3}, {1}], None);
        let expected = vec![vec!["Jim", "Bob"], vec!["Jon"]];
        let actual = result.into_pretty_vec_vec(&fbas, None);
        assert_eq!(expected, actual);
    }

    #[test]
    fn into_pretty_vec_vec_works_with_orgs() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "Jim"
            },
            {
                "publicKey": "Jon"
            },
            {
                "publicKey": "Alex"
            },
            {
                "publicKey": "Bob"
            }
            ]"#,
        );
        let organizations = Groupings::organizations_from_json_str(
            r#"[
            {
                "name": "J Mafia",
                "validators": [ "Jim", "Jon" ]
            }
            ]"#,
            &fbas,
        );
        let result = NodeIdSetVecResult::new(bitsetvec![{0, 3}, {1}], None);
        let expected = vec![vec!["J Mafia", "Bob"], vec!["J Mafia"]];
        let actual = result.into_pretty_vec_vec(&fbas, Some(&organizations));
        assert_eq!(expected, actual);
    }

    #[test]
    fn into_pretty_quorum_set() {
        let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let organizations = Groupings::organizations_from_json_file(
            Path::new("test_data/stellarbeat_organizations_2019-09-17.json"),
            &fbas,
        );
        let analysis = Analysis::new(&fbas);

        let symmetric_top_tier = analysis.symmetric_top_tier().unwrap();
        let actual = symmetric_top_tier.into_pretty_quorum_set(&fbas, Some(&organizations));

        let expected = PrettyQuorumSet {
            threshold: 4,
            validators: vec![
                "Stellar Development Foundation".to_string(),
                "COINQVEST Limited".to_string(),
                "SatoshiPay".to_string(),
                "Keybase".to_string(),
                "LOBSTR".to_string(),
            ],
            inner_quorum_sets: vec![],
        };

        assert_eq!(expected, actual);
    }
}
