use serde::{Serialize, Serializer};
use super::*;

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

pub trait AnalysisResult: Sized {
    fn into_id_string(self) -> String;
    fn into_pretty_string(self, _: &Fbas, _: &Option<Organizations>) -> String {
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

impl AnalysisResult for Vec<QuorumSet> {
    fn into_id_string(self) -> String {
        json_format_single_line!(self)
    }
    fn into_pretty_string(self, fbas: &Fbas, organizations: &Option<Organizations>) -> String {
        let raw_self: Vec<RawQuorumSet> = self
            .into_iter()
            .map(|q| q.into_raw(fbas, organizations))
            .collect();
        json_format_pretty!(raw_self)
    }
    fn into_describe_string(self) -> String {
        self.into_id_string()
    }
}

impl<'a> AnalysisResult for NodeIdSetResult<'a> {
    fn into_id_string(self) -> String {
        json_format_single_line!(self.into_vec())
    }
    fn into_pretty_string(self, fbas: &Fbas, organizations: &Option<Organizations>) -> String {
        json_format_single_line!(self.into_pretty_vec(fbas, organizations))
    }
    fn into_describe_string(self) -> String {
        self.len().to_string()
    }
}
impl<'a> Serialize for NodeIdSetResult<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.clone().into_vec().serialize(serializer)
    }
}

impl<'a> AnalysisResult for NodeIdSetVecResult<'a> {
    fn into_id_string(self) -> String {
        json_format_single_line!(self.into_vec_vec())
    }
    fn into_pretty_string(self, fbas: &Fbas, organizations: &Option<Organizations>) -> String {
        let result: Vec<Vec<&PublicKey>> = self
            .node_sets
            .iter()
            .map(|node_set| {
                NodeIdSetResult::new(node_set.clone(), self.unshrink_table)
                    .into_pretty_vec(fbas, organizations)
            })
            .collect();
        json_format_single_line!(result)
    }
    fn into_describe_string(self) -> String {
        json_format_single_line!(self.describe())
    }
}
impl<'a> Serialize for NodeIdSetVecResult<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.clone().into_vec_vec().serialize(serializer)
    }
}

impl QuorumSet {
    fn into_raw(self, fbas: &Fbas, organizations: &Option<Organizations>) -> RawQuorumSet {
        let QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        } = self;
        let validators = if let Some(ref orgs) = organizations {
            to_organization_names(validators, fbas, orgs)
        } else {
            to_public_keys(validators, fbas)
        }
        .into_iter()
        .cloned()
        .collect();
        let inner_quorum_sets = inner_quorum_sets
            .into_iter()
            .map(|q| q.into_raw(fbas, organizations))
            .collect();
        RawQuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
    }
}

impl<'a> NodeIdSetResult<'a> {
    fn into_pretty_vec(
        self,
        fbas: &'a Fbas,
        organizations: &'a Option<Organizations>,
    ) -> Vec<&'a PublicKey> {
        if let Some(ref orgs) = organizations {
            to_organization_names(&self.unwrap(), fbas, orgs)
        } else {
            to_public_keys(&self.unwrap(), fbas)
        }
    }
}

fn to_public_keys<'a>(
    nodes: impl IntoIterator<Item = NodeId>,
    fbas: &'a Fbas,
) -> Vec<&'a PublicKey> {
    nodes
        .into_iter()
        .map(|id| &fbas.nodes[id].public_key)
        .collect()
}
fn to_organization_names<'a>(
    nodes: impl IntoIterator<Item = NodeId>,
    fbas: &'a Fbas,
    organizations: &'a Organizations,
) -> Vec<&'a PublicKey> {
    nodes
        .into_iter()
        .map(|id| match &organizations.get_by_member(id) {
            Some(org) => &org.name,
            None => &fbas.nodes[id].public_key,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_contains {
        ($actual_collection:expr, $expected_element:expr) => {
            assert!(
                $actual_collection.contains($expected_element),
                format!(
                    "{:?} does not contain {:?}",
                    $actual_collection, $expected_element
                )
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
        let organizations = None;
        let mut analysis = Analysis::new(&fbas, organizations.as_ref());

        // all in one test to share the analysis (it is not *that* fast)
        // values found with fbas_analyzer v0.1 + some python and jq
        let qi = analysis.has_quorum_intersection();
        assert_eq!(qi.clone().into_id_string(), "true");
        assert_eq!(qi.clone().into_pretty_string(&fbas, &organizations), "true");
        assert_eq!(qi.clone().into_describe_string(), "true");

        let tt = analysis.top_tier();
        assert_eq!(
            tt.clone().into_id_string(),
            "[1,4,8,23,29,36,37,43,44,52,56,69,86,105,167,168,171]"
        );
        assert_eq!(
            tt.clone().into_pretty_string(&fbas, &organizations),
            r#"["GDXQB3OMMQ6MGG43PWFBZWBFKBBDUZIVSUDAZZTRAWQZKES2CDSE5HKJ","GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ","GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH","GADLA6BJK6VK33EM2IDQM37L5KGVCY5MSHSHVJA4SCNGNUIEOTCR6J5T","GC5SXLNAM3C4NMGK2PXK4R34B5GNZ47FYQ24ZIBFDFOCU6D4KBN4POAE","GDKWELGJURRKXECG3HHFHXMRX64YWQPUHKCVRESOX3E5PM6DM4YXLZJM","GA7TEPCBDQKI7JQLQ34ZURRMK44DVYCIGVXQQWNSWAEQR6KB4FMCBT7J","GD5QWEVV4GZZTQP46BRXV5CUMMMLP4JTGFD7FWYJJWRL54CELY6JGQ63","GA35T3723UP2XJLC2H7MNL6VMKZZIFL2VW7XHMFFJKKIA2FJCYTLKFBW","GCFONE23AB7Y6C5YZOMKUKGETPIAJA4QOYLS5VNS4JHBGKRZCPYHDLW7","GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK","GAZ437J46SCFPZEDLVGDMKZPLFO77XJ4QVAURSJVRZK2T5S7XUFHXI2Z","GA5STBMV6QDXFDGD62MEHLLHZTPDI77U3PFOD2SELU5RJDHQWBR5NNK7","GBJQUIXUO4XSNPAUT6ODLZUJRV2NPXYASKUBY4G5MYP3M47PCVI55MNT","GAK6Z5UVGUVSEK6PEOCAYJISTT5EJBB34PN3NOLEQG2SUKXRVV2F6HZY","GD6SZQV3WEJUH352NTVLKEV2JM2RH266VPEM7EH5QLLI7ZZAALMLNUVN","GCWJKM4EGTGJUVSWUJDPCQEOEP5LHSOFKSA4HALBTOO4T4H3HCHOM6UX"]"#
        );
        assert_eq!(tt.clone().into_describe_string(), "17");

        let mq = analysis.minimal_quorums();
        assert_eq!(mq.len(), 1161);
        assert_contains!(mq.clone().into_id_string(), "[4,8,23,29,36,44,69,105]");
        assert_contains!(mq.clone().into_id_string(), "[1,4,29,36,37,43,56,105,171]");
        assert_contains!(
            mq.clone().into_pretty_string(&fbas, &organizations),
            // [4,8,23,29,36,44,69,105]
            r#"["GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ","GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH","GADLA6BJK6VK33EM2IDQM37L5KGVCY5MSHSHVJA4SCNGNUIEOTCR6J5T","GC5SXLNAM3C4NMGK2PXK4R34B5GNZ47FYQ24ZIBFDFOCU6D4KBN4POAE","GDKWELGJURRKXECG3HHFHXMRX64YWQPUHKCVRESOX3E5PM6DM4YXLZJM","GA35T3723UP2XJLC2H7MNL6VMKZZIFL2VW7XHMFFJKKIA2FJCYTLKFBW","GAZ437J46SCFPZEDLVGDMKZPLFO77XJ4QVAURSJVRZK2T5S7XUFHXI2Z","GBJQUIXUO4XSNPAUT6ODLZUJRV2NPXYASKUBY4G5MYP3M47PCVI55MNT"]"#
        );
        assert_contains!(
            mq.clone().into_pretty_string(&fbas, &organizations),
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
        let organizations = Some(Organizations::from_json_file(
            Path::new("test_data/stellarbeat_organizations_2019-09-17.json"),
            &fbas,
        ));
        let mut analysis = Analysis::new(&fbas, organizations.as_ref());

        // all in one test to share the analysis (it is not *that* fast)
        // values found with v0.1 of fbas_analyzer
        let qi = analysis.has_quorum_intersection();
        assert_eq!(qi.clone().into_id_string(), "true");
        assert_eq!(qi.clone().into_pretty_string(&fbas, &organizations), "true");
        assert_eq!(qi.clone().into_describe_string(), "true");

        let tt = analysis.top_tier();
        assert_eq!(tt.clone().into_id_string(), "[1,4,23,29,36]");
        assert_eq!(
            tt.clone().into_pretty_string(&fbas, &organizations),
            r#"["LOBSTR","Stellar Development Foundation","COINQVEST Limited","SatoshiPay","Keybase"]"#
        );
        assert_eq!(tt.clone().into_describe_string(), "5");

        let mq = analysis.minimal_quorums();
        assert_eq!(
            mq.clone().into_id_string(),
            "[[1,4,23,29],[1,4,23,36],[1,4,29,36],[1,23,29,36],[4,23,29,36]]"
        );
        assert_contains!(
            mq.clone().into_pretty_string(&fbas, &organizations),
            // [1,23,29,36]
            r#"["LOBSTR","COINQVEST Limited","SatoshiPay","Keybase"]"#
        );
        assert_eq!(
            mq.clone().into_describe_string(),
            "[5,5,[4,4,4.0],[0,0,0,0,5]]"
        );
    }

    #[test]
    fn symmetric_clusters_id_output_correctly() {
        let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let organizations = None;
        let analysis = Analysis::new(&fbas, organizations.as_ref());

        let clusters = analysis.symmetric_clusters();

        let expected = r#"[{"threshold":4,"innerQuorumSets":[{"threshold":2,"validators":[4,8,56]},{"threshold":2,"validators":[23,69,168]},{"threshold":2,"validators":[29,105,167]},{"threshold":2,"validators":[36,44,171]},{"threshold":3,"validators":[1,37,43,52,86]}]}]"#;
        let actual = clusters.into_id_string();
        assert_eq!(expected, actual);
    }

    #[test]
    fn symmetric_clusters_by_organization_pretty_output_correctly() {
        let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let organizations = Some(Organizations::from_json_file(
            Path::new("test_data/stellarbeat_organizations_2019-09-17.json"),
            &fbas,
        ));
        let analysis = Analysis::new(&fbas, organizations.as_ref());

        let clusters = analysis.symmetric_clusters();

        let expected = r#"[{"threshold":4,"validators":["Stellar Development Foundation","COINQVEST Limited","SatoshiPay","Keybase","LOBSTR"]}]"#;
        let actual = clusters.into_pretty_string(&fbas, &organizations);

        assert_eq_ex_whitespace!(expected, actual);
    }
}
