use super::*;
use serde_with::{serde_as, NoneAsEmptyString};
use std::convert::TryInto;

#[derive(Serialize, Deserialize)]
pub(crate) struct RawFbas(pub(crate) Vec<RawNode>);
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawNode {
    pub(crate) public_key: PublicKey,
    // If no quorum set is given, we assume that the node is unsatisfiable, i.e., broken.
    #[serde(default = "RawQuorumSet::new_unsatisfiable")]
    pub(crate) quorum_set: RawQuorumSet,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) isp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) geo_data: Option<RawGeoData>,
}
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawQuorumSet {
    pub(crate) threshold: u64,
    pub(crate) validators: Vec<PublicKey>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) inner_quorum_sets: Vec<RawQuorumSet>,
}
impl RawQuorumSet {
    fn new_unsatisfiable() -> Self {
        Self {
            threshold: 1,
            validators: vec![],
            inner_quorum_sets: vec![],
        }
    }
}
#[serde_as]
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawGeoData {
    #[serde_as(as = "NoneAsEmptyString")]
    #[serde(default)]
    pub(crate) country_name: Option<String>,
}

impl Fbas {
    pub fn from_json_str(json: &str) -> Self {
        serde_json::from_str(json).expect("Error parsing FBAS JSON")
    }
    pub fn from_json_file(path: &Path) -> Self {
        Self::from_json_str(&read_or_panic!(path))
    }
    pub fn from_json_stdin() -> Self {
        serde_json::from_reader(io::stdin()).expect("Error reading FBAS JSON from STDIN")
    }
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(&self).expect("Error converting FBAS to JSON!")
    }
    pub fn to_json_string_pretty(&self) -> String {
        serde_json::to_string_pretty(&self).expect("Error converting FBAS to pretty JSON!")
    }
    pub(crate) fn from_raw(raw_fbas: RawFbas) -> Self {
        let raw_nodes: Vec<RawNode> = raw_fbas.0.into_iter().collect();

        let pk_to_id: HashMap<PublicKey, NodeId> = raw_nodes
            .iter()
            .enumerate()
            .map(|(x, y)| (y.public_key.clone(), x))
            .collect();

        let nodes = raw_nodes
            .into_iter()
            .map(|x| Node::from_raw(x, &pk_to_id))
            .collect();

        Fbas { nodes, pk_to_id }
    }
    pub(crate) fn to_raw(&self) -> RawFbas {
        RawFbas(self.nodes.iter().map(|n| n.to_raw(self)).collect())
    }
}
impl fmt::Display for Fbas {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_json_string_pretty())
    }
}
impl Serialize for Fbas {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_raw().serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for Fbas {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw_fbas = RawFbas::deserialize(deserializer)?;
        Ok(Fbas::from_raw(raw_fbas))
    }
}
impl Node {
    fn from_raw(raw_node: RawNode, pk_to_id: &HashMap<PublicKey, NodeId>) -> Self {
        Node {
            public_key: raw_node.public_key,
            quorum_set: QuorumSet::from_raw(raw_node.quorum_set, pk_to_id),
        }
    }
    fn to_raw(&self, fbas: &Fbas) -> RawNode {
        RawNode {
            public_key: self.public_key.clone(),
            quorum_set: self.quorum_set.to_raw(fbas),
            isp: None,
            geo_data: None,
        }
    }
}
impl QuorumSet {
    fn from_raw(raw_quorum_set: RawQuorumSet, pk_to_id: &HashMap<PublicKey, NodeId>) -> Self {
        let mut validators: Vec<NodeId> = raw_quorum_set
            .validators
            .into_iter()
            .filter_map(|x| pk_to_id.get(&x))
            .copied()
            .collect();
        let mut inner_quorum_sets: Vec<QuorumSet> = raw_quorum_set
            .inner_quorum_sets
            .into_iter()
            .map(|x| QuorumSet::from_raw(x, pk_to_id))
            .collect();
        let threshold = raw_quorum_set.threshold;
        // sort to make comparisons between quorum sets easier
        validators.sort_unstable();
        inner_quorum_sets.sort_unstable();
        QuorumSet {
            validators,
            inner_quorum_sets,
            threshold: threshold.try_into().unwrap_or(usize::MAX),
        }
    }
    fn to_raw(&self, fbas: &Fbas) -> RawQuorumSet {
        RawQuorumSet {
            threshold: self
                .threshold
                .try_into()
                .expect("Error converting threshold from usize to u64."),
            validators: self
                .validators
                .iter()
                .map(|&v| match fbas.nodes.get(v) {
                    Some(node) => node.public_key.clone(),
                    None => format!("missing #{}", v),
                })
                .collect(),
            inner_quorum_sets: self
                .inner_quorum_sets
                .iter()
                .map(|iqs| iqs.to_raw(fbas))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn from_json_to_fbas() {
        let input = r#"[
            {
                "publicKey": "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                "quorumSet": {
                    "threshold": 1,
                    "validators": [],
                    "innerQuorumSets": [
                        {
                            "threshold": 2,
                            "validators": [
                                "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                                "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ",
                                "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK"
                            ],
                            "innerQuorumSets": []
                        }
                    ]
                }
            },
            {
                "publicKey": "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ",
                "quorumSet": {
                    "threshold": 3,
                    "validators": [
                        "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                        "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ",
                        "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK"
                    ]
                },
                "aFieldWeIgnore": 42
            },
            {
                "publicKey": "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK"
            }]"#;

        let expected_public_keys = vec![
            "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
            "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ",
            "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK",
        ];
        let expected_quorum_sets = vec![
            QuorumSet {
                threshold: 1,
                validators: vec![].into_iter().collect(),
                inner_quorum_sets: vec![QuorumSet {
                    threshold: 2,
                    validators: vec![0, 1, 2].into_iter().collect(),
                    inner_quorum_sets: vec![],
                }],
            },
            QuorumSet {
                threshold: 3,
                validators: vec![0, 1, 2].into_iter().collect(),
                inner_quorum_sets: vec![],
            },
            QuorumSet::new_unsatisfiable(),
        ];

        let actual = Fbas::from_json_str(input);
        let actual_public_keys: Vec<PublicKey> =
            actual.nodes.iter().map(|x| x.public_key.clone()).collect();
        let actual_quorum_sets: Vec<QuorumSet> =
            actual.nodes.into_iter().map(|x| x.quorum_set).collect();

        assert_eq!(expected_public_keys, actual_public_keys);
        assert_eq!(expected_quorum_sets, actual_quorum_sets);
    }

    #[test]
    fn from_json_ignores_unknown_public_keys() {
        let input = r#"[
            {
                "publicKey": "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                "quorumSet": {
                    "threshold": 2,
                    "validators": [
                        "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                        "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ",
                        "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK"
                    ]
                }
            },
            {
                "publicKey": "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ"
            }]"#;

        let expected_quorum_sets = vec![
            QuorumSet {
                threshold: 2,
                validators: vec![0, 1].into_iter().collect(),
                inner_quorum_sets: Default::default(),
            },
            QuorumSet::new(vec![], vec![], 1),
        ];

        let actual = Fbas::from_json_str(input);
        let actual_quorum_sets: Vec<QuorumSet> =
            actual.nodes.into_iter().map(|x| x.quorum_set).collect();

        assert_eq!(expected_quorum_sets, actual_quorum_sets);
    }

    #[test]
    fn from_json_keeps_inactive_nodes() {
        // otherwise IDs don't match indices
        let input = r#"[
            {
                "publicKey": "GCGB2",
                "active": false
            },
            {
                "publicKey": "GCM6Q",
                "active": true
            },
            {
                "publicKey": "GABMK"
            }]"#;

        let fbas = Fbas::from_json_str(input);

        let expected = vec!["GCGB2", "GCM6Q", "GABMK"];
        let actual: Vec<PublicKey> = fbas.nodes.into_iter().map(|x| x.public_key).collect();

        assert_eq!(expected, actual);
    }

    #[test]
    fn to_json_and_back_results_in_identical_fbas() {
        let original = Fbas::new_generic_unconfigured(7);
        let json = original.to_json_string();
        let recombined = Fbas::from_json_str(&json);
        assert_eq!(original, recombined);
    }

    #[test]
    fn can_serizalize_quorum_sets_with_unknown_nodes() {
        let fbas = Fbas::new();
        let quorum_set = QuorumSet {
            threshold: 2,
            validators: vec![0, 1].into_iter().collect(),
            inner_quorum_sets: Default::default(),
        };
        let expected = RawQuorumSet {
            threshold: 2,
            validators: vec![String::from("missing #0"), String::from("missing #1")],
            inner_quorum_sets: vec![],
        };
        let actual = quorum_set.to_raw(&fbas);
        assert_eq!(expected, actual);
    }

    // broken since we also have "organizations" test files now
    // #[test]
    // fn from_json_doesnt_panic_for_test_files() {
    //     use std::fs;
    //     for item in fs::read_dir("test_data").unwrap() {
    //         let path = item.unwrap().path();
    //         Fbas::from_json_file(path.to_str().unwrap());
    //     }
    // }
}
