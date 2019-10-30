use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json;

use std::fs;
use std::io;
use std::path::Path;

use crate::*;

#[derive(Serialize, Deserialize)]
struct RawFbas(Vec<RawNode>);
#[derive(Serialize, Deserialize)]
struct RawNode {
    #[serde(rename = "publicKey")]
    public_key: PublicKey,
    #[serde(rename = "quorumSet", default)]
    quorum_set: RawQuorumSet,
}
#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
struct RawQuorumSet {
    threshold: usize,
    validators: Vec<PublicKey>,
    #[serde(rename = "innerQuorumSets", default)]
    inner_quorum_sets: Vec<RawQuorumSet>,
}

impl Fbas {
    pub fn from_json_str(json: &str) -> Self {
        serde_json::from_str(json).expect("Error parsing FBAS JSON")
    }
    pub fn from_json_file(path: &Path) -> Self {
        let json =
            fs::read_to_string(path).unwrap_or_else(|_| panic!("Error reading file {:?}", path));
        Self::from_json_str(&json)
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
    fn from_raw(raw_fbas: RawFbas) -> Self {
        let raw_nodes = raw_fbas.0;

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
    fn to_raw(&self) -> RawFbas {
        RawFbas(self.nodes.iter().map(|n| n.to_raw(&self)).collect())
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
            quorum_set: self.quorum_set.to_raw(&fbas),
        }
    }
}
impl QuorumSet {
    fn from_raw(raw_quorum_set: RawQuorumSet, pk_to_id: &HashMap<PublicKey, NodeId>) -> Self {
        QuorumSet {
            threshold: raw_quorum_set.threshold,
            validators: raw_quorum_set
                .validators
                .into_iter()
                .filter_map(|x| pk_to_id.get(&x))
                .copied()
                .collect(),
            inner_quorum_sets: raw_quorum_set
                .inner_quorum_sets
                .into_iter()
                .map(|x| QuorumSet::from_raw(x, pk_to_id))
                .collect(),
        }
    }
    fn to_raw(&self, fbas: &Fbas) -> RawQuorumSet {
        RawQuorumSet {
            threshold: self.threshold,
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
                .map(|iqs| iqs.to_raw(&fbas))
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RawOrganizations(Vec<RawOrganization>);
#[derive(Serialize, Deserialize)]
struct RawOrganization {
    name: String,
    validators: Vec<PublicKey>,
}
impl<'fbas> Organizations<'fbas> {
    pub fn from_json_str(json: &str, fbas: &'fbas Fbas) -> Self {
        Self::from_raw(
            serde_json::from_str(json).expect("Error parsing Organizations JSON"),
            fbas,
        )
    }
    pub fn from_json_file(path: &Path, fbas: &'fbas Fbas) -> Self {
        let json =
            fs::read_to_string(path).unwrap_or_else(|_| panic!("Error reading file {:?}", path));
        Self::from_json_str(&json, fbas)
    }
    fn from_raw(raw_organizations: RawOrganizations, fbas: &'fbas Fbas) -> Self {
        let organizations: Vec<Organization> = raw_organizations
            .0
            .into_iter()
            .map(|x| Organization::from_raw(x, &fbas.pk_to_id))
            .collect();

        Organizations::new(organizations, fbas)
    }
    fn to_raw(&self) -> RawOrganizations {
        RawOrganizations(
            self.organizations
                .iter()
                .map(|org| org.to_raw(self.fbas))
                .collect(),
        )
    }
}
impl<'fbas> Serialize for Organizations<'fbas> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_raw().serialize(serializer)
    }
}
impl Organization {
    fn from_raw(raw_organization: RawOrganization, pk_to_id: &HashMap<PublicKey, NodeId>) -> Self {
        Organization {
            name: raw_organization.name,
            validators: raw_organization
                .validators
                .into_iter()
                .filter_map(|pk| pk_to_id.get(&pk))
                .cloned()
                .collect(),
        }
    }
    fn to_raw(&self, fbas: &Fbas) -> RawOrganization {
        RawOrganization {
            name: self.name.clone(),
            validators: self
                .validators
                .iter()
                .map(|&x| fbas.nodes[x].public_key.clone())
                .collect(),
        }
    }
}

macro_rules! json_format {
    ($x:expr) => {
        serde_json::to_string(&$x).expect("Error formatting as JSON")
    };
}
/// Smart format. If `output_description`, only statistics are output. If `output_pretty`,
/// public keys or organization IDs (if available) are used instead of node IDs.
pub fn format_node_id_sets(
    node_sets: &[NodeIdSet],
    fbas: &Fbas,
    organizations: &Option<Organizations>,
    output_description: bool,
    output_pretty: bool,
) -> String {
    if output_description {
        format!("{:?}", describe(node_sets))
    } else if output_pretty {
        if let Some(ref orgs) = organizations {
            json_format!(to_organization_names_vecs(node_sets, &fbas, &orgs))
        } else {
            json_format!(to_public_keys_vecs(node_sets, &fbas))
        }
    } else {
        json_format!(to_node_ids_vecs(node_sets))
    }
}
/// Smart format. If `output_description`, only their count is output. If `output_pretty`,
/// public keys or organization IDs (if available) are used instead of node IDs.
pub fn format_node_ids(
    node_ids: &[NodeId],
    fbas: &Fbas,
    organizations: &Option<Organizations>,
    output_description: bool,
    output_pretty: bool,
) -> String {
    if output_description {
        format!("{}", node_ids.len())
    } else if output_pretty {
        if let Some(ref orgs) = organizations {
            json_format!(to_organization_names(node_ids.iter().copied(), fbas, orgs))
        } else {
            json_format!(to_public_keys(node_ids.iter().copied(), fbas))
        }
    } else {
        json_format!(node_ids)
    }
}
fn to_node_ids_vecs(node_sets: &[NodeIdSet]) -> Vec<Vec<NodeId>> {
    node_sets.iter().map(|x| x.iter().collect()).collect()
}
fn to_public_keys_vecs<'fbas>(
    node_sets: &[NodeIdSet],
    fbas: &'fbas Fbas,
) -> Vec<Vec<&'fbas PublicKey>> {
    node_sets
        .iter()
        .map(|node_set| to_public_keys(node_set, fbas))
        .collect()
}
fn to_public_keys<'fbas>(
    node_ids: impl IntoIterator<Item = NodeId>,
    fbas: &'fbas Fbas,
) -> Vec<&'fbas PublicKey> {
    node_ids
        .into_iter()
        .map(|id| &fbas.nodes[id].public_key)
        .collect()
}
fn to_organization_names_vecs<'a>(
    node_sets: &[NodeIdSet],
    fbas: &'a Fbas,
    organizations: &'a Organizations,
) -> Vec<Vec<&'a PublicKey>> {
    node_sets
        .iter()
        .map(|node_set| to_organization_names(node_set, fbas, organizations))
        .collect()
}
fn to_organization_names<'a>(
    node_ids: impl IntoIterator<Item = NodeId>,
    fbas: &'a Fbas,
    organizations: &'a Organizations,
) -> Vec<&'a PublicKey> {
    node_ids
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
            Default::default(),
        ];

        let actual = Fbas::from_json_str(&input);
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
            Default::default(),
        ];

        let actual = Fbas::from_json_str(&input);
        let actual_quorum_sets: Vec<QuorumSet> =
            actual.nodes.into_iter().map(|x| x.quorum_set).collect();

        assert_eq!(expected_quorum_sets, actual_quorum_sets);
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
