use serde::Deserialize;
use serde_json;

use std::fs;
use std::path::Path;

use crate::*;

#[derive(Deserialize)]
struct RawFbas(Vec<RawNode>);
impl RawFbas {
    fn from_json_str(json: &str) -> Self {
        serde_json::from_str(json).expect("Error parsing JSON")
    }
    fn from_json_file(path: &Path) -> Self {
        let json =
            fs::read_to_string(path).unwrap_or_else(|_| panic!("Error reading file {:?}", path));
        Self::from_json_str(&json)
    }
}
#[derive(Deserialize)]
struct RawNode {
    #[serde(rename = "publicKey")]
    public_key: PublicKey,
    #[serde(rename = "quorumSet", default)]
    quorum_set: RawQuorumSet,
}
#[derive(Deserialize, Default)]
struct RawQuorumSet {
    threshold: usize,
    validators: Vec<PublicKey>,
    #[serde(rename = "innerQuorumSets", default)]
    inner_quorum_sets: Vec<RawQuorumSet>,
}
impl Fbas {
    pub fn from_json_str(json: &str) -> Self {
        Self::from_raw(RawFbas::from_json_str(json))
    }
    pub fn from_json_file(path: &Path) -> Self {
        Self::from_raw(RawFbas::from_json_file(path))
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
}
impl Node {
    fn from_raw(raw_node: RawNode, pk_to_id: &HashMap<PublicKey, NodeId>) -> Self {
        Node {
            public_key: raw_node.public_key,
            quorum_set: QuorumSet::from_raw(raw_node.quorum_set, pk_to_id),
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
}

#[derive(Deserialize)]
struct RawOrganizations(Vec<RawOrganization>);
impl RawOrganizations {
    fn from_json_str(json: &str) -> Self {
        serde_json::from_str(json).expect("Error parsing JSON")
    }
    fn from_json_file(path: &Path) -> Self {
        let json =
            fs::read_to_string(path).unwrap_or_else(|_| panic!("Error reading file {:?}", path));
        Self::from_json_str(&json)
    }
}
#[derive(Deserialize)]
struct RawOrganization {
    id: String,
    name: String,
    validators: Vec<PublicKey>,
}
impl Organizations {
    pub fn from_json_str(json: &str, fbas: &Fbas) -> Self {
        Self::from_raw(RawOrganizations::from_json_str(json), fbas)
    }
    pub fn from_json_file(path: &Path, fbas: &Fbas) -> Self {
        Self::from_raw(RawOrganizations::from_json_file(path), fbas)
    }
    fn from_raw(raw_organizations: RawOrganizations, fbas: &Fbas) -> Self {
        let organizations: Vec<Organization> = raw_organizations
            .0
            .into_iter()
            .map(|x| Organization::from_raw(x, &fbas.pk_to_id))
            .collect();

        Organizations::new(organizations, fbas)
    }
}
impl Organization {
    fn from_raw(raw_organization: RawOrganization, pk_to_id: &HashMap<PublicKey, NodeId>) -> Self {
        Organization {
            id: raw_organization.id,
            name: raw_organization.name,
            validators: raw_organization
                .validators
                .into_iter()
                .filter_map(|pk| pk_to_id.get(&pk))
                .cloned()
                .collect(),
        }
    }
}

/// Nodes represented by NodeIds (which should be equal to nodes' indices in the input JSON).
pub fn to_json_str_using_node_ids(node_sets: &[NodeIdSet]) -> String {
    let node_sets: Vec<Vec<NodeId>> = node_sets.iter().map(|x| x.iter().collect()).collect();

    serde_json::to_string(&node_sets).expect("Error converting node set to JSON!")
}

/// Nodes represented by their public keys.
pub fn to_json_str_using_public_keys(node_sets: &[NodeIdSet], fbas: &Fbas) -> String {
    let node_sets: Vec<Vec<&PublicKey>> = node_sets
        .iter()
        .map(|x| x.iter().map(|x| &fbas.nodes[x].public_key).collect())
        .collect();

    serde_json::to_string(&node_sets).expect("Error converting node set to JSON!")
}

/// Nodes represented by their organization's name.
pub fn to_json_str_using_organization_names(
    node_sets: &[NodeIdSet],
    fbas: &Fbas,
    organizations: &Organizations,
) -> String {
    let node_sets: Vec<Vec<&String>> = node_sets
        .iter()
        .map(|x| {
            x.iter()
                .map(|x| match &organizations.get_by_member(&x) {
                    Some(org) => &org.name,
                    None => &fbas.nodes[x].public_key,
                })
                .collect()
        })
        .collect();

    serde_json::to_string(&node_sets).expect("Error converting node set to JSON!")
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
