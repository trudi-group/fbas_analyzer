use serde::Deserialize;
use serde_json;

use std::collections::HashMap;

use crate::*;

#[derive(Deserialize)]
struct RawNetwork(Vec<RawNode>);
impl RawNetwork {
    fn from_json_str(json: &str) -> Self {
        serde_json::from_str(json).expect("Error parsing JSON")
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
    #[serde(rename = "innerQuorumSets")]
    inner_quorum_sets: Vec<RawQuorumSet>,
}

impl Network {
    fn from_json_str(json: &str) -> Self {
        Network::from_raw(RawNetwork::from_json_str(json))
    }

    fn from_raw(raw_network: RawNetwork) -> Self {
        let raw_nodes = raw_network.0;

        let pk_to_id: HashMap<PublicKey, NodeID> = raw_nodes
            .iter()
            .enumerate()
            .map(|(x, y)| (y.public_key.clone(), x))
            .collect();

        let nodes = raw_nodes
            .into_iter()
            .map(|x| Node::from_raw(x, &pk_to_id))
            .collect();

        Network { nodes }
    }
}
impl Node {
    fn from_raw(raw_node: RawNode, pk_to_id: &HashMap<PublicKey, NodeID>) -> Self {
        Node {
            public_key: raw_node.public_key,
            quorum_set: QuorumSet::from_raw(raw_node.quorum_set, pk_to_id),
        }
    }
}
impl QuorumSet {
    fn from_raw(raw_quorum_set: RawQuorumSet, pk_to_id: &HashMap<PublicKey, NodeID>) -> Self {
        QuorumSet {
            threshold: raw_quorum_set.threshold,
            validators: raw_quorum_set
                .validators
                .into_iter()
                .map(|x| pk_to_id[&x])
                .collect(),
            inner_quorum_sets: raw_quorum_set
                .inner_quorum_sets
                .into_iter()
                .map(|x| QuorumSet::from_raw(x, pk_to_id))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn from_json_to_network_quorum_sets_match() {
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
                    "threshold": 2,
                    "validators": [
                        "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                        "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ",
                        "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK"
                    ],
                    "innerQuorumSets": []
                }
            },
            {
                "publicKey": "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK",
                "quorumSet": {
                    "threshold": 3,
                    "validators": [
                        "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                        "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ",
                        "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK"
                    ],
                    "innerQuorumSets": []
                }
            }]"#;

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
                threshold: 2,
                validators: vec![0, 1, 2].into_iter().collect(),
                inner_quorum_sets: vec![],
            },
            QuorumSet {
                threshold: 3,
                validators: vec![0, 1, 2].into_iter().collect(),
                inner_quorum_sets: vec![],
            },
        ];
        let actual_quorum_sets: Vec<QuorumSet> = Network::from_json_str(&input)
            .nodes
            .into_iter()
            .map(|x| x.quorum_set)
            .collect();

        assert_eq!(expected_quorum_sets, actual_quorum_sets);
    }

}
