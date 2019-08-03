use serde_json;
use serde::Deserialize;

use crate::PublicKey;

#[derive(Deserialize)]
struct RawNetwork(Vec<RawNode>);
impl RawNetwork {
    fn from_json(json: &str) ->  Self {
        serde_json::from_str(json).expect("Error parsing JSON")
    }
}

#[derive(Deserialize)]
struct RawNode {
    #[serde(rename="publicKey")]
    public_key: PublicKey,
    #[serde(default)]
    quorum_set: RawQuorumSet,
}
#[derive(Deserialize, Default)]
struct RawQuorumSet {
    threshold: usize,
    validators: Vec<PublicKey>,
    #[serde(rename="innerQuorumSets")]
    inner_quorum_sets: Vec<RawQuorumSet>
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn json_parsing_doesnt_panic() {

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
        RawNetwork::from_json(&input);
    }

}
