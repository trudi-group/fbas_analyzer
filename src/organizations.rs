use serde::Deserialize;
use serde_json;

use std::collections::HashMap;
use std::fs;

use crate::*;

pub struct Organizations {
    organizations: Vec<Organization>,
    collapsed_ids: Vec<NodeId>,
}
impl Organizations {
    pub fn from_json_str(json: &str, fbas: &Fbas) -> Self {
        Self::from_raw(RawOrganizations::from_json_str(json), fbas)
    }
    pub fn from_json_file(path: &str, fbas: &Fbas) -> Self {
        Self::from_raw(RawOrganizations::from_json_file(path), fbas)
    }
    pub fn collapse_node(self: &Self, node_id: NodeId) -> NodeId {
        self.collapsed_ids[node_id]
    }
    pub fn collapse_node_set(self: &Self, node_set: NodeIdSet) -> NodeIdSet {
        node_set
            .into_iter()
            .map(|x| self.collapse_node(x))
            .collect()
    }
    pub fn collapse_node_sets(self: &Self, node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
        node_sets
            .into_iter()
            .map(|x| self.collapse_node_set(x))
            .collect()
    }

    fn from_raw(raw_organizations: RawOrganizations, fbas: &Fbas) -> Self {
        let pk_to_id: HashMap<PublicKey, NodeId> = fbas
            .nodes
            .iter()
            .enumerate()
            .map(|(ni, n)| (n.public_key.clone(), ni))
            .collect();
        let organizations: Vec<Organization> = raw_organizations
            .0
            .into_iter()
            .map(|x| Organization::from_raw(x, &pk_to_id))
            .collect();

        let mut collapsed_ids: Vec<NodeId> = (0..fbas.nodes.len()).collect();

        for organization in organizations.iter() {
            let mut validator_it = organization.validators.iter().copied();
            if let Some(collapsed_id) = validator_it.next() {
                for validator in validator_it {
                    collapsed_ids[validator] = collapsed_id;
                }
            }
        }
        Organizations {
            organizations,
            collapsed_ids,
        }
    }
}
struct Organization {
    id: String,
    name: String,
    validators: Vec<NodeId>,
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

#[derive(Deserialize)]
struct RawOrganizations(Vec<RawOrganization>);
impl RawOrganizations {
    fn from_json_str(json: &str) -> Self {
        serde_json::from_str(json).expect("Error parsing JSON")
    }
    fn from_json_file(path: &str) -> Self {
        let json = fs::read_to_string(path).expect(&format!("Error reading file {:?}", path));
        Self::from_json_str(&json)
    }
}
#[derive(Deserialize)]
struct RawOrganization {
    id: String,
    name: String,
    validators: Vec<PublicKey>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collapse_node_sets_by_organization() {
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
        let fbas = Fbas::from_json_str(&fbas_input);
        let organizations = Organizations::from_json_str(&organizations_input, &fbas);

        let node_sets = vec![bitset![0], bitset![1, 2]];

        let expected = vec![bitset![0], bitset![0, 2]];
        let actual = organizations.collapse_node_sets(node_sets);

        assert_eq!(expected, actual);
    }
}
