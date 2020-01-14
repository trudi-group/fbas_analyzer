use super::*;

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
    output_histogram: bool,
    output_pretty: bool,
) -> String {
    if output_histogram {
        format!("{:?}", describe_with_histogram(node_sets))
    } else if output_description {
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
