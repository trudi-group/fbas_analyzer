use super::*;

/// Finds groups of nodes (represented as quorum sets) such that all members of the same group have
/// the exact identical quorum set, and the nodes contained in this quorum set are exactly the
/// group of nodes (a symmetric cluster). Once no more such clusters are found, returns the maximum
/// quorum of the remaining nodes. (So, getting a result with more than 1 entry implies that we
/// don't have quorum intersection.)
pub fn find_symmetric_clusters(fbas: &Fbas) -> Vec<QuorumSet> {
    info!("Starting to look for symmetric quorum clusters...");
    let quorums = find_sets(fbas, symmetric_clusters_finder);
    info!("Found {} different quorum clusters.", quorums.len());
    quorums
}

fn symmetric_clusters_finder(consensus_clusters: Vec<NodeIdSet>, fbas: &Fbas) -> Vec<QuorumSet> {
    let mut found_clusters_in_all_clusters = vec![];
    for (i, nodes) in consensus_clusters.into_iter().enumerate() {
        debug!("Finding symmetric quorum cluster in cluster {}...", i);
        found_clusters_in_all_clusters
            .append(&mut find_symmetric_clusters_in_node_set(&nodes, fbas));
    }
    found_clusters_in_all_clusters
}

pub(crate) fn find_symmetric_cluster_in_consensus_cluster(
    cluster: &NodeIdSet,
    fbas: &Fbas,
) -> Option<QuorumSet> {
    let symmetric_clusters = find_symmetric_clusters_in_node_set(cluster, fbas);
    if !symmetric_clusters.is_empty() {
        assert!(symmetric_clusters.len() == 1, "More than one symmetric clusters found - perhaps input wasn't a consensus cluster, i.e., not a strongly-connected component?");
        assert!(symmetric_clusters[0].contained_nodes() == *cluster);
        Some(symmetric_clusters.into_iter().next().unwrap())
    } else {
        None
    }
}

pub(crate) fn find_symmetric_clusters_in_node_set(
    nodes: &NodeIdSet,
    fbas: &Fbas,
) -> Vec<QuorumSet> {
    // qset -> (#occurances, goal #occurances)
    let mut qset_occurances: BTreeMap<QuorumSet, (usize, usize)> = BTreeMap::new();
    let mut found_clusters = vec![];

    for node_id in nodes.iter() {
        let qset = &fbas.nodes[node_id].quorum_set;
        if qset.contained_nodes().contains(node_id) {
            let (count, goal) = if let Some((counter, goal)) = qset_occurances.get_mut(qset) {
                *counter += 1;
                (*counter, *goal)
            } else {
                let goal = qset.contained_nodes().len();
                qset_occurances.insert(qset.clone(), (1, goal));
                (1, goal)
            };
            if count == goal {
                found_clusters.push(qset.clone());
            }
        }
    }
    found_clusters
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn find_symmetric_cluster_in_correct_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        let expected = vec![QuorumSet {
            validators: vec![0, 1, 2],
            threshold: 2,
            inner_quorum_sets: vec![],
        }];
        let actual = find_symmetric_clusters(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_symmetric_cluster_in_symmetric_cluster() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            }
        ]"#,
        );
        let expected = Some(QuorumSet {
            validators: vec![0, 1],
            threshold: 2,
            inner_quorum_sets: vec![],
        });
        let actual = find_symmetric_cluster_in_consensus_cluster(&bitset![0, 1], &fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_symmetric_cluster_in_weird_cluster() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 1, "validators": ["n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            }
        ]"#,
        );
        let expected = None;
        let actual = find_symmetric_cluster_in_consensus_cluster(&bitset![0, 1], &fbas);

        assert_eq!(expected, actual);
    }
}
