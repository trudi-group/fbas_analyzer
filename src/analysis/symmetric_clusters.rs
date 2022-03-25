use super::*;

/// Finds groups of nodes (represented as quorum sets) such that all members of the same group have
/// (logically) the same quorum set and the common quorum set is comprised of exactly the group of
/// nodes (a symmetric cluster). This function implicitly patches quorum sets so that each node is
/// always included in its own quorum set.
/// Getting a result with more than 1 entry implies that we don't have quorum intersection.
pub fn find_symmetric_clusters(fbas: &Fbas) -> Vec<QuorumSet> {
    info!("Starting to look for symmetric quorum clusters...");
    let mut symmetric_clusters = find_sets(fbas, symmetric_clusters_finder);
    symmetric_clusters.sort_unstable();
    info!(
        "Found {} different quorum clusters.",
        symmetric_clusters.len()
    );
    symmetric_clusters
}

/// If the top tier is symmetric, i.e., each two top-tier nodes have the same quorum set,
/// return the top tier's common quorum set. Else return `None`. This function implicitly patches
/// quorum sets so that each node is always included in its own quorum set.
pub fn find_symmetric_top_tier(fbas: &Fbas) -> Option<QuorumSet> {
    let symmetric_clusters = find_symmetric_clusters(fbas);
    if symmetric_clusters.len() == 1
        && !complement_contains_quorum(&symmetric_clusters[0].contained_nodes(), fbas)
    {
        Some(symmetric_clusters.into_iter().next().unwrap())
    } else {
        None
    }
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

/// Can fail if quorum sets are not in "standard form"! You can get there via `Fbas::with_standard_form_quorum_sets`.
pub(crate) fn is_symmetric_cluster<'a>(
    cluster: &NodeIdSet,
    fbas: &'a Fbas,
) -> Option<&'a QuorumSet> {
    if let Some(first_node_id) = cluster.iter().next() {
        let cluster_quorum_set = &fbas.nodes[first_node_id].quorum_set;
        if cluster_quorum_set.contained_nodes().eq(cluster)
            && cluster
                .iter()
                .skip(1)
                .all(|node_id| fbas.nodes[node_id].quorum_set.eq(cluster_quorum_set))
        {
            Some(cluster_quorum_set)
        } else {
            None
        }
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
        let qset = fbas.nodes[node_id].quorum_set.to_standard_form(node_id);
        if qset.contained_nodes().contains(node_id) {
            let (count, goal) = if let Some((counter, goal)) = qset_occurances.get_mut(&qset) {
                *counter += 1;
                (*counter, *goal)
            } else {
                let goal = qset.contained_nodes().len();
                qset_occurances.insert(qset.clone(), (1, goal));
                (1, goal)
            };
            if count == goal {
                found_clusters.push(qset);
            }
        }
    }
    found_clusters
}

impl QuorumSet {
    /// Make sure that node is included and all validator lists are sorted.
    pub(crate) fn to_standard_form(&self, node_id: NodeId) -> Self {
        let mut qset = self.clone();
        qset.ensure_node_included(node_id);
        qset.ensure_sorted();
        qset
    }
    fn ensure_node_included(&mut self, node_id: NodeId) {
        if !self.contained_nodes().contains(node_id) {
            self.validators.push(node_id);
            self.threshold += 1;
        }
    }
    fn ensure_sorted(&mut self) {
        self.validators.sort_unstable();
        for qset in self.inner_quorum_sets.iter_mut() {
            qset.ensure_sorted();
        }
        self.inner_quorum_sets.sort_unstable();
    }
}

impl Fbas {
    pub(crate) fn with_standard_form_quorum_sets(&self) -> Self {
        let mut fbas = self.clone();
        for (node_id, node) in fbas.nodes.iter_mut().enumerate() {
            node.quorum_set = node.quorum_set.to_standard_form(node_id);
        }
        fbas
    }
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
    fn find_symmetric_cluster_in_split_fbas() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 1, "validators": ["n3"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 1, "validators": ["n2"] }
            }
        ]"#,
        );
        assert!(!Analysis::new(&fbas).has_quorum_intersection());

        let expected = vec![
            QuorumSet {
                validators: vec![0, 1],
                threshold: 2,
                inner_quorum_sets: vec![],
            },
            QuorumSet {
                validators: vec![2, 3],
                threshold: 2,
                inner_quorum_sets: vec![],
            },
        ];
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
        let actual = is_symmetric_cluster(&bitset![0, 1], &fbas).cloned();

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
        let expected = None; // because we need quorum sets to be in standard form!
        let actual = is_symmetric_cluster(&bitset![0, 1], &fbas).cloned();

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_symmetric_cluster_in_weird_cluster_made_standard() {
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
        let fbas = fbas.with_standard_form_quorum_sets();
        let expected = Some(QuorumSet {
            validators: vec![0, 1],
            threshold: 2,
            inner_quorum_sets: vec![],
        });
        let actual = is_symmetric_cluster(&bitset![0, 1], &fbas).cloned();

        assert_eq!(expected, actual);
    }
    #[test]
    fn find_symmetric_top_tier_in_symmetric_fbas() {
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
        let actual = find_symmetric_top_tier(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_symmetric_top_tier_in_mobilecoinish_fbas() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 1, "validators": ["n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 1, "validators": ["n0"] }
            }
        ]"#,
        );
        let expected = Some(QuorumSet {
            validators: vec![0, 1],
            threshold: 2,
            inner_quorum_sets: vec![],
        });
        let actual = find_symmetric_top_tier(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_symmetric_top_tier_in_broken_symmetric_fbas() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 1, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 1, "validators": ["n0", "n1"] }
            }
        ]"#,
        );
        assert!(!Analysis::new(&fbas).has_quorum_intersection());

        let expected = Some(QuorumSet {
            validators: vec![0, 1],
            threshold: 1,
            inner_quorum_sets: vec![],
        });
        let actual = find_symmetric_top_tier(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_symmetric_top_tier_in_split_fbas() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n2", "n3"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 2, "validators": ["n2", "n3"] }
            }
        ]"#,
        );
        assert!(!Analysis::new(&fbas).has_quorum_intersection());

        let expected = None;
        let actual = find_symmetric_top_tier(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_symmetric_top_tier_in_weird_split_fbas() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 1, "validators": ["n3"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 1, "validators": ["n2"] }
            }
        ]"#,
        );
        assert!(!Analysis::new(&fbas).has_quorum_intersection());

        let expected = None;
        let actual = find_symmetric_top_tier(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn standard_form_on_good_qset_does_nothing() {
        let qset = QuorumSet {
            threshold: 2,
            validators: vec![0, 1],
            inner_quorum_sets: vec![],
        };
        let node_id = 0;

        let expected = qset.clone();
        let actual = qset.to_standard_form(node_id);

        assert_eq!(expected, actual);
    }

    #[test]
    fn standard_form_on_weird_qset_includes_node_and_sorts_correctly() {
        let qset = QuorumSet {
            threshold: 1,
            validators: vec![1],
            inner_quorum_sets: vec![],
        };
        let node_id = 0;

        let expected = QuorumSet {
            threshold: 2,
            validators: vec![0, 1],
            inner_quorum_sets: vec![],
        };
        let actual = qset.to_standard_form(node_id);

        assert_eq!(expected, actual);
    }
}
