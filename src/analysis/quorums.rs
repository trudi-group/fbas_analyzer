use super::*;

/// Find all minimal quorums in the FBAS.
pub fn find_minimal_quorums(fbas: &Fbas) -> Vec<NodeIdSet> {
    info!("Starting to look for minimal quorums...");
    let minimal_quorums = find_minimal_sets(fbas, minimal_quorums_finder);
    info!("Found {} minimal quorums.", minimal_quorums.len());
    minimal_quorums
}

/// Find at least two non-intersecting quorums. Use this function if you don't want to enumerate
/// all minimal quorums and/or it is likely that the FBAS lacks quorum intersection and you want to
/// stop early in such cases.
pub fn find_nonintersecting_quorums(fbas: &Fbas) -> Option<Vec<NodeIdSet>> {
    info!("Starting to look for potentially non-intersecting quorums...");
    let quorums = find_sets(fbas, nonintersecting_quorums_finder);
    if quorums.len() < 2 {
        info!("Found no non-intersecting quorums.");
        None
    } else {
        warn!(
            "Found {} non-intersecting quorums (there could more).",
            quorums.len()
        );
        Some(quorums)
    }
}

fn minimal_quorums_finder(consensus_clusters: Vec<NodeIdSet>, fbas: &Fbas) -> Vec<NodeIdSet> {
    let mut found_quorums: Vec<NodeIdSet> = vec![];

    for (i, nodes) in consensus_clusters.into_iter().enumerate() {
        debug!("Finding minimal quorums in cluster {}...", i);

        if let Some(symmetric_cluster) =
            is_symmetric_cluster(&nodes, &fbas.with_standard_form_quorum_sets())
        {
            debug!("Cluster contains a symmetric quorum cluster! Extracting quorums...");
            found_quorums.append(&mut symmetric_cluster.to_minimal_quorums(fbas));
        } else {
            debug!("Sorting nodes by rank...");
            let sorted_candidate_nodes = sort_by_rank(nodes.into_iter().collect(), fbas);
            debug!("Sorted.");

            debug!("Looking for symmetric nodes...");
            let symmetric_nodes = find_symmetric_nodes_in_node_set(&nodes, fbas);
            debug!("Done.");

            let mut found_unexpanded_quorums_in_this_cluster = vec![];

            debug!("Collecting quorums...");
            minimal_quorums_finder_step(
                &mut CandidateValuesMq::new(sorted_candidate_nodes),
                &mut found_unexpanded_quorums_in_this_cluster,
                &FbasValues::new(fbas, &symmetric_nodes),
                true,
            );
            found_quorums
                .append(&mut symmetric_nodes.expand_sets(found_unexpanded_quorums_in_this_cluster))
        }
    }
    found_quorums
}
fn minimal_quorums_finder_step(
    candidates: &mut CandidateValuesMq,
    found_quorums: &mut Vec<NodeIdSet>,
    fbas_values: &FbasValues,
    selection_changed: bool,
) {
    if selection_changed && fbas_values.fbas.is_quorum(&candidates.selection) {
        if is_minimal_for_quorum(&candidates.selection, fbas_values.fbas) {
            found_quorums.push(candidates.selection.clone());
            if found_quorums.len() % 100_000 == 0 {
                debug!("...{} quorums found", found_quorums.len());
            }
        }
    } else if let Some(current_candidate) = candidates.unprocessed.pop_front() {
        // We require that symmetric nodes are used in a fixed order; this way we can omit
        // redundant branches (we expand all combinations of symmetric nodes in the final result
        // sets).
        if fbas_values
            .symmetric_nodes
            .is_non_redundant_next(current_candidate, &candidates.selection)
        {
            candidates.selection.insert(current_candidate);
            minimal_quorums_finder_step(candidates, found_quorums, fbas_values, true);
            candidates.selection.remove(current_candidate);
        }
        candidates.available.remove(current_candidate);

        if selection_satisfiable(
            &candidates.selection,
            &candidates.available,
            fbas_values.fbas,
        ) {
            minimal_quorums_finder_step(candidates, found_quorums, fbas_values, false);
        }
        candidates.unprocessed.push_front(current_candidate);
        candidates.available.insert(current_candidate);
    }
}

fn nonintersecting_quorums_finder(
    consensus_clusters: Vec<NodeIdSet>,
    fbas: &Fbas,
) -> Vec<NodeIdSet> {
    if consensus_clusters.len() > 1 {
        debug!("More than one consensus clusters - reducing to maximal quorums.");
        consensus_clusters
            .into_iter()
            .map(|node_set| find_satisfiable_nodes(&node_set, fbas).0)
            .collect()
    } else {
        warn!("There is only one consensus cluster - there might be no non-intersecting quorums and the subsequent search might be slow.");
        let nodes = consensus_clusters.into_iter().next().unwrap_or_default();
        nonintersecting_quorums_finder_using_cluster(&nodes, fbas)
    }
}
fn nonintersecting_quorums_finder_using_cluster(cluster: &NodeIdSet, fbas: &Fbas) -> Vec<BitSet> {
    if let Some(symmetric_cluster) =
        is_symmetric_cluster(cluster, &fbas.with_standard_form_quorum_sets())
    {
        debug!("Cluster contains a symmetric quorum cluster! Extracting using that...");
        if let Some((quorum1, quorum2)) = symmetric_cluster.has_nonintersecting_quorums() {
            vec![quorum1, quorum2]
        } else {
            vec![]
        }
    } else {
        debug!("Sorting nodes by rank...");
        let sorted_nodes = sort_by_rank(cluster.iter().collect(), fbas);
        debug!("Sorted.");

        nonintersecting_quorums_finder_using_sorted_nodes(sorted_nodes, fbas)
    }
}
// This function is used many times in a row in `find_minimal_splitting_sets`, hence we use logging
// more sparingly.
pub(crate) fn nonintersecting_quorums_finder_using_sorted_nodes(
    sorted_nodes: Vec<usize>,
    fbas: &Fbas,
) -> Vec<BitSet> {
    let mut candidates = CandidateValuesNi::new(sorted_nodes);
    let symmetric_nodes = find_symmetric_nodes_in_node_set(&candidates.available, fbas);

    // testing bigger quorums yields no benefit
    let picks_left = candidates.unprocessed.len() / 2;

    if let Some(intersecting_quorums) = nonintersecting_quorums_finder_step(
        &mut candidates,
        &FbasValues::new(fbas, &symmetric_nodes),
        picks_left,
        true,
    ) {
        assert!(intersecting_quorums.iter().all(|x| fbas.is_quorum(x)));
        assert!(intersecting_quorums[0].is_disjoint(&intersecting_quorums[1]));
        intersecting_quorums.to_vec()
    } else {
        assert!(fbas.is_quorum(&candidates.available));
        vec![candidates.available.clone()]
    }
}
fn nonintersecting_quorums_finder_step(
    candidates: &mut CandidateValuesNi,
    fbas_values: &FbasValues,
    picks_left: usize,
    selection_changed: bool,
) -> Option<[NodeIdSet; 2]> {
    debug_assert!(candidates.selection.is_disjoint(&candidates.antiselection));

    if selection_changed && fbas_values.fbas.is_quorum(&candidates.selection) {
        let (potential_complement, _) =
            find_satisfiable_nodes(&candidates.antiselection, fbas_values.fbas);

        if !potential_complement.is_empty() {
            return Some([candidates.selection.clone(), potential_complement]);
        }
    } else if picks_left == 0 {
        return None;
    } else if let Some(current_candidate) = candidates.unprocessed.pop_front() {
        // We require that symmetric nodes are used in a fixed order; this way we can omit
        // redundant branches.
        if fbas_values
            .symmetric_nodes
            .is_non_redundant_next(current_candidate, &candidates.selection)
        {
            candidates.selection.insert(current_candidate);
            candidates.antiselection.remove(current_candidate);

            if let Some(intersecting_quorums) =
                nonintersecting_quorums_finder_step(candidates, fbas_values, picks_left - 1, true)
            {
                return Some(intersecting_quorums);
            }
            candidates.selection.remove(current_candidate);
            candidates.antiselection.insert(current_candidate);
        }
        candidates.available.remove(current_candidate);

        if selection_satisfiable(
            &candidates.selection,
            &candidates.available,
            fbas_values.fbas,
        ) {
            if let Some(intersecting_quorums) =
                nonintersecting_quorums_finder_step(candidates, fbas_values, picks_left, false)
            {
                return Some(intersecting_quorums);
            }
        }
        candidates.unprocessed.push_front(current_candidate);
        candidates.available.insert(current_candidate);
    }
    None
}

#[derive(Debug, Clone)]
struct CandidateValuesMq {
    selection: NodeIdSet,
    unprocessed: NodeIdDeque,
    available: NodeIdSet,
}
impl CandidateValuesMq {
    fn new(sorted_nodes_to_process: Vec<NodeId>) -> Self {
        let selection = bitset![];
        let unprocessed: NodeIdDeque = sorted_nodes_to_process.into();
        let available = unprocessed.iter().copied().collect();
        Self {
            selection,
            unprocessed,
            available,
        }
    }
}

#[derive(Debug, Clone)]
struct CandidateValuesNi {
    selection: NodeIdSet,
    unprocessed: NodeIdDeque,
    available: NodeIdSet,
    antiselection: NodeIdSet,
}
impl CandidateValuesNi {
    fn new(sorted_nodes_to_process: Vec<NodeId>) -> Self {
        let selection = bitset![];
        let unprocessed: NodeIdDeque = sorted_nodes_to_process.into();
        let available: NodeIdSet = unprocessed.iter().copied().collect();
        let antiselection = available.clone();
        Self {
            selection,
            unprocessed,
            available,
            antiselection,
        }
    }
}

#[derive(Debug, Clone)]
struct FbasValues<'a> {
    fbas: &'a Fbas,
    symmetric_nodes: &'a SymmetricNodesMap,
}
impl<'a> FbasValues<'a> {
    fn new(fbas: &'a Fbas, symmetric_nodes: &'a SymmetricNodesMap) -> Self {
        Self {
            fbas,
            symmetric_nodes,
        }
    }
}

impl QuorumSet {
    /// Makes sense if the quorum set represents a symmetric quorum cluster...
    fn to_minimal_quorums(&self, fbas: &Fbas) -> Vec<NodeIdSet> {
        let quorums = self.to_quorum_slices();
        if self.contains_duplicates() {
            remove_non_minimal_x(quorums, is_minimal_for_quorum, fbas)
        } else {
            quorums
        }
    }
    /// Makes sense if the quorum set represents a symmetric quorum cluster...
    pub(crate) fn has_nonintersecting_quorums(&self) -> Option<(NodeIdSet, NodeIdSet)> {
        // make sure we aren't really a 1-node quorum
        if self
            .nonempty_slices_iter(|qset| qset.threshold)
            .take(2)
            .count()
            > 1
        {
            self.has_nonintersecting_quorum_slices()
        } else {
            None
        }
    }
}

fn selection_satisfiable(selection: &NodeIdSet, available: &NodeIdSet, fbas: &Fbas) -> bool {
    selection
        .iter()
        .all(|x| fbas.nodes[x].quorum_set.is_quorum_slice(available))
}

/// Returns `true` if any subset of `node_set` forms a quorum for `fbas`.
pub fn contains_quorum(node_set: &NodeIdSet, fbas: &Fbas) -> bool {
    let mut satisfiable = node_set.clone();

    while let Some(unsatisfiable_node) = satisfiable
        .iter()
        .find(|&x| !fbas.nodes[x].quorum_set.is_quorum_slice(&satisfiable))
    {
        satisfiable.remove(unsatisfiable_node);
    }
    !satisfiable.is_empty()
}

pub(crate) fn complement_contains_quorum(node_set: &NodeIdSet, fbas: &Fbas) -> bool {
    let mut complement = fbas.all_nodes();
    complement.difference_with(node_set);
    contains_quorum(&complement, fbas)
}

fn is_minimal_for_quorum(quorum: &NodeIdSet, fbas: &Fbas) -> bool {
    let mut tester = quorum.clone();

    for node_id in quorum.iter() {
        tester.remove(node_id);
        if contains_quorum(&tester, fbas) {
            return false;
        }
        tester.insert(node_id);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn minimal_quorums_in_correct_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        let expected = vec![bitset![0, 1], bitset![0, 2], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_quorums_in_broken_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));

        let expected = vec![bitset![0], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_quorums_in_broken_trivial_reversed_node_ids() {
        let mut fbas = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));
        fbas.nodes.reverse();

        let expected = vec![bitset![2], bitset![0, 1]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn minimal_quorums_when_naive_remove_non_minimal_optimization_doesnt_work() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n3"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n3"] }
            }
        ]"#,
        );
        let expected = vec![bitset![0, 3], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);
        assert_eq!(expected, actual);
    }

    #[test]
    fn nonintersecting_quorums_in_half_half() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2", "n3"] }
            },
            {
                "publicKey": "n3",
                "quorumSet": { "threshold": 2, "validators": ["n2", "n3"] }
            }
        ]"#,
        );

        let expected = Some(vec![bitset![0, 1], bitset![2, 3]]);
        let actual = find_nonintersecting_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn nonintersecting_quorums_in_broken() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken.json"));

        let expected = Some(vec![bitset![3, 10], bitset![4, 6]]);
        let actual = find_nonintersecting_quorums(&fbas);

        assert_eq!(expected, actual);
    }
}
