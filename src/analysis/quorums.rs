use super::*;
use itertools::Itertools;

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

/// A fast heuristic quorum intersection check via checking if there are any non-intersecting
/// quorum *slices* in the FBAS core. If this function returns `true` it is certain that the FBAS
/// enjoys quorum intersection. If this function returns `false` the FBAS still might enjoy quorum
/// intersection but a slower check is necessary.
pub fn has_quorum_intersection_via_heuristic_check(fbas: &Fbas) -> bool {
    let core_nodes = fbas.core_nodes();
    !core_nodes.is_empty() && have_quorum_intersection_via_fast_check(&core_nodes, fbas)
}
pub(crate) fn have_quorum_intersection_via_fast_check(nodes: &NodeIdSet, fbas: &Fbas) -> bool {
    let n = nodes.len();

    let quorum_sets: Vec<QuorumSet> = nodes
        .iter()
        .map(|node_id| fbas.nodes[node_id].quorum_set.to_standard_form(node_id))
        .unique()
        .collect();

    if quorum_sets
        .iter()
        .all(|qset| qset.smallest_slice_len_lower_bound() > n / 2)
    {
        true
    } else {
        let slices = quorum_sets
            .into_iter()
            .flat_map(|qset| qset.to_quorum_slices())
            .collect_vec();
        !slices.is_empty() && all_intersect(&slices.into_iter().collect_vec())
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

            let unprocessed = sorted_candidate_nodes;
            let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());
            let mut available = nodes.clone();

            let symmetric_nodes = find_symmetric_nodes_in_node_set(&available, fbas);

            let mut found_unexpanded_quorums_in_this_cluster = vec![];

            debug!("Collecting quorums...");
            minimal_quorums_finder_step(
                &mut unprocessed.into(),
                &mut selection,
                &mut available,
                &mut found_unexpanded_quorums_in_this_cluster,
                fbas,
                &symmetric_nodes,
                true,
            );
            found_quorums
                .append(&mut symmetric_nodes.expand_sets(found_unexpanded_quorums_in_this_cluster))
        }
    }
    found_quorums
}
fn minimal_quorums_finder_step(
    unprocessed: &mut NodeIdDeque,
    selection: &mut NodeIdSet,
    available: &mut NodeIdSet,
    found_quorums: &mut Vec<NodeIdSet>,
    fbas: &Fbas,
    symmetric_nodes: &SymmetricNodesMap,
    selection_changed: bool,
) {
    if selection_changed && fbas.is_quorum(selection) {
        if is_minimal_for_quorum(selection, fbas) {
            found_quorums.push(selection.clone());
            if found_quorums.len() % 100_000 == 0 {
                debug!("...{} quorums found", found_quorums.len());
            }
        }
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        // We require that symmetric nodes are used in a fixed order; this way we can omit
        // redundant branches (we expand all combinations of symmetric nodes in the final result
        // sets).
        if symmetric_nodes.is_non_redundant_next(current_candidate, selection) {
            selection.insert(current_candidate);
            minimal_quorums_finder_step(
                unprocessed,
                selection,
                available,
                found_quorums,
                fbas,
                symmetric_nodes,
                true,
            );
            selection.remove(current_candidate);
        }
        available.remove(current_candidate);

        if selection_satisfiable(selection, available, fbas) {
            minimal_quorums_finder_step(
                unprocessed,
                selection,
                available,
                found_quorums,
                fbas,
                symmetric_nodes,
                false,
            );
        }
        unprocessed.push_front(current_candidate);
        available.insert(current_candidate);
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
    /// A lower bound for the length of the smallest slice that satisfied this quorum set. If there
    /// are no duplicates and the node that this quorum set belongs to is explicitly included in
    /// `self.validators`, this actually returns a precise result.
    fn smallest_slice_len_lower_bound(&self) -> usize {
        let with_duplicates = if self.threshold <= self.validators.len() {
            self.threshold
        } else {
            self.validators.len()
                + self
                    .inner_quorum_sets
                    .iter()
                    .map(|qset| qset.smallest_slice_len_lower_bound())
                    .sorted()
                    .take(self.threshold - self.validators.len())
                    .sum::<usize>()
        };

        let number_of_duplicates = self
            .contained_nodes_with_duplicates()
            .into_iter()
            .duplicates()
            .count();

        // hence it's a lower bound
        with_duplicates.saturating_sub(number_of_duplicates)
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
pub(crate) fn nonintersecting_quorums_finder_using_sorted_nodes(
    sorted_nodes: Vec<usize>,
    fbas: &Fbas,
) -> Vec<BitSet> {
    let unprocessed = sorted_nodes;
    let mut selection = NodeIdSet::with_capacity(fbas.nodes.len());
    let mut available: NodeIdSet = unprocessed.iter().cloned().collect();
    let mut antiselection = available.clone();
    let picks_left = unprocessed.len() / 2;
    // testing bigger quorums yields no benefit
    let symmetric_nodes = find_symmetric_nodes_in_node_set(&available, fbas);
    if let Some(intersecting_quorums) = nonintersecting_quorums_finder_step(
        &mut unprocessed.into(),
        &mut selection,
        &mut available,
        &mut antiselection,
        &FbasValues {
            fbas,
            symmetric_nodes: &symmetric_nodes,
        },
        picks_left,
        true,
    ) {
        assert!(intersecting_quorums.iter().all(|x| fbas.is_quorum(x)));
        assert!(intersecting_quorums[0].is_disjoint(&intersecting_quorums[1]));
        intersecting_quorums.to_vec()
    } else {
        assert!(fbas.is_quorum(&available));
        vec![available.clone()]
    }
}
fn nonintersecting_quorums_finder_step(
    unprocessed: &mut NodeIdDeque,
    selection: &mut NodeIdSet,
    available: &mut NodeIdSet,
    antiselection: &mut NodeIdSet,
    fbas_values: &FbasValues,
    picks_left: usize,
    selection_changed: bool,
) -> Option<[NodeIdSet; 2]> {
    debug_assert!(selection.is_disjoint(antiselection));
    if selection_changed && fbas_values.fbas.is_quorum(selection) {
        let (potential_complement, _) = find_satisfiable_nodes(antiselection, fbas_values.fbas);

        if !potential_complement.is_empty() {
            return Some([selection.clone(), potential_complement]);
        }
    } else if picks_left == 0 {
        return None;
    } else if let Some(current_candidate) = unprocessed.pop_front() {
        // We require that symmetric nodes are used in a fixed order; this way we can omit
        // redundant branches.
        if fbas_values
            .symmetric_nodes
            .is_non_redundant_next(current_candidate, selection)
        {
            selection.insert(current_candidate);
            antiselection.remove(current_candidate);
            if let Some(intersecting_quorums) = nonintersecting_quorums_finder_step(
                unprocessed,
                selection,
                available,
                antiselection,
                fbas_values,
                picks_left - 1,
                true,
            ) {
                return Some(intersecting_quorums);
            }
            selection.remove(current_candidate);
            antiselection.insert(current_candidate);
        }
        available.remove(current_candidate);

        if selection_satisfiable(selection, available, fbas_values.fbas) {
            if let Some(intersecting_quorums) = nonintersecting_quorums_finder_step(
                unprocessed,
                selection,
                available,
                antiselection,
                fbas_values,
                picks_left,
                false,
            ) {
                return Some(intersecting_quorums);
            }
        }
        unprocessed.push_front(current_candidate);
        available.insert(current_candidate);
    }
    None
}

// This exists because clippy complained that we have too many arguments.
#[derive(Debug, Clone)]
struct FbasValues<'a> {
    fbas: &'a Fbas,
    symmetric_nodes: &'a SymmetricNodesMap,
}

fn selection_satisfiable(selection: &NodeIdSet, available: &NodeIdSet, fbas: &Fbas) -> bool {
    selection
        .iter()
        .all(|x| fbas.nodes[x].quorum_set.is_quorum_slice(available))
}

pub(crate) fn contains_quorum(node_set: &NodeIdSet, fbas: &Fbas) -> bool {
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
    fn find_minimal_quorums_in_correct_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        let expected = vec![bitset![0, 1], bitset![0, 2], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_quorums_in_broken_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));

        let expected = vec![bitset![0], bitset![1, 2]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_quorums_in_broken_trivial_reversed_node_ids() {
        let mut fbas = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));
        fbas.nodes.reverse();

        let expected = vec![bitset![2], bitset![0, 1]];
        let actual = find_minimal_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_minimal_quorums_when_naive_remove_non_minimal_optimization_doesnt_work() {
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
    fn find_nonintersecting_quorums_in_half_half() {
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
    fn find_nonintersecting_quorums_in_broken() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken.json"));

        let expected = Some(vec![bitset![3, 10], bitset![4, 6]]);
        let actual = find_nonintersecting_quorums(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn has_quorum_intersection_via_fast_check_in_correct_trivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));

        let expected = true;
        let actual = has_quorum_intersection_via_heuristic_check(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn has_quorum_intersection_via_fast_check_in_broken() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken.json"));

        let expected = false;
        let actual = has_quorum_intersection_via_heuristic_check(&fbas);

        assert_eq!(expected, actual);
    }

    #[test]
    fn smallest_slice_len_lower_bound_in_flat_quorum_set() {
        let qset = QuorumSet::new(vec![0, 1, 2], vec![], 2);
        let expected = 2;
        let actual = qset.smallest_slice_len_lower_bound();
        assert_eq!(expected, actual);
    }

    #[test]
    fn smallest_slice_len_lower_bound_in_flat_quorum_set_with_duplicates() {
        let qset = QuorumSet::new(vec![0, 1, 2, 2], vec![], 2);
        let expected = 1;
        let actual = qset.smallest_slice_len_lower_bound();
        assert_eq!(expected, actual);
    }

    #[test]
    fn smallest_slice_len_lower_bound_in_nested_quorum_set() {
        let qset = QuorumSet::new(
            vec![0, 1, 2],
            vec![
                QuorumSet::new(vec![3, 4, 5], vec![], 3),
                QuorumSet::new(vec![6, 7, 8, 9], vec![], 2),
            ],
            4,
        );
        let expected = 5;
        let actual = qset.smallest_slice_len_lower_bound();
        assert_eq!(expected, actual);
    }

    #[test]
    fn smallest_slice_len_lower_bound_in_nested_quorum_set_with_duplicates() {
        let qset = QuorumSet::new(
            vec![0, 1, 2],
            vec![
                QuorumSet::new(vec![0, 1, 2, 3, 4, 5], vec![], 3),
                QuorumSet::new(vec![0, 3, 4, 5, 6], vec![], 2),
            ],
            4,
        );
        let expected = 0; // the precise result would be 3 but calculating this is not so cheap
        let actual = qset.smallest_slice_len_lower_bound();
        assert_eq!(expected, actual);
    }
}
