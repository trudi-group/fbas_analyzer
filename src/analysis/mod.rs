use super::*;
use log::log_enabled;
use log::Level::Warn;

mod blocking_sets;
mod quorums;
mod rank;
mod shrink;
mod splitting_sets;

pub use blocking_sets::find_minimal_blocking_sets;
pub use quorums::{
    find_minimal_quorums, find_nonintersecting_quorums, find_symmetric_quorum_clusters,
    find_unsatisfiable_nodes,
};
pub use splitting_sets::find_minimal_splitting_sets;

pub(crate) use rank::*;

use quorums::reduce_to_strongly_connected_nodes;
use shrink::{reshrink_sets, unshrink_set, unshrink_sets};

/// Most methods require &mut because they cache intermediate results.
pub struct Analysis<'a> {
    fbas_original: &'a Fbas,
    organizations_original: Option<&'a Organizations<'a>>,
    fbas_shrunken: Fbas,
    unshrink_table: Vec<NodeId>,
    has_quorum_intersection: Option<bool>,
    minimal_quorums_shrunken: Option<Vec<NodeIdSet>>,
    minimal_blocking_sets_shrunken: Option<Vec<NodeIdSet>>,
    minimal_splitting_sets_shrunken: Option<Vec<NodeIdSet>>,
    expect_quorum_intersection: bool,
}
impl<'a> Analysis<'a> {
    pub fn new(fbas: &'a Fbas) -> Self {
        Self::new_with_options(fbas, None, true)
    }
    pub fn new_with_options(
        fbas: &'a Fbas,
        organizations: Option<&'a Organizations<'a>>,
        expect_quorum_intersection: bool,
    ) -> Self {
        debug!(
            "Shrinking FBAS of size {} to set of strongly connected nodes (for performance)...",
            fbas.number_of_nodes()
        );
        let strongly_connected_nodes =
            reduce_to_strongly_connected_nodes(fbas.unsatisfiable_nodes().0, fbas).0;
        let (fbas_shrunken, unshrink_table, _) = Fbas::shrunken(fbas, strongly_connected_nodes);
        debug!(
            "Shrank to an FBAS of size {}.",
            fbas_shrunken.number_of_nodes()
        );
        Analysis {
            fbas_original: fbas,
            organizations_original: organizations,
            fbas_shrunken,
            unshrink_table,
            has_quorum_intersection: None,
            minimal_quorums_shrunken: None,
            minimal_blocking_sets_shrunken: None,
            minimal_splitting_sets_shrunken: None,
            expect_quorum_intersection,
        }
    }
    pub fn has_quorum_intersection(&mut self) -> bool {
        if self.has_quorum_intersection.is_none() {
            info!("Checking for intersection of all minimal quorums...");
            let minimal_quorums_shrunken = self.minimal_quorums_shrunken();
            self.has_quorum_intersection = Some(
                !minimal_quorums_shrunken.is_empty() && all_intersect(&minimal_quorums_shrunken),
            )
        }
        self.has_quorum_intersection.unwrap()
    }
    pub fn all_nodes(&self) -> Vec<NodeId> {
        (0..self.fbas_original.nodes.len()).collect()
    }
    pub fn all_nodes_collapsed(&self) -> Vec<NodeId> {
        self.maybe_collapse_node_ids(self.all_nodes())
    }
    pub fn satisfiable_nodes(&self) -> Vec<NodeId> {
        let (satisfiable, _) =
            find_unsatisfiable_nodes(&self.all_nodes().into_iter().collect(), self.fbas_original);
        self.maybe_collapse_node_ids(satisfiable.into_iter())
    }
    pub fn unsatisfiable_nodes(&self) -> Vec<NodeId> {
        let (_, unsatisfiable) =
            find_unsatisfiable_nodes(&self.all_nodes().into_iter().collect(), self.fbas_original);
        self.maybe_collapse_node_ids(unsatisfiable.into_iter())
    }
    pub fn minimal_quorums(&mut self) -> Vec<NodeIdSet> {
        let minimal_quorums_shrunken = self.minimal_quorums_shrunken();
        self.unshrink_sets(&minimal_quorums_shrunken)
    }
    pub fn minimal_blocking_sets(&mut self) -> Vec<NodeIdSet> {
        let minimal_blocking_sets_shrunken = self.minimal_blocking_sets_shrunken();
        self.unshrink_sets(&minimal_blocking_sets_shrunken)
    }
    pub fn minimal_splitting_sets(&mut self) -> Vec<NodeIdSet> {
        let minimal_splitting_sets_shrunken = self.minimal_splitting_sets_shrunken();
        self.unshrink_sets(&minimal_splitting_sets_shrunken)
    }
    fn minimal_quorums_shrunken(&mut self) -> Vec<NodeIdSet> {
        if self.minimal_quorums_shrunken.is_none() {
            self.find_and_cache_minimal_quorums();
        } else {
            info!("Using cached minimal quorums.");
        }
        self.minimal_quorums_shrunken.clone().unwrap()
    }
    fn minimal_blocking_sets_shrunken(&mut self) -> Vec<NodeIdSet> {
        if self.minimal_blocking_sets_shrunken.is_none() {
            warn!("Computing minimal blocking sets...");
            self.minimal_blocking_sets_shrunken =
                Some(find_minimal_blocking_sets(&self.minimal_quorums_shrunken()));
        } else {
            info!("Using cached minimal blocking sets.");
        }
        self.minimal_blocking_sets_shrunken.clone().unwrap()
    }
    pub fn minimal_splitting_sets_shrunken(&mut self) -> Vec<NodeIdSet> {
        if self.minimal_splitting_sets_shrunken.is_none() {
            warn!("Computing minimal splitting sets...");
            self.minimal_splitting_sets_shrunken = Some(find_minimal_splitting_sets(
                &self.minimal_quorums_shrunken(),
            ));
        } else {
            info!("Using cached minimal splitting sets.");
        }
        self.minimal_splitting_sets_shrunken.clone().unwrap()
    }
    pub fn symmetric_quorum_clusters(&self) -> Vec<QuorumSet> {
        find_symmetric_quorum_clusters(self.fbas_original)
    }
    pub fn top_tier(&mut self) -> Vec<NodeId> {
        // TODO Refactor
        let involved_nodes_shrunken = involved_nodes(&self.minimal_quorums_shrunken());
        self.unshrink_set(&involved_nodes_shrunken.into_iter().collect())
            .into_iter()
            .collect()
    }
    fn find_and_cache_minimal_quorums(&mut self) {
        warn!("Computing minimal quorums...");
        let mut minimal_quorums_shrunken = if self.expect_quorum_intersection {
            find_minimal_quorums(&self.fbas_shrunken)
        } else {
            // FIXME : this should be handled differently
            find_nonintersecting_quorums(&self.fbas_shrunken)
        };
        debug!("Shrinking FBAS again, to top tier (for performance)...",);
        let top_tier = self.unshrink_set(&involved_nodes(&minimal_quorums_shrunken));
        let (new_fbas_shrunken, new_unshrink_table, new_shrink_map) =
            Fbas::shrunken(&self.fbas_original, top_tier);
        debug!(
            "Shrank to an FBAS of size {} (from size {}).",
            new_fbas_shrunken.number_of_nodes(),
            self.fbas_shrunken.number_of_nodes(),
        );
        minimal_quorums_shrunken = reshrink_sets(
            &minimal_quorums_shrunken,
            &self.unshrink_table,
            &new_shrink_map,
        );
        self.fbas_shrunken = new_fbas_shrunken;
        self.unshrink_table = new_unshrink_table;
        let shrink_map = new_shrink_map;

        // if an organizations structure has been passed: collapse
        if let Some(ref orgs) = self.organizations_original {
            debug!("Collapsing nodes by organization...");
            info!(
                "{} involved nodes before collapsing by organization.",
                involved_nodes(&minimal_quorums_shrunken).len()
            );
            let orgs_shrunken = Organizations::shrunken(&orgs, &shrink_map, &self.fbas_shrunken);
            minimal_quorums_shrunken = remove_non_minimal_node_sets(
                orgs_shrunken.collapse_node_sets(minimal_quorums_shrunken),
            );
            info!(
                "{} involved nodes after collapsing by organization.",
                involved_nodes(&minimal_quorums_shrunken).len()
            );
        }
        self.minimal_quorums_shrunken = Some(minimal_quorums_shrunken);

        if log_enabled!(Warn) {
            if self.has_quorum_intersection() {
                debug!("FBAS enjoys quorum intersection.");
            } else {
                warn!("FBAS doesn't enjoy quorum intersection!");
            }
        }
    }
    fn maybe_collapse_node_ids(&self, node_ids: impl IntoIterator<Item = NodeId>) -> Vec<NodeId> {
        if let Some(ref orgs) = self.organizations_original {
            orgs.collapse_node_set(node_ids.into_iter().collect())
                .into_iter()
                .collect()
        } else {
            node_ids.into_iter().collect()
        }
    }
    fn unshrink_set(&self, node_set: &NodeIdSet) -> NodeIdSet {
        unshrink_set(node_set, &self.unshrink_table)
    }
    fn unshrink_sets(&self, node_sets: &[NodeIdSet]) -> Vec<NodeIdSet> {
        unshrink_sets(node_sets, &self.unshrink_table)
    }
}

/// Returns (number_of_sets, number_of_distinct_nodes, <minmaxmean_set_size>)
pub fn describe(node_sets: &[NodeIdSet]) -> (usize, usize, usize, usize, f64) {
    let (min, max, mean) = minmaxmean(node_sets);
    let involved_nodes = involved_nodes(node_sets);
    (node_sets.len(), involved_nodes.len(), min, max, mean)
}

/// Returns (number_of_sets, number_of_distinct_nodes, <histogram>)
pub fn describe_with_histogram(node_sets: &[NodeIdSet]) -> (usize, usize, Vec<usize>) {
    let histogram = histogram(node_sets);
    let involved_nodes = involved_nodes(node_sets);
    (node_sets.len(), involved_nodes.len(), histogram)
}

/// Returns (min_set_size, max_set_size, mean_set_size)
pub fn minmaxmean(node_sets: &[NodeIdSet]) -> (usize, usize, f64) {
    let min = node_sets.iter().map(|s| s.len()).min().unwrap_or(0);
    let max = node_sets.iter().map(|s| s.len()).max().unwrap_or(0);
    let mean = if node_sets.is_empty() {
        0.0
    } else {
        node_sets.iter().map(|s| s.len()).sum::<usize>() as f64 / (node_sets.len() as f64)
    };
    (min, max, mean)
}

/// Returns [ #members with size 0, #members with size 1, ... , #members with maximum size ]
pub fn histogram(node_sets: &[NodeIdSet]) -> Vec<usize> {
    let max = node_sets.iter().map(|s| s.len()).max().unwrap_or(0);
    let mut histogram: Vec<usize> = vec![0; max + 1];
    for node_set in node_sets.iter() {
        let size = node_set.len();
        histogram[size] = histogram[size].checked_add(1).unwrap();
    }
    histogram
}

pub fn all_intersect(node_sets: &[NodeIdSet]) -> bool {
    node_sets
        .iter()
        .enumerate()
        .all(|(i, x)| node_sets.iter().skip(i + 1).all(|y| !x.is_disjoint(y)))
}

pub fn involved_nodes(node_sets: &[NodeIdSet]) -> NodeIdSet {
    let mut all_nodes: NodeIdSet = bitset![];
    for node_set in node_sets {
        all_nodes.union_with(node_set);
    }
    all_nodes
}

/// Reduce to minimal node sets, i.e. to a set of node sets so that no member set is a superset of another.
pub fn remove_non_minimal_node_sets(mut node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
    debug!("Removing duplicates...");
    let len_before = node_sets.len();
    node_sets.sort();
    node_sets.dedup();
    debug!("Done; removed {} duplicates.", len_before - node_sets.len());

    debug!("Sorting node sets into buckets, by length...");
    let max_len_upper_bound = node_sets.iter().map(|x| x.len()).max().unwrap_or(0) + 1;
    let mut buckets_by_len: Vec<Vec<NodeIdSet>> = vec![vec![]; max_len_upper_bound];
    for node_set in node_sets.into_iter() {
        buckets_by_len[node_set.len()].push(node_set);
    }
    debug!(
        "Sorting done; #nodes per bucket: {:?}",
        buckets_by_len
            .iter()
            .map(|x| x.len())
            .enumerate()
            .collect::<Vec<(usize, usize)>>()
    );
    remove_non_minimal_node_sets_from_buckets(buckets_by_len)
}

pub fn contains_only_minimal_node_sets(node_sets: &[NodeIdSet]) -> bool {
    node_sets.iter().all(|x| {
        node_sets
            .iter()
            .filter(|&y| x != y)
            .all(|y| !y.is_subset(x))
    })
}

fn remove_non_minimal_node_sets_from_buckets(
    buckets_by_len: Vec<impl IntoIterator<Item = NodeIdSet>>,
) -> Vec<NodeIdSet> {
    debug!("Filtering non-minimal node sets...");
    let mut minimal_node_sets: Vec<NodeIdSet> = vec![];
    let mut minimal_node_sets_current_len: Vec<NodeIdSet> = vec![];
    for (i, bucket) in buckets_by_len.into_iter().enumerate() {
        debug!(
            "...at bucket {}; {} minimal node sets",
            i,
            minimal_node_sets.len()
        );
        for node_set in bucket.into_iter() {
            if minimal_node_sets.iter().all(|x| !x.is_subset(&node_set)) {
                minimal_node_sets_current_len.push(node_set);
            }
        }
        minimal_node_sets.append(&mut minimal_node_sets_current_len);
    }
    debug!("Filtering done.");
    debug_assert!(contains_only_minimal_node_sets(&minimal_node_sets));
    minimal_node_sets
}

// For each member node set, check if one of its "smaller by one" subsets is also a member.
// If yes, then filter it out, as it is obviously non-minimal.
// This function can be used to reduce (in some cases even eliminate) the workload on the slower
// `remove_non_minimal_node_sets`.
fn remove_node_sets_that_are_non_minimal_by_one(node_sets: HashSet<NodeIdSet>) -> Vec<NodeIdSet> {
    let mut remaining_sets = vec![];
    let mut tester: NodeIdSet;
    let mut is_minimal_by_one;

    debug!("Filtering node sets that are non-minimal by one...");
    for (i, node_set) in node_sets.iter().enumerate() {
        if i % 100_000 == 0 {
            debug!(
                "...at node set {}; {} remaining sets",
                i,
                remaining_sets.len()
            );
        }
        is_minimal_by_one = true;
        // whyever, using clone() here seems to be faster than clone_from()
        tester = node_set.clone();

        for node_id in node_set.iter() {
            tester.remove(node_id);
            if node_sets.contains(&tester) {
                is_minimal_by_one = false;
                break;
            }
            tester.insert(node_id);
        }
        if is_minimal_by_one {
            remaining_sets.push(node_set.clone());
        }
    }
    debug!("Filtering done.");
    remaining_sets
}

impl<'fbas> Organizations<'fbas> {
    /// Collapse a node ID so that all nodes by the same organization get the same ID.
    pub fn collapse_node(self: &Self, node_id: NodeId) -> NodeId {
        self.collapsed_ids[node_id]
    }
    /// Collapse a node ID set so that all nodes by the same organization get the same ID.
    pub fn collapse_node_set(self: &Self, node_set: NodeIdSet) -> NodeIdSet {
        node_set
            .into_iter()
            .map(|x| self.collapse_node(x))
            .collect()
    }
    /// Collapse a list of node ID sets so that all nodes by the same organization get the same ID.
    pub fn collapse_node_sets(self: &Self, node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
        node_sets
            .into_iter()
            .map(|x| self.collapse_node_set(x))
            .collect()
    }
}

impl Fbas {
    pub fn has_quorum_intersection(&self) -> bool {
        Analysis::new(&self).has_quorum_intersection()
    }
    fn unsatisfiable_nodes(&self) -> (NodeIdSet, NodeIdSet) {
        let all_nodes = (0..self.nodes.len()).collect();
        find_unsatisfiable_nodes(&all_nodes, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn node_set_intersections() {
        assert!(all_intersect(&vec![
            bitset![0, 1],
            bitset![0, 2],
            bitset![1, 2]
        ]));
        assert!(!all_intersect(&vec![bitset![0], bitset![1, 2]]));
    }

    #[test]
    fn has_quorum_intersection_trivial() {
        let correct = Fbas::from_json_file(Path::new("test_data/correct_trivial.json"));
        let broken = Fbas::from_json_file(Path::new("test_data/broken_trivial.json"));

        assert!(Analysis::new(&correct).has_quorum_intersection());
        assert!(!Analysis::new(&broken).has_quorum_intersection());
    }

    #[test]
    fn has_quorum_intersection_nontrivial() {
        let correct = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let broken = Fbas::from_json_file(Path::new("test_data/broken.json"));

        assert!(Analysis::new(&correct).has_quorum_intersection());
        assert!(!Analysis::new(&broken).has_quorum_intersection());
    }

    #[test]
    fn has_quorum_intersection_if_just_one_quorum() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            }
        ]"#,
        );
        assert!(Analysis::new(&fbas).has_quorum_intersection());
    }

    #[test]
    fn no_has_quorum_intersection_if_there_is_no_quorum() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            }
        ]"#,
        );
        assert!(!Analysis::new(&fbas).has_quorum_intersection());
    }

    #[test]
    fn analysis_nontrivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let mut analysis = Analysis::new(&fbas);

        assert!(analysis.has_quorum_intersection());
        assert_eq!(
            describe_with_histogram(&analysis.minimal_quorums()),
            describe_with_histogram(&[bitset![0, 1], bitset![0, 10], bitset![1, 10]])
        );
        assert_eq!(
            describe_with_histogram(&analysis.minimal_blocking_sets()),
            describe_with_histogram(&[bitset![0, 1], bitset![0, 10], bitset![1, 10]])
        );
        assert_eq!(
            describe_with_histogram(&analysis.minimal_splitting_sets()),
            describe_with_histogram(&[bitset![0], bitset![1], bitset![10]])
        );
    }

    #[test]
    fn analysis_with_collapsing_by_organization_nontrivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let organizations = Organizations::from_json_str(
            r#"[
            {
                "id": "266107f8966d45eedce41fee2581326d",
                "name": "Stellar Development Foundation",
                "validators": [
                    "GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK",
                    "GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH",
                    "GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ"
                ]
            }]"#,
            &fbas,
        );
        let mut analysis = Analysis::new_with_options(&fbas, Some(&organizations), true);

        assert!(analysis.has_quorum_intersection());
        assert_eq!(analysis.minimal_quorums().len(), 1);
        assert_eq!(analysis.minimal_blocking_sets().len(), 1);
        assert_eq!(analysis.minimal_splitting_sets().len(), 1);
    }

    #[test]
    fn minimal_quorums_id_ordering() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            },
            {
                "publicKey": "n1",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            },
            {
                "publicKey": "n2",
                "quorumSet": { "threshold": 2, "validators": ["n1", "n2"] }
            }
        ]"#,
        );
        let mut analysis = Analysis::new(&fbas);
        let expected = vec![bitset![1, 2]];
        let actual = analysis.minimal_quorums();
        assert_eq!(expected, actual);
    }

    #[test]
    fn node_sets_describe() {
        let node_sets = vec![
            bitset![0, 1],
            bitset![2, 3],
            bitset![4, 5, 6, 7],
            bitset![1, 4],
        ];
        let actual = describe(&node_sets);
        let expected = (4, 8, 2, 4, 2.5);
        assert_eq!(expected, actual)
    }

    #[test]
    fn node_sets_histogram() {
        let node_sets = vec![
            bitset![0, 1],
            bitset![2, 3],
            bitset![4, 5, 6, 7],
            bitset![1, 4],
        ];
        let actual = histogram(&node_sets);
        let expected = vec![0, 0, 3, 0, 1];
        assert_eq!(expected, actual)
    }

    #[test]
    fn minimize_node_sets() {
        let non_minimal = vec![bitset![0, 1, 2], bitset![0, 1], bitset![0, 2]];
        let expected = vec![bitset![0, 1], bitset![0, 2]];
        let actual = remove_non_minimal_node_sets(non_minimal);
        assert_eq!(expected, actual);
    }

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
