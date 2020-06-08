use super::*;
use log::log_enabled;
use log::Level::Warn;

mod blocking_sets;
mod quorums;
mod rank;
mod shrink;
mod splitting_sets;

pub use blocking_sets::find_minimal_blocking_sets;
pub use quorums::{find_minimal_quorums, find_nonintersecting_quorums, find_symmetric_clusters};
pub use splitting_sets::find_minimal_splitting_sets;

pub(crate) use rank::*;

use quorums::{find_unsatisfiable_nodes, reduce_to_strongly_connected_nodes}; // TODO why in quorums?
use shrink::{reshrink_sets, unshrink_set, unshrink_sets};

/// Front end for all interesting FBAS analyses. Caches intermediate results
/// (hence some methods require `&mut`).
pub struct Analysis<'a> {
    fbas_original: &'a Fbas,
    organizations_original: Option<&'a Organizations<'a>>,
    fbas_shrunken: Fbas,
    unshrink_table: Vec<NodeId>,
    has_quorum_intersection: Option<bool>,
    minimal_quorums_shrunken: Option<Vec<NodeIdSet>>,
    minimal_blocking_sets_shrunken: Option<Vec<NodeIdSet>>,
    minimal_splitting_sets_shrunken: Option<Vec<NodeIdSet>>,
}
impl<'a> Analysis<'a> {
    /// Start a new `Analysis`. If `organizations` is set, nodes belonging to the same organization
    /// will be merged into one node in analysis results.
    pub fn new(fbas: &'a Fbas, organizations: Option<&'a Organizations<'a>>) -> Self {
        debug!(
            "Shrinking FBAS of size {} to set of strongly connected nodes (for performance)...",
            fbas.number_of_nodes()
        );
        let strongly_connected_nodes =
            reduce_to_strongly_connected_nodes(fbas.unsatisfiable_nodes(), fbas).0;
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
        }
    }
    /// Whether nodes belonging to the same organization will be merged in analysis results.
    pub fn merging_by_organization(&self) -> bool {
        self.organizations_original.is_some()
    }
    /// Raw nodes in the analyzed FBAS, not filtered and not merged by organization.
    pub fn all_physical_nodes(&self) -> NodeIdSetResult {
        self.make_unshrunken_set_result(self.fbas_original.all_nodes())
    }
    /// Nodes in the analyzed FBAS. Raw nodes if no organizations are given, one node per
    /// organization else. Not filtered.
    pub fn all_nodes(&self) -> NodeIdSetResult {
        self.make_unshrunken_set_result(
            self.maybe_merge_node_ids_by_organization(self.fbas_original.all_nodes()),
        )
    }
    /// Nodes in the analyzed FBAS that can be satisfied given their quorum sets and the nodes
    /// existing in the FBAS.
    pub fn satisfiable_nodes(&self) -> NodeIdSetResult {
        let satisfiable_nodes =
            self.maybe_merge_node_ids_by_organization(self.fbas_original.satisfiable_nodes());
        self.make_unshrunken_set_result(satisfiable_nodes)
    }
    /// Nodes in the analyzed FBAS that can never be satisfied given their quorum sets and the
    /// nodes existing in the FBAS.
    pub fn unsatisfiable_nodes(&self) -> NodeIdSetResult {
        let unsatisfiable_nodes =
            self.maybe_merge_node_ids_by_organization(self.fbas_original.unsatisfiable_nodes());
        self.make_unshrunken_set_result(unsatisfiable_nodes)
    }
    /// Regular quorum intersection check via finding all minimal quorums.
    /// Algorithm inspired by [Lachowski 2019](https://arxiv.org/abs/1902.06493)).
    pub fn has_quorum_intersection(&mut self) -> bool {
        if self.has_quorum_intersection.is_none() {
            self.find_and_cache_has_quorum_intersection();
        }
        self.has_quorum_intersection.unwrap()
    }
    /// Quorum intersection check that works faster for FBASs that do not enjoy quorum
    /// intersection.
    pub fn has_quorum_intersection_via_alternative_check(
        &self,
    ) -> (bool, Option<NodeIdSetVecResult>) {
        if let Some(quorums) = find_nonintersecting_quorums(&self.fbas_shrunken) {
            assert!(quorums[0].is_disjoint(&quorums[1]));
            (
                false,
                Some(NodeIdSetVecResult::new(
                    quorums.to_vec(),
                    Some(&self.unshrink_table),
                )),
            )
        } else {
            (true, None)
        }
    }
    /// Minimal quorums - no proper subset of any of these node sets is a quorum.
    pub fn minimal_quorums(&mut self) -> NodeIdSetVecResult {
        let minimal_quorums_shrunken = self.minimal_quorums_shrunken();
        self.make_shrunken_set_vec_result(minimal_quorums_shrunken)
    }
    /// Minimal blocking sets - minimal indispensable sets for global liveness.
    pub fn minimal_blocking_sets(&mut self) -> NodeIdSetVecResult {
        let minimal_blocking_sets_shrunken = self.minimal_blocking_sets_shrunken();
        self.make_shrunken_set_vec_result(minimal_blocking_sets_shrunken)
    }
    /// Minimal splitting sets - minimal indispensable sets for safety.
    pub fn minimal_splitting_sets(&mut self) -> NodeIdSetVecResult {
        let minimal_splitting_sets_shrunken = self.minimal_splitting_sets_shrunken();
        self.make_shrunken_set_vec_result(minimal_splitting_sets_shrunken)
    }
    /// Top tier - the set of nodes exclusively relevant when determining minimal blocking sets and
    /// minimal splitting sets.
    pub fn top_tier(&mut self) -> NodeIdSetResult {
        let top_tier = involved_nodes(&self.minimal_quorums_shrunken());
        self.make_shrunken_set_result(top_tier)
    }
    /// Symmetric clusters - sets of nodes in which each two nodes have the same quorum set.
    /// Here, each found symmetric cluster is represented by its common quorum set.
    pub fn symmetric_clusters(&self) -> Vec<QuorumSet> {
        let clusters = find_symmetric_clusters(self.fbas_original);
        if let Some(ref orgs) = self.organizations_original {
            orgs.merge_quorum_sets(clusters)
        } else {
            clusters
        }
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
            self.find_and_cache_minimal_blocking_sets();
        } else {
            info!("Using cached minimal blocking sets.");
        }
        self.minimal_blocking_sets_shrunken.clone().unwrap()
    }
    fn minimal_splitting_sets_shrunken(&mut self) -> Vec<NodeIdSet> {
        if self.minimal_splitting_sets_shrunken.is_none() {
            self.find_and_cache_minimal_splitting_sets();
        } else {
            info!("Using cached minimal splitting sets.");
        }
        self.minimal_splitting_sets_shrunken.clone().unwrap()
    }

    fn find_and_cache_has_quorum_intersection(&mut self) {
        info!("Checking for intersection of all minimal quorums...");
        let minimal_quorums_shrunken = self.minimal_quorums_shrunken();
        self.has_quorum_intersection =
            Some(!minimal_quorums_shrunken.is_empty() && all_intersect(&minimal_quorums_shrunken));
    }
    fn find_and_cache_minimal_quorums(&mut self) {
        info!("Computing minimal quorums...");
        let mut minimal_quorums_shrunken = find_minimal_quorums(&self.fbas_shrunken);
        debug!("Shrinking FBAS again, to top tier (for performance)...",);
        let top_tier_original = unshrink_set(
            &involved_nodes(&minimal_quorums_shrunken),
            &self.unshrink_table,
        );
        let (new_fbas_shrunken, new_unshrink_table, new_shrink_map) =
            Fbas::shrunken(&self.fbas_original, top_tier_original);
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

        // if an organizations structure has been passed: merge nodes
        if let Some(ref orgs) = self.organizations_original {
            debug!("Collapsing nodes by organization...");
            info!(
                "{} involved nodes before collapsing by organization.",
                involved_nodes(&minimal_quorums_shrunken).len()
            );
            let orgs_shrunken = Organizations::shrunken(&orgs, &shrink_map, &self.fbas_shrunken);
            minimal_quorums_shrunken = remove_non_minimal_node_sets(
                orgs_shrunken.merge_node_sets(minimal_quorums_shrunken),
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
    fn find_and_cache_minimal_blocking_sets(&mut self) {
        info!("Computing minimal blocking sets...");
        self.minimal_blocking_sets_shrunken =
            Some(find_minimal_blocking_sets(&self.minimal_quorums_shrunken()));
    }
    fn find_and_cache_minimal_splitting_sets(&mut self) {
        info!("Computing minimal splitting sets...");
        self.minimal_splitting_sets_shrunken = Some(find_minimal_splitting_sets(
            &self.minimal_quorums_shrunken(),
        ));
    }
    fn maybe_merge_node_ids_by_organization(&self, node_set: NodeIdSet) -> NodeIdSet {
        if let Some(ref orgs) = self.organizations_original {
            orgs.merge_node_set(node_set)
        } else {
            node_set
        }
    }
    fn make_unshrunken_set_result(&self, payload: NodeIdSet) -> NodeIdSetResult {
        NodeIdSetResult::new(payload, None)
    }
    fn make_shrunken_set_result(&self, payload: NodeIdSet) -> NodeIdSetResult {
        NodeIdSetResult::new(payload, Some(&self.unshrink_table))
    }
    fn make_shrunken_set_vec_result(&self, payload: Vec<NodeIdSet>) -> NodeIdSetVecResult {
        NodeIdSetVecResult::new(payload, Some(&self.unshrink_table))
    }
}

#[derive(Debug, Clone)]
pub struct NodeIdSetResult<'a> {
    pub(crate) node_set: NodeIdSet,
    pub(crate) unshrink_table: Option<&'a [NodeId]>,
}
impl<'a> NodeIdSetResult<'a> {
    pub fn new(node_set: NodeIdSet, unshrink_table: Option<&'a [NodeId]>) -> Self {
        NodeIdSetResult {
            node_set,
            unshrink_table,
        }
    }
    pub fn unwrap(self) -> NodeIdSet {
        if let Some(unshrink_table) = self.unshrink_table {
            unshrink_set(&self.node_set, unshrink_table)
        } else {
            self.node_set
        }
    }
    pub fn into_vec(self) -> Vec<NodeId> {
        self.unwrap().into_iter().collect()
    }
    pub fn involved_nodes(&self) -> NodeIdSet {
        self.node_set.clone()
    }
    pub fn len(&self) -> usize {
        self.node_set.len()
    }
    pub fn is_empty(&self) -> bool {
        self.node_set.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct NodeIdSetVecResult<'a> {
    pub(crate) node_sets: Vec<NodeIdSet>,
    pub(crate) unshrink_table: Option<&'a [NodeId]>,
}
impl<'a> NodeIdSetVecResult<'a> {
    pub fn new(node_sets: Vec<NodeIdSet>, unshrink_table: Option<&'a [NodeId]>) -> Self {
        NodeIdSetVecResult {
            node_sets,
            unshrink_table,
        }
    }
    pub fn unwrap(self) -> Vec<NodeIdSet> {
        if let Some(unshrink_table) = self.unshrink_table {
            unshrink_sets(&self.node_sets, unshrink_table)
        } else {
            self.node_sets
        }
    }
    pub fn into_vec_vec(self) -> Vec<Vec<NodeId>> {
        self.node_sets
            .iter()
            .map(|node_set| {
                if let Some(unshrink_table) = self.unshrink_table {
                    unshrink_set(node_set, unshrink_table).into_iter().collect()
                } else {
                    node_set.iter().collect()
                }
            })
            .collect()
    }
    pub fn involved_nodes(&self) -> NodeIdSet {
        involved_nodes(&self.node_sets)
    }
    pub fn len(&self) -> usize {
        self.node_sets.len()
    }
    pub fn is_empty(&self) -> bool {
        self.node_sets.is_empty()
    }
    /// Returns (number_of_sets, number_of_distinct_nodes, <minmaxmean_set_size>, <histogram>)
    pub fn describe(&self) -> (usize, usize, (usize, usize, f64), Vec<usize>) {
        (
            self.node_sets.len(),
            self.involved_nodes().len(),
            self.minmaxmean(),
            self.histogram(),
        )
    }
    /// Returns (min_set_size, max_set_size, mean_set_size)
    pub fn minmaxmean(&self) -> (usize, usize, f64) {
        let min = self.node_sets.iter().map(|s| s.len()).min().unwrap_or(0);
        let max = self.node_sets.iter().map(|s| s.len()).max().unwrap_or(0);
        let mean = if self.node_sets.is_empty() {
            0.0
        } else {
            self.node_sets.iter().map(|s| s.len()).sum::<usize>() as f64
                / (self.node_sets.len() as f64)
        };
        (min, max, mean)
    }
    /// Returns [ #members with size 0, #members with size 1, ... , #members with maximum size ]
    pub fn histogram(&self) -> Vec<usize> {
        let max = self.node_sets.iter().map(|s| s.len()).max().unwrap_or(0);
        let mut histogram: Vec<usize> = vec![0; max + 1];
        for node_set in self.node_sets.iter() {
            let size = node_set.len();
            histogram[size] = histogram[size].checked_add(1).unwrap();
        }
        histogram
    }
}

pub fn all_intersect(node_sets: &[NodeIdSet]) -> bool {
    // quick check
    let max_size = involved_nodes(node_sets).len();
    if node_sets.iter().all(|x| x.len() > max_size / 2) {
        true
    } else {
        // slow check
        node_sets
            .iter()
            .enumerate()
            .all(|(i, x)| node_sets.iter().skip(i + 1).all(|y| !x.is_disjoint(y)))
    }
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
    /// Merge a node ID so that all nodes by the same organization get the same ID.
    pub fn merge_node(self: &Self, node_id: NodeId) -> NodeId {
        self.merged_ids[node_id]
    }
    /// Merge a node ID set so that all nodes by the same organization get the same ID.
    pub fn merge_node_set(self: &Self, node_set: NodeIdSet) -> NodeIdSet {
        node_set.into_iter().map(|x| self.merge_node(x)).collect()
    }
    /// Merge a list of node ID sets so that all nodes by the same organization get the same ID.
    pub fn merge_node_sets(self: &Self, node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
        node_sets
            .into_iter()
            .map(|x| self.merge_node_set(x))
            .collect()
    }
    /// Merge a quorum set so that all nodes by the same organization get the same ID and
    /// validator lists consisting of only of one organization are collapsed into one validator.
    pub fn merge_quorum_set(self: &Self, quorum_set: QuorumSet) -> QuorumSet {
        let mut threshold = quorum_set.threshold;
        let mut validators: Vec<NodeId> = quorum_set
            .validators
            .iter()
            .map(|&x| self.merge_node(x))
            .collect();

        let (new_validator_candidates, inner_quorum_sets): (Vec<QuorumSet>, Vec<QuorumSet>) =
            quorum_set
                .inner_quorum_sets
                .into_iter()
                .map(|q| self.merge_quorum_set(q))
                .partition(|q| q.validators.len() == 1);

        validators.extend(
            new_validator_candidates
                .into_iter()
                .map(|q| q.validators[0]),
        );
        if !validators.is_empty() && validators.iter().all(|&v| v == validators[0]) {
            validators = vec![validators[0]];
            threshold = 1;
        }
        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
    }
    /// calls `merge_quorum_set` on each vector element
    pub fn merge_quorum_sets(self: &Self, quorum_set: Vec<QuorumSet>) -> Vec<QuorumSet> {
        quorum_set
            .into_iter()
            .map(|q| self.merge_quorum_set(q))
            .collect()
    }
}

impl Fbas {
    pub fn unsatisfiable_nodes(&self) -> NodeIdSet {
        find_unsatisfiable_nodes(&self.all_nodes(), self).0
    }
    pub fn satisfiable_nodes(&self) -> NodeIdSet {
        find_unsatisfiable_nodes(&self.all_nodes(), self).1
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

        assert!(Analysis::new(&correct, None).has_quorum_intersection());
        assert!(!Analysis::new(&broken, None).has_quorum_intersection());
    }

    #[test]
    fn has_quorum_intersection_nontrivial() {
        let correct = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let broken = Fbas::from_json_file(Path::new("test_data/broken.json"));

        assert!(Analysis::new(&correct, None).has_quorum_intersection());
        assert!(!Analysis::new(&broken, None).has_quorum_intersection());
    }

    #[test]
    fn has_quorum_intersection_if_just_one_quorum() {
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
        assert!(Analysis::new(&fbas, None).has_quorum_intersection());
    }

    #[test]
    fn no_has_quorum_intersection_if_there_is_no_quorum() {
        let fbas = Fbas::from_json_str(
            r#"[
            {
                "publicKey": "n0",
                "quorumSet": { "threshold": 2, "validators": ["n0", "n1"] }
            }
        ]"#,
        );
        assert!(!Analysis::new(&fbas, None).has_quorum_intersection());
    }

    #[test]
    fn analysis_nontrivial() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let mut analysis = Analysis::new(&fbas, None);

        assert!(analysis.has_quorum_intersection());
        assert_eq!(
            analysis.minimal_quorums().describe(),
            NodeIdSetVecResult::new(vec![bitset![0, 1], bitset![0, 10], bitset![1, 10]], None)
                .describe()
        );
        assert_eq!(
            analysis.minimal_blocking_sets().describe(),
            NodeIdSetVecResult::new(vec![bitset![0, 1], bitset![0, 10], bitset![1, 10]], None)
                .describe()
        );
        assert_eq!(
            analysis.minimal_splitting_sets().describe(),
            NodeIdSetVecResult::new(vec![bitset![0], bitset![1], bitset![10]], None).describe()
        );
    }

    #[test]
    fn alternative_check_on_broken() {
        let fbas = Fbas::from_json_file(Path::new("test_data/broken.json"));
        let analysis = Analysis::new(&fbas, None);

        let (has_intersection, quorums) = analysis.has_quorum_intersection_via_alternative_check();

        assert!(!has_intersection);

        let quorums: Vec<NodeIdSet> = quorums.unwrap().unwrap();

        assert_eq!(quorums.len(), 2);
        assert!(fbas.is_quorum(&quorums[0]));
        assert!(fbas.is_quorum(&quorums[1]));
        assert!(quorums[0].is_disjoint(&quorums[1]));
    }

    #[test]
    fn analysis_with_merging_by_organization_nontrivial() {
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
        let mut analysis = Analysis::new(&fbas, Some(&organizations));

        assert!(analysis.has_quorum_intersection());
        assert_eq!(analysis.minimal_quorums().len(), 1);
        assert_eq!(analysis.minimal_blocking_sets().len(), 1);
        assert_eq!(analysis.minimal_splitting_sets().len(), 1);
    }

    #[test]
    #[ignore]
    fn top_tier_analysis_big() {
        let fbas = Fbas::from_json_file(Path::new("test_data/stellarbeat_nodes_2019-09-17.json"));
        let organizations = None;
        let mut analysis = Analysis::new(&fbas, organizations.as_ref());

        // calculated with fbas_analyzer v0.1
        let expected = bitset![1, 4, 8, 23, 29, 36, 37, 43, 44, 52, 56, 69, 86, 105, 167, 168, 171];
        let actual = analysis.top_tier().unwrap();
        assert_eq!(expected, actual);
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
        let mut analysis = Analysis::new(&fbas, None);
        let expected = vec![bitset![1, 2]];
        let actual = analysis.minimal_quorums().unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn node_sets_histogram() {
        let node_sets_result = NodeIdSetVecResult::new(
            vec![
                bitset![0, 1],
                bitset![2, 3],
                bitset![4, 5, 6, 7],
                bitset![1, 4],
            ],
            None,
        );
        let actual = node_sets_result.histogram();
        let expected = vec![0, 0, 3, 0, 1];
        assert_eq!(expected, actual)
    }

    #[test]
    fn node_sets_describe() {
        let node_sets_result = NodeIdSetVecResult::new(
            vec![
                bitset![0, 1],
                bitset![2, 3],
                bitset![4, 5, 6, 7],
                bitset![1, 4],
            ],
            None,
        );
        let actual = node_sets_result.describe();
        let expected = (4, 8, (2, 4, 2.5), vec![0, 0, 3, 0, 1]);
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
    fn merge_node_sets_by_organization() {
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
        let actual = organizations.merge_node_sets(node_sets);

        assert_eq!(expected, actual);
    }
}
