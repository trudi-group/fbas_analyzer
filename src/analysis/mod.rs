use super::*;
use log::log_enabled;
use log::Level::Warn;

mod find_blocking_sets;
mod find_quorums;
mod find_splitting_sets;

mod rank;
pub(crate) use rank::*;

pub use find_blocking_sets::find_minimal_blocking_sets;
use find_quorums::reduce_to_strongly_connected_components;
pub use find_quorums::{
    find_minimal_quorums, find_nonintersecting_quorums, find_symmetric_quorum_clusters,
    find_unsatisfiable_nodes,
};
pub use find_splitting_sets::find_minimal_splitting_sets;

/// Most methods require &mut because they cache intermediate results.
pub struct Analysis<'a> {
    fbas: &'a Fbas,
    fbas_shrunken: Fbas,
    unshrink_table: Vec<NodeId>,
    organizations: Option<&'a Organizations<'a>>,
    minimal_quorums: Option<Vec<NodeIdSet>>,
    minimal_quorums_shrunken: Option<Vec<NodeIdSet>>,
    minimal_blocking_sets: Option<Vec<NodeIdSet>>,
    minimal_splitting_sets: Option<Vec<NodeIdSet>>,
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
            "Shrinking FBAS of size {} to save memory...",
            fbas.number_of_nodes()
        );
        let (fbas_shrunken, unshrink_table) = Fbas::shrunken(fbas);
        debug!(
            "Shrank to an FBAS of size {}.",
            fbas_shrunken.number_of_nodes()
        );
        Analysis {
            fbas,
            fbas_shrunken,
            unshrink_table,
            organizations,
            minimal_quorums: None,
            minimal_quorums_shrunken: None,
            minimal_blocking_sets: None,
            minimal_splitting_sets: None,
            expect_quorum_intersection,
        }
    }
    pub fn has_quorum_intersection(&mut self) -> bool {
        info!("Checking for intersection of all minimal quorums...");
        !self.minimal_quorums_shrunken().is_empty()
            && all_intersect(self.minimal_quorums_shrunken())
    }
    pub fn all_nodes(&self) -> Vec<NodeId> {
        (0..self.fbas.nodes.len()).collect()
    }
    pub fn all_nodes_collapsed(&self) -> Vec<NodeId> {
        self.maybe_collapse_node_ids(self.all_nodes())
    }
    pub fn satisfiable_nodes(&self) -> Vec<NodeId> {
        let (satisfiable, _) =
            find_unsatisfiable_nodes(&self.all_nodes().into_iter().collect(), self.fbas);
        self.maybe_collapse_node_ids(satisfiable.into_iter())
    }
    pub fn unsatisfiable_nodes(&self) -> Vec<NodeId> {
        let (_, unsatisfiable) =
            find_unsatisfiable_nodes(&self.all_nodes().into_iter().collect(), self.fbas);
        self.maybe_collapse_node_ids(unsatisfiable.into_iter())
    }
    pub fn minimal_quorums(&mut self) -> &[NodeIdSet] {
        if self.minimal_quorums.is_none() {
            self.find_and_cache_minimal_quorums();
        } else {
            info!("Using cached minimal quorums.");
        }
        self.minimal_quorums.as_ref().unwrap()
    }
    fn minimal_quorums_shrunken(&mut self) -> &[NodeIdSet] {
        if self.minimal_quorums_shrunken.is_none() {
            self.find_and_cache_minimal_quorums();
        } else {
            info!("Using cached minimal quorums.");
        }
        self.minimal_quorums_shrunken.as_ref().unwrap()
    }
    pub fn minimal_blocking_sets(&mut self) -> &[NodeIdSet] {
        if self.minimal_blocking_sets.is_none() {
            warn!("Computing minimal blocking sets...");
            let minimal_blocking_sets_shrunken =
                find_minimal_blocking_sets(self.minimal_quorums_shrunken());
            self.minimal_blocking_sets =
                Some(self.maybe_collapse_minimal_node_sets(
                    self.unshrink(minimal_blocking_sets_shrunken),
                ));
        } else {
            info!("Using cached minimal blocking sets.");
        }
        self.minimal_blocking_sets.as_ref().unwrap()
    }
    pub fn minimal_splitting_sets(&mut self) -> &[NodeIdSet] {
        if self.minimal_splitting_sets.is_none() {
            warn!("Computing minimal splitting sets...");
            let minimal_splitting_sets_shrunken =
                find_minimal_splitting_sets(self.minimal_quorums_shrunken());
            self.minimal_splitting_sets =
                Some(self.maybe_collapse_minimal_node_sets(
                    self.unshrink(minimal_splitting_sets_shrunken),
                ));
        } else {
            info!("Using cached minimal splitting sets.");
        }
        self.minimal_splitting_sets.as_ref().unwrap()
    }
    pub fn symmetric_quorum_clusters(&self) -> Vec<QuorumSet> {
        find_symmetric_quorum_clusters(self.fbas)
    }
    pub fn top_tier(&mut self) -> Vec<NodeId> {
        involved_nodes(self.minimal_quorums())
    }
    fn find_and_cache_minimal_quorums(&mut self) {
        warn!("Computing minimal quorums...");
        let minimal_quorums_shrunken = if self.expect_quorum_intersection {
            find_minimal_quorums(&self.fbas_shrunken)
        } else {
            find_nonintersecting_quorums(&self.fbas_shrunken)
        };
        let minimal_quorums =
            self.maybe_collapse_minimal_node_sets(self.unshrink(minimal_quorums_shrunken.clone()));
        self.minimal_quorums = Some(minimal_quorums);
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
        if let Some(ref orgs) = self.organizations {
            orgs.collapse_node_set(node_ids.into_iter().collect())
                .into_iter()
                .collect()
        } else {
            node_ids.into_iter().collect()
        }
    }
    fn unshrink(&self, node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
        if self.organizations.is_some() {
            unshrink(node_sets, &self.unshrink_table)
        } else {
            node_sets
        }
    }
    fn maybe_collapse_minimal_node_sets(&self, node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
        if let Some(ref orgs) = self.organizations {
            debug!("Collapsing nodes by organization...");
            info!(
                "{} involved nodes before collapsing by organization.",
                involved_nodes(&node_sets).len()
            );
            let collapsed_node_sets =
                remove_non_minimal_node_sets(orgs.collapse_node_sets(node_sets));
            info!(
                "{} involved nodes after collapsing by organization.",
                involved_nodes(&collapsed_node_sets).len()
            );
            collapsed_node_sets
        } else {
            node_sets
        }
    }
    /// Temporary workaround for being able to map shrunken IDs to correct pretty names
    pub fn internal_fbas_clone(&self) -> Fbas {
        self.fbas_shrunken.clone()
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
    let number_of_involved_nodes = involved_nodes(node_sets).len();

    let mut node_sets_by_size: Vec<(NodeIdSet, usize)> =
        node_sets.iter().map(|ns| (ns.clone(), ns.len())).collect();
    node_sets_by_size.sort_by_key(|x| x.1);

    for (i, (ns1, ns1_len)) in node_sets_by_size.iter().enumerate() {
        if *ns1_len > number_of_involved_nodes / 2 {
            break;
        }
        for (ns2, ns2_len) in node_sets_by_size.iter().skip(i + 1) {
            if ns1_len + ns2_len > number_of_involved_nodes {
                break;
            } else if ns1.is_disjoint(ns2) {
                return false;
            }
        }
    }
    true
}

pub fn involved_nodes(node_sets: &[NodeIdSet]) -> Vec<NodeId> {
    let mut all_nodes: NodeIdSet = bitset![];
    for node_set in node_sets {
        all_nodes.union_with(node_set);
    }
    all_nodes.into_iter().collect()
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
    minimal_node_sets
}

fn unshrink(node_sets: Vec<NodeIdSet>, unshrink_table: &[NodeId]) -> Vec<NodeIdSet> {
    node_sets
        .into_iter()
        .map(|node_set| node_set.into_iter().map(|id| unshrink_table[id]).collect())
        .collect()
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
    fn shrunken(fbas: &Self) -> (Self, Vec<NodeId>) {
        let (satisfiable_nodes, _) = fbas.unsatisfiable_nodes();
        let (strongly_connected_nodes, _) =
            reduce_to_strongly_connected_components(satisfiable_nodes, fbas);

        let shrink_map: HashMap<NodeId, NodeId> = strongly_connected_nodes
            .iter()
            .enumerate()
            .map(|(new, old)| (old, new))
            .collect();
        let unshrink_table: Vec<NodeId> = strongly_connected_nodes.into_iter().collect();

        let mut fbas_shrunken = Fbas::new_generic_unconfigured(unshrink_table.len());
        for old_id in 0..fbas.nodes.len() {
            if let Some(&new_id) = shrink_map.get(&old_id) {
                fbas_shrunken.nodes[new_id] = Node::shrunken(&fbas.nodes[old_id], &shrink_map);
            }
        }
        (fbas_shrunken, unshrink_table)
    }
}
impl Node {
    fn shrunken(node: &Self, shrink_map: &HashMap<NodeId, NodeId>) -> Self {
        Node {
            public_key: node.public_key.clone(),
            quorum_set: QuorumSet::shrunken(&node.quorum_set, shrink_map),
        }
    }
}
impl QuorumSet {
    fn shrunken(quorum_set: &Self, shrink_map: &HashMap<NodeId, NodeId>) -> Self {
        let mut validators = vec![];
        for old_id in quorum_set.validators.iter() {
            if let Some(&new_id) = shrink_map.get(&old_id) {
                validators.push(new_id);
            }
        }
        validators.sort();

        let mut inner_quorum_sets = vec![];
        for inner_quorum_set in quorum_set.inner_quorum_sets.iter() {
            let shrunken_inner_quorum_set = QuorumSet::shrunken(inner_quorum_set, shrink_map);
            if shrunken_inner_quorum_set != QuorumSet::new() {
                inner_quorum_sets.push(shrunken_inner_quorum_set);
            }
        }
        inner_quorum_sets.sort();

        let threshold = quorum_set.threshold;

        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
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
            describe_with_histogram(analysis.minimal_quorums()),
            describe_with_histogram(&[bitset![0, 1], bitset![0, 10], bitset![1, 10]])
        );
        assert_eq!(
            describe_with_histogram(analysis.minimal_blocking_sets()),
            describe_with_histogram(&[bitset![0, 1], bitset![0, 10], bitset![1, 10]])
        );
        assert_eq!(
            describe_with_histogram(analysis.minimal_splitting_sets()),
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
    fn shrink_fbas_reduces_size() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let (fbas_shrunken, _) = Fbas::shrunken(&fbas);
        assert!(fbas_shrunken.number_of_nodes() < fbas.number_of_nodes());
    }

    #[test]
    fn shrink_quorum_set() {
        let qset = QuorumSet {
            threshold: 2,
            validators: vec![2, 3, 4],
            inner_quorum_sets: vec![],
        };
        let shrink_map: HashMap<NodeId, NodeId> = vec![(2, 0), (4, 1)].into_iter().collect();
        let expected = QuorumSet {
            threshold: 2,
            validators: vec![0, 1],
            inner_quorum_sets: vec![],
        };
        let actual = QuorumSet::shrunken(&qset, &shrink_map);
        assert_eq!(expected, actual);
    }

    #[test]
    fn shrink_unshrink_find_minimal_quorums() {
        let fbas = Fbas::from_json_file(Path::new("test_data/correct.json"));
        let (fbas_shrunken, unshrink_table) = Fbas::shrunken(&fbas);

        let expected = find_minimal_quorums(&fbas);
        let actual = unshrink(find_minimal_quorums(&fbas_shrunken), &unshrink_table);
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
