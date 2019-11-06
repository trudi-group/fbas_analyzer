use super::*;
use log::log_enabled;
use log::Level::Warn;

mod find_blocking_sets;
mod find_intersections;
mod find_quorums;

pub use find_blocking_sets::{
    find_minimal_blocking_sets, find_optionally_smallest_minimal_blocking_sets,
    find_smallest_minimal_blocking_sets,
};
pub use find_intersections::{
    find_minimal_intersections, find_optionally_smallest_minimal_intersections,
    find_smallest_minimal_intersections,
};
pub use find_quorums::{find_minimal_quorums, find_unsatisfiable_nodes};

/// Most methods require &mut because they cache intermediate results.
pub struct Analysis<'a> {
    fbas: &'a Fbas,
    organizations: Option<&'a Organizations<'a>>,
    epsilon: Option<usize>,
    minimal_quorums: Option<Vec<NodeIdSet>>,
    minimal_blocking_sets: Option<Vec<NodeIdSet>>,
    minimal_intersections: Option<Vec<NodeIdSet>>,
}
impl<'a> Analysis<'a> {
    pub fn new(fbas: &'a Fbas) -> Self {
        Analysis {
            fbas,
            organizations: None,
            epsilon: None,
            minimal_quorums: None,
            minimal_blocking_sets: None,
            minimal_intersections: None,
        }
    }
    pub fn new_with_options(
        fbas: &'a Fbas,
        organizations: Option<&'a Organizations<'a>>,
        epsilon: Option<usize>,
    ) -> Self {
        Analysis {
            fbas,
            organizations,
            epsilon,
            minimal_quorums: None,
            minimal_blocking_sets: None,
            minimal_intersections: None,
        }
    }
    pub fn has_quorum_intersection(&mut self) -> bool {
        info!("Checking for intersection of all minimal quorums...");
        !self.minimal_quorums().is_empty() && all_interesect(self.minimal_quorums())
    }
    pub fn unsatisfiable_nodes(&self) -> Vec<NodeId> {
        let all_nodes: NodeIdSet = (0..self.fbas.nodes.len()).collect();
        let (_, unsatisfiable) = find_unsatisfiable_nodes(&all_nodes, self.fbas);
        unsatisfiable.into_iter().collect()
    }
    pub fn minimal_quorums(&mut self) -> &[NodeIdSet] {
        if self.minimal_quorums.is_none() {
            warn!("Computing minimal quorums...");
            self.minimal_quorums = Some(self.maybe_collapse(find_minimal_quorums(&self.fbas)));
            if log_enabled!(Warn) {
                if self.has_quorum_intersection() {
                    debug!("FBAS enjoys quorum intersection.");
                } else {
                    warn!("FBAS doesn't enjoy quorum intersection!");
                }
            }
        } else {
            info!("Using cached minimal quorums.");
        }
        self.minimal_quorums.as_ref().unwrap()
    }
    pub fn minimal_blocking_sets(&mut self) -> &[NodeIdSet] {
        if self.minimal_blocking_sets.is_none() {
            warn!("Computing minimal blocking sets...");
            let o_epsilon = self.epsilon; // for the borrow checker
            self.minimal_blocking_sets = Some(find_optionally_smallest_minimal_blocking_sets(
                self.minimal_quorums(),
                o_epsilon,
            ));
        } else {
            info!("Using cached minimal blocking sets.");
        }
        self.minimal_blocking_sets.as_ref().unwrap()
    }
    pub fn minimal_intersections(&mut self) -> &[NodeIdSet] {
        if self.minimal_intersections.is_none() {
            warn!("Computing minimal intersections...");
            let o_epsilon = self.epsilon; // for the borrow checker
            self.minimal_intersections = Some(find_optionally_smallest_minimal_intersections(
                self.minimal_quorums(),
                o_epsilon,
            ));
        } else {
            info!("Using cached minimal intersections.");
        }
        self.minimal_intersections.as_ref().unwrap()
    }
    pub fn involved_nodes(&mut self) -> Vec<NodeId> {
        involved_nodes(self.minimal_quorums())
    }
    fn maybe_collapse(&self, node_sets: Vec<NodeIdSet>) -> Vec<NodeIdSet> {
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
}

/// Returns (number_of_sets, min_set_size, max_set_size, mean_set_size, number_of_distinct_nodes)
pub fn describe(node_sets: &[NodeIdSet]) -> (usize, usize, usize, f64, usize) {
    let min = node_sets.iter().map(|s| s.len()).min().unwrap_or(0);
    let max = node_sets.iter().map(|s| s.len()).max().unwrap_or(0);
    let mean = if node_sets.is_empty() {
        0.0
    } else {
        node_sets.iter().map(|s| s.len()).sum::<usize>() as f64 / (node_sets.len() as f64)
    };
    let involved_nodes = involved_nodes(node_sets);
    (node_sets.len(), min, max, mean, involved_nodes.len())
}

pub fn all_interesect(node_sets: &[NodeIdSet]) -> bool {
    node_sets
        .iter()
        .enumerate()
        .all(|(i, x)| node_sets.iter().skip(i + 1).all(|y| !x.is_disjoint(y)))
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
    let mut minimal_node_sets: Vec<NodeIdSet> = vec![];

    debug!("Sorting node sets by length...");
    node_sets.sort_by_cached_key(|x| x.len());
    debug!("Sorting done.");

    debug!("Removing duplicates...");
    let len_before = node_sets.len();
    node_sets.dedup();
    debug!("Done; removed {} duplicates.", len_before - node_sets.len());

    debug!("Filtering non-minimal node sets...");
    for node_set in node_sets.into_iter() {
        if minimal_node_sets.iter().all(|x| !x.is_subset(&node_set)) {
            minimal_node_sets.push(node_set);
        }
    }
    debug!("Filtering done.");
    minimal_node_sets
}

/// Keep only the smallest node sets and node sets with up to `epsilon` more nodes.
/// Helpful for avoiding lengthy computations.
pub fn reduce_to_smallest(mut node_sets: Vec<NodeIdSet>, epsilon: usize) -> Vec<NodeIdSet> {
    let min = node_sets.iter().map(|x| x.len()).min().unwrap_or(0);
    node_sets = node_sets
        .iter()
        .filter(|x| x.len() <= min + epsilon)
        .cloned()
        .collect();
    node_sets
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
    /// Comfort function; we recommend using `Analysis` directly
    pub fn has_quorum_intersection(&self) -> bool {
        Analysis::new(&self).has_quorum_intersection()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn node_set_interesections() {
        assert!(all_interesect(&vec![
            bitset![0, 1],
            bitset![0, 2],
            bitset![1, 2]
        ]));
        assert!(!all_interesect(&vec![bitset![0], bitset![1, 2]]));
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
            analysis.minimal_quorums(),
            &[bitset![0, 1], bitset![0, 10], bitset![1, 10]]
        );
        assert_eq!(
            analysis.minimal_blocking_sets(),
            &[bitset![0, 1], bitset![0, 10], bitset![1, 10]]
        );
        assert_eq!(
            analysis.minimal_intersections(),
            &[bitset![0], bitset![1], bitset![10]]
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
        let mut analysis = Analysis::new_with_options(&fbas, Some(&organizations), None);

        assert!(analysis.has_quorum_intersection());
        assert_eq!(analysis.minimal_quorums().len(), 1);
        assert_eq!(analysis.minimal_blocking_sets().len(), 1);
        assert_eq!(analysis.minimal_intersections().len(), 0);
    }

    #[test]
    fn describe_node_sets() {
        let node_sets = vec![
            bitset![0, 1],
            bitset![2, 3],
            bitset![4, 5, 6, 7],
            bitset![1, 4],
        ];
        let actual = describe(&node_sets);
        let expected = (4, 2, 4, 2.5, 8);
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

    #[test]
    fn reduce_to_smallest_reduces_to_smallest_and_not_more() {
        let node_sets = vec![
            bitset![0],
            bitset![1, 2, 3, 4, 5, 9],
            bitset![0, 2],
            bitset![1, 2, 5],
            bitset![1, 2, 7, 9],
            bitset![1, 2],
        ];

        let actual = reduce_to_smallest(node_sets, 1);
        let expected = vec![bitset![0], bitset![0, 2], bitset![1, 2]];
        assert_eq!(expected, actual);
    }
}
