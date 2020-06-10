use super::*;

use std::cell::RefCell;

/// Front end for the most interesting FBAS analyses.
/// Among other things, it does ID space shrinking (which improves memory and performance when
/// using bit sets) and caches the results of long-running computations.
pub struct Analysis<'a> {
    fbas_original: &'a Fbas,
    organizations_original: Option<&'a Organizations<'a>>,
    fbas_shrunken: RefCell<Fbas>,
    shrink_manager: RefCell<ShrinkManager>,
    hqi_cache: RefCell<Option<bool>>,
    mq_shrunken_cache: RefCell<Option<Vec<NodeIdSet>>>,
    mbs_shrunken_cache: RefCell<Option<Vec<NodeIdSet>>>,
    mss_shrunken_cache: RefCell<Option<Vec<NodeIdSet>>>,
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
        let (fbas_shrunken, shrink_manager) = Fbas::shrunken(fbas, strongly_connected_nodes);
        debug!(
            "Shrank to an FBAS of size {}.",
            fbas_shrunken.number_of_nodes()
        );
        Analysis {
            fbas_original: fbas,
            organizations_original: organizations,
            fbas_shrunken: RefCell::new(fbas_shrunken),
            shrink_manager: RefCell::new(shrink_manager),
            hqi_cache: RefCell::new(None),
            mq_shrunken_cache: RefCell::new(None),
            mbs_shrunken_cache: RefCell::new(None),
            mss_shrunken_cache: RefCell::new(None),
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
            self.maybe_merge_unshrunken_node_set_by_organization(self.fbas_original.all_nodes()),
        )
    }
    /// Nodes in the analyzed FBAS that can be satisfied given their quorum sets and the nodes
    /// existing in the FBAS.
    pub fn satisfiable_nodes(&self) -> NodeIdSetResult {
        self.make_unshrunken_set_result(self.maybe_merge_unshrunken_node_set_by_organization(
            self.fbas_original.satisfiable_nodes(),
        ))
    }
    /// Nodes in the analyzed FBAS that can never be satisfied given their quorum sets and the
    /// nodes existing in the FBAS.
    pub fn unsatisfiable_nodes(&self) -> NodeIdSetResult {
        self.make_unshrunken_set_result(self.maybe_merge_unshrunken_node_set_by_organization(
            self.fbas_original.unsatisfiable_nodes(),
        ))
    }
    /// Regular quorum intersection check via finding all minimal quorums.
    /// Algorithm inspired by [Lachowski 2019](https://arxiv.org/abs/1902.06493)).
    pub fn has_quorum_intersection(&self) -> bool {
        self.cached_computation_from_minimal_quorums_shrunken(
            &self.hqi_cache,
            |quorums| !quorums.is_empty() && all_intersect(quorums),
            "has quorum intersection",
        )
    }
    /// Quorum intersection check that works faster for FBASs that do not enjoy quorum
    /// intersection.
    pub fn has_quorum_intersection_via_alternative_check(
        &self,
    ) -> (bool, Option<NodeIdSetVecResult>) {
        if let Some(quorums) = find_nonintersecting_quorums(&self.fbas_shrunken.borrow()) {
            assert!(quorums[0].is_disjoint(&quorums[1]));
            (
                false,
                Some(self.make_shrunken_set_vec_result(quorums.to_vec())),
            )
        } else {
            (true, None)
        }
    }
    /// Minimal quorums - no proper subset of any of these node sets is a quorum.
    pub fn minimal_quorums(&self) -> NodeIdSetVecResult {
        self.make_shrunken_set_vec_result(self.minimal_quorums_shrunken())
    }
    /// Minimal blocking sets - minimal indispensable sets for global liveness.
    pub fn minimal_blocking_sets(&self) -> NodeIdSetVecResult {
        self.make_shrunken_set_vec_result(self.minimal_blocking_sets_shrunken())
    }
    /// Minimal splitting sets - minimal indispensable sets for safety.
    pub fn minimal_splitting_sets(&self) -> NodeIdSetVecResult {
        self.make_shrunken_set_vec_result(self.minimal_splitting_sets_shrunken())
    }
    /// Top tier - the set of nodes exclusively relevant when determining minimal blocking sets and
    /// minimal splitting sets.
    pub fn top_tier(&self) -> NodeIdSetResult {
        let top_tier_shrunken = involved_nodes(&self.minimal_quorums_shrunken());
        self.make_shrunken_set_result(top_tier_shrunken)
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

    fn minimal_quorums_shrunken(&self) -> Vec<NodeIdSet> {
        if self.mq_shrunken_cache.borrow().is_none() {
            self.find_minimal_quorums_and_update_shrinking_rules();
        } else {
            info!("Using cached minimal quorums.");
        }
        self.mq_shrunken_cache.borrow().clone().unwrap()
    }
    fn minimal_blocking_sets_shrunken(&self) -> Vec<NodeIdSet> {
        self.cached_computation_from_minimal_quorums_shrunken(
            &self.mbs_shrunken_cache,
            find_minimal_blocking_sets,
            "minimal blocking sets",
        )
    }
    fn minimal_splitting_sets_shrunken(&self) -> Vec<NodeIdSet> {
        self.cached_computation_from_minimal_quorums_shrunken(
            &self.mss_shrunken_cache,
            find_minimal_splitting_sets,
            "minimal splitting sets",
        )
    }

    fn find_minimal_quorums_and_update_shrinking_rules(&self) {
        self.find_merge_and_cache_minimal_quorums();
        self.shrink_id_space_to_top_tier();

        if log_enabled!(Warn) {
            if self.has_quorum_intersection() {
                debug!("FBAS enjoys quorum intersection.");
            } else {
                warn!("FBAS doesn't enjoy quorum intersection!");
            }
        }
    }
    fn find_merge_and_cache_minimal_quorums(&self) {
        info!("Computing minimal quorums...");
        let minimal_quorums_shrunken = find_minimal_quorums(&self.fbas_shrunken.borrow());

        if self.organizations_original.is_some() {
            debug!("Merging nodes by organization...");
            info!(
                "{} top tier nodes before merging by organization.",
                involved_nodes(&minimal_quorums_shrunken).len()
            );
        }
        let minimal_quorums_shrunken =
            self.maybe_merge_shrunken_minimal_node_sets_by_organization(minimal_quorums_shrunken);
        if self.organizations_original.is_some() {
            info!(
                "{} top tier nodes after merging by organization.",
                involved_nodes(&minimal_quorums_shrunken).len()
            );
        }
        self.mq_shrunken_cache
            .replace(Some(minimal_quorums_shrunken));
    }
    fn shrink_id_space_to_top_tier(&self) {
        debug!("Shrinking FBAS again, to top tier (for performance)...",);
        let top_tier_original = self.top_tier().unwrap();
        let (new_fbas_shrunken, new_shrink_manager) =
            Fbas::shrunken(&self.fbas_original, top_tier_original);
        debug!(
            "Shrank to an FBAS of size {} (from size {}).",
            new_fbas_shrunken.number_of_nodes(),
            self.fbas_shrunken.borrow().number_of_nodes(),
        );
        let minimal_quorums_shrunken = new_shrink_manager.reshrink_sets(
            &self.minimal_quorums_shrunken(),
            &self.shrink_manager.borrow(),
        );

        self.fbas_shrunken.replace(new_fbas_shrunken);
        self.shrink_manager.replace(new_shrink_manager);
        self.mq_shrunken_cache
            .replace(Some(minimal_quorums_shrunken));
    }

    fn cached_computation_from_minimal_quorums_shrunken<R, F>(
        &self,
        cache: &RefCell<Option<R>>,
        computation: F,
        log_name: &str,
    ) -> R
    where
        R: Clone,
        F: Fn(&[NodeIdSet]) -> R,
    {
        let cache_is_empty = cache.borrow().is_none();
        if cache_is_empty {
            info!("Computing {}...", log_name);
            cache.replace(Some(computation(&self.minimal_quorums_shrunken())));
        } else {
            info!("Using cached {}.", log_name);
        }
        cache.borrow().clone().unwrap()
    }

    fn maybe_merge_unshrunken_node_set_by_organization(&self, node_set: NodeIdSet) -> NodeIdSet {
        if let Some(ref orgs) = self.organizations_original {
            orgs.merge_node_set(node_set)
        } else {
            node_set
        }
    }
    fn maybe_merge_shrunken_minimal_node_sets_by_organization(
        &self,
        node_sets: Vec<NodeIdSet>,
    ) -> Vec<NodeIdSet> {
        if let Some(ref orgs) = self.organizations_original {
            let fbas_shrunken = &self.fbas_shrunken.borrow();
            let orgs_shrunken =
                Organizations::shrunken(orgs, &self.shrink_manager.borrow(), &fbas_shrunken);
            orgs_shrunken.merge_minimal_node_sets(node_sets)
        } else {
            node_sets
        }
    }

    fn make_unshrunken_set_result(&self, payload: NodeIdSet) -> NodeIdSetResult {
        NodeIdSetResult::new(payload, None)
    }
    fn make_shrunken_set_result(&self, payload: NodeIdSet) -> NodeIdSetResult {
        NodeIdSetResult::new(payload, Some(&self.shrink_manager.borrow()))
    }
    fn make_shrunken_set_vec_result(&self, payload: Vec<NodeIdSet>) -> NodeIdSetVecResult {
        NodeIdSetVecResult::new(payload, Some(&self.shrink_manager.borrow()))
    }
}
