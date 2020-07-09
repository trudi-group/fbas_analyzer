use super::*;

use std::cell::RefCell;

/// Front end for the most interesting FBAS analyses.
/// Among other things, it does ID space shrinking (which improves memory and performance when
/// using bit sets) and caches the results of long-running computations.
#[derive(Debug)]
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
        let relevant_nodes = fbas.relevant_nodes();
        let (fbas_shrunken, shrink_manager) = Fbas::shrunken(fbas, relevant_nodes);
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
        self.cached_computation_from_maybe_merged_minimal_quorums_shrunken(
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
        self.make_shrunken_set_vec_result(
            self.maybe_merge_shrunken_minimal_node_sets_by_organization(
                self.minimal_quorums_shrunken(),
            ),
        )
    }
    /// Minimal blocking sets - minimal indispensable sets for global liveness.
    pub fn minimal_blocking_sets(&self) -> NodeIdSetVecResult {
        self.make_shrunken_set_vec_result(
            self.maybe_merge_shrunken_minimal_node_sets_by_organization(
                self.minimal_blocking_sets_shrunken(),
            ),
        )
    }
    /// Minimal splitting sets - minimal indispensable sets for safety.
    pub fn minimal_splitting_sets(&self) -> NodeIdSetVecResult {
        self.make_shrunken_set_vec_result(self.maybe_merged_minimal_splitting_sets_shrunken())
    }
    /// Top tier - the set of nodes exclusively relevant when determining minimal blocking sets and
    /// minimal splitting sets.
    pub fn top_tier(&self) -> NodeIdSetResult {
        self.make_shrunken_set_result(
            self.maybe_merge_shrunken_node_set_by_organization(self.top_tier_shrunken()),
        )
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
        self.cached_top_tier_defining_computation_from_fbas_shrunken(
            &self.mq_shrunken_cache,
            find_minimal_quorums,
            "minimal quorums",
        )
    }
    fn minimal_blocking_sets_shrunken(&self) -> Vec<NodeIdSet> {
        self.cached_top_tier_defining_computation_from_fbas_shrunken(
            &self.mbs_shrunken_cache,
            find_minimal_blocking_sets,
            "minimal blocking sets",
        )
    }
    fn maybe_merged_minimal_splitting_sets_shrunken(&self) -> Vec<NodeIdSet> {
        self.cached_computation_from_maybe_merged_minimal_quorums_shrunken(
            &self.mss_shrunken_cache,
            find_minimal_splitting_sets,
            "minimal splitting sets",
        )
    }
    fn top_tier_shrunken(&self) -> NodeIdSet {
        if self.mq_shrunken_cache.borrow().is_some() || self.mbs_shrunken_cache.borrow().is_none() {
            involved_nodes(&self.minimal_quorums_shrunken())
        } else {
            involved_nodes(&self.minimal_blocking_sets_shrunken())
        }
    }

    fn cached_top_tier_defining_computation_from_fbas_shrunken<F>(
        &self,
        cache: &RefCell<Option<Vec<NodeIdSet>>>,
        computation: F,
        log_name: &str,
    ) -> Vec<NodeIdSet>
    where
        F: Fn(&Fbas) -> Vec<NodeIdSet>,
    {
        let cache_is_empty = cache.borrow().is_none();
        if cache_is_empty {
            let top_tier_known = self.mq_shrunken_cache.borrow().is_some()
                || self.mbs_shrunken_cache.borrow().is_some();
            let should_shrink = !top_tier_known;

            info!("Computing {}...", log_name);
            let result = computation(&self.fbas_shrunken.borrow());
            cache.replace(Some(result));
            if should_shrink {
                self.shrink_id_space_to_top_tier();
            }
        } else {
            info!("Using cached {}.", log_name);
        }
        cache.borrow().clone().unwrap()
    }

    fn cached_computation_from_maybe_merged_minimal_quorums_shrunken<R, F>(
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
            let minimal_quorums_shrunken = self
                .maybe_merge_shrunken_minimal_node_sets_by_organization(
                    self.minimal_quorums_shrunken(),
                );
            let result = computation(&minimal_quorums_shrunken);
            cache.replace(Some(result));
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
    fn maybe_merge_shrunken_node_set_by_organization(&self, node_set: NodeIdSet) -> NodeIdSet {
        if let Some(ref orgs) = self.organizations_original {
            let fbas_shrunken = &self.fbas_shrunken.borrow();
            let orgs_shrunken =
                Organizations::shrunken(orgs, &self.shrink_manager.borrow(), &fbas_shrunken);
            orgs_shrunken.merge_node_set(node_set)
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

    fn shrink_id_space_to_top_tier(&self) {
        debug!("Shrinking FBAS again, to top tier (for performance)...",);
        let top_tier_original = self
            .shrink_manager
            .borrow()
            .unshrink_set(&self.top_tier_shrunken());
        let (new_fbas_shrunken, new_shrink_manager) =
            Fbas::shrunken(&self.fbas_original, top_tier_original);
        debug!(
            "Shrank to an FBAS of size {} (from size {}).",
            new_fbas_shrunken.number_of_nodes(),
            self.fbas_shrunken.borrow().number_of_nodes(),
        );

        debug!("Fixing previously cached values...");
        assert!(
            self.mq_shrunken_cache.borrow().is_none() || self.mbs_shrunken_cache.borrow().is_none()
        );
        let mq_shrunken_cache = if let Some(mq_shrunken) = self.mq_shrunken_cache.borrow().clone() {
            Some(new_shrink_manager.reshrink_sets(&mq_shrunken, &self.shrink_manager.borrow()))
        } else {
            None
        };
        let mbs_shrunken_cache =
            if let Some(mbs_shrunken) = self.mbs_shrunken_cache.borrow().clone() {
                Some(new_shrink_manager.reshrink_sets(&mbs_shrunken, &self.shrink_manager.borrow()))
            } else {
                None
            };
        self.fbas_shrunken.replace(new_fbas_shrunken);
        self.shrink_manager.replace(new_shrink_manager);
        self.mq_shrunken_cache.replace(mq_shrunken_cache);
        self.mbs_shrunken_cache.replace(mbs_shrunken_cache);
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
