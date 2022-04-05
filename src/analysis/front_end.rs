use super::*;

use std::cell::RefCell;

/// Front end for many interesting FBAS analyses. Among other things, it does ID space shrinking
/// (which improves memory and performance when using bit sets) and caches the results of
/// long-running computations.
#[derive(Debug)]
pub struct Analysis {
    fbas_original: Fbas,
    fbas_shrunken: RefCell<Fbas>,
    shrink_manager: RefCell<ShrinkManager>,
    hqi_cache: RefCell<Option<bool>>,
    mq_shrunken_cache: RefCell<Option<Vec<NodeIdSet>>>,
    mbs_shrunken_cache: RefCell<Option<Vec<NodeIdSet>>>,
    mss_shrunken_cache: RefCell<Option<Vec<NodeIdSet>>>,
}
impl Analysis {
    /// Start a new `Analysis`
    pub fn new(fbas: &Fbas) -> Self {
        debug!(
            "Shrinking FBAS of size {} to set of satisfiable nodes (for performance)...",
            fbas.number_of_nodes()
        );
        let (fbas_shrunken, shrink_manager) = Fbas::shrunken(fbas, fbas.satisfiable_nodes());
        debug!(
            "Shrank to an FBAS of size {}.",
            fbas_shrunken.number_of_nodes()
        );
        Analysis {
            fbas_original: fbas.clone(),
            fbas_shrunken: RefCell::new(fbas_shrunken),
            shrink_manager: RefCell::new(shrink_manager),
            hqi_cache: RefCell::new(None),
            mq_shrunken_cache: RefCell::new(None),
            mbs_shrunken_cache: RefCell::new(None),
            mss_shrunken_cache: RefCell::new(None),
        }
    }
    /// Shrink the FBAS to its core nodes, i.e., to the union of all quorum-containing strongly
    /// connected components. Future splitting sets returned by this object will miss any splitting
    /// sets that do not consist entirely of core nodes and don't cause at least one pair of core
    /// nodes to end up in non-intersecting quorums.
    pub fn shrink_to_core_nodes(&mut self) {
        debug!("Shrinking FBAS to core nodes...",);
        let core_nodes_original = self.fbas_original.core_nodes();
        let (new_fbas_shrunken, new_shrink_manager) =
            Fbas::shrunken(&self.fbas_original, core_nodes_original);
        debug!(
            "Shrank to an FBAS of size {} (from size {}).",
            new_fbas_shrunken.number_of_nodes(),
            self.fbas_shrunken.borrow().number_of_nodes(),
        );
        debug!("Fixing previously cached values...");
        self.reshrink_cached_results(&new_shrink_manager);
        self.fbas_shrunken.replace(new_fbas_shrunken);
        self.shrink_manager.replace(new_shrink_manager);
    }
    /// Nodes in the analyzed FBAS - not filtered by relevance.
    pub fn all_nodes(&self) -> NodeIdSetResult {
        self.make_unshrunken_set_result(self.fbas_original.all_nodes())
    }
    /// Nodes in the analyzed FBAS that can be satisfied given their quorum sets and the nodes
    /// existing in the FBAS.
    pub fn satisfiable_nodes(&self) -> NodeIdSetResult {
        self.make_unshrunken_set_result(self.fbas_original.satisfiable_nodes())
    }
    /// Nodes in the analyzed FBAS that can never be satisfied given their quorum sets and the
    /// nodes existing in the FBAS.
    pub fn unsatisfiable_nodes(&self) -> NodeIdSetResult {
        self.make_unshrunken_set_result(self.fbas_original.unsatisfiable_nodes())
    }
    /// Regular quorum intersection check via finding all minimal quorums (algorithm inspired by
    /// [Lachowski 2019](https://arxiv.org/abs/1902.06493)).
    pub fn has_quorum_intersection(&self) -> bool {
        self.has_quorum_intersection_from_shrunken()
    }
    /// Quorum intersection check that works without enumerating all minimal quorums.
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
    /// For each minimal splitting set, returns two or more quorums that it's splitting, i.e.,
    /// quorums that lack quorum intersection after the splitting sets are deleted from the FBAS.
    pub fn minimal_splitting_sets_with_affected_quorums(
        &self,
    ) -> Vec<(NodeIdSetResult, NodeIdSetVecResult)> {
        self.minimal_splitting_sets_with_affected_quorums_shrunken()
            .into_iter()
            .map(|(splitting_set, split_quorums)| {
                let key = self.make_shrunken_set_result(splitting_set);
                let result = self.make_shrunken_set_vec_result(split_quorums);
                (key, result)
            })
            .collect()
    }
    /// Top tier - the set of nodes exclusively relevant when determining minimal quorums and
    /// minimal blocking sets.
    pub fn top_tier(&self) -> NodeIdSetResult {
        self.make_shrunken_set_result(self.top_tier_shrunken())
    }
    /// If the top tier is symmetric, i.e., each two top-tier nodes have the same quorum set,
    /// return the top tier's common quorum set. Else return `None`.
    pub fn symmetric_top_tier(&self) -> Option<QuorumSet> {
        find_symmetric_top_tier(&self.fbas_original)
    }
    /// Symmetric clusters - sets of nodes in which each two nodes have the same quorum set.
    /// Here, each found symmetric cluster is represented by its common quorum set.
    pub fn symmetric_clusters(&self) -> Vec<QuorumSet> {
        find_symmetric_clusters(&self.fbas_original)
    }

    #[rustfmt::skip]
    fn reshrink_cached_results(&mut self, new_shrink_manager: &ShrinkManager) {
        let mq_shrunken_cache = self.mq_shrunken_cache.borrow().clone().map(|mq_shrunken| {
            new_shrink_manager.reshrink_sets(&mq_shrunken, &self.shrink_manager.borrow())
        });
        let mbs_shrunken_cache = self.mbs_shrunken_cache.borrow().clone().map(|mbs_shrunken| {
            new_shrink_manager.reshrink_sets(&mbs_shrunken, &self.shrink_manager.borrow())
        });
        self.mq_shrunken_cache.replace(mq_shrunken_cache);
        self.mbs_shrunken_cache.replace(mbs_shrunken_cache);
        self.mss_shrunken_cache.replace(None);
    }
    fn has_quorum_intersection_from_shrunken(&self) -> bool {
        self.cached_computation(
            &self.hqi_cache,
            || {
                let quorums = self.minimal_quorums_shrunken();
                !quorums.is_empty() && all_intersect(&quorums)
            },
            "has quorum intersection",
        )
    }
    fn minimal_quorums_shrunken(&self) -> Vec<NodeIdSet> {
        self.cached_computation_from_fbas_shrunken(
            &self.mq_shrunken_cache,
            find_minimal_quorums,
            "minimal quorums",
        )
    }
    fn minimal_blocking_sets_shrunken(&self) -> Vec<NodeIdSet> {
        self.cached_computation_from_fbas_shrunken(
            &self.mbs_shrunken_cache,
            find_minimal_blocking_sets,
            "minimal blocking sets",
        )
    }
    fn minimal_splitting_sets_shrunken(&self) -> Vec<NodeIdSet> {
        self.cached_computation_from_fbas_shrunken(
            &self.mss_shrunken_cache,
            find_minimal_splitting_sets,
            "minimal splitting sets",
        )
    }
    fn minimal_splitting_sets_with_affected_quorums_shrunken(
        &self,
    ) -> Vec<(NodeIdSet, Vec<NodeIdSet>)> {
        let minimal_splitting_sets = self.minimal_splitting_sets_shrunken();
        minimal_splitting_sets
            .into_iter()
            .map(|splitting_set| {
                let mut fbas = self.fbas_shrunken.borrow().clone();
                fbas.assume_split_faulty(&splitting_set);
                let split_quorums = find_nonintersecting_quorums(&fbas).unwrap();
                (splitting_set, split_quorums)
            })
            .collect()
    }
    fn top_tier_shrunken(&self) -> NodeIdSet {
        // The top tier is defined as either the union of all minimal quorums but can also be found
        // by forming the union of all minimal blocking sets.
        if self.mq_shrunken_cache.borrow().is_some() || self.mbs_shrunken_cache.borrow().is_none() {
            involved_nodes(&self.minimal_quorums_shrunken())
        } else {
            involved_nodes(&self.minimal_blocking_sets_shrunken())
        }
    }

    fn cached_computation_from_fbas_shrunken<R, F>(
        &self,
        cache: &RefCell<Option<R>>,
        computation: F,
        log_name: &str,
    ) -> R
    where
        R: Clone,
        F: Fn(&Fbas) -> R,
    {
        self.cached_computation(
            cache,
            || computation(&self.fbas_shrunken.borrow()),
            log_name,
        )
    }
    fn cached_computation<R, F>(
        &self,
        cache: &RefCell<Option<R>>,
        computation: F,
        log_name: &str,
    ) -> R
    where
        R: Clone,
        F: Fn() -> R,
    {
        let cache_is_empty = cache.borrow().is_none();
        if cache_is_empty {
            info!("Computing {}...", log_name);
            let result = computation();
            cache.replace(Some(result));
        } else {
            info!("Using cached {}.", log_name);
        }
        cache.borrow().clone().unwrap()
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
