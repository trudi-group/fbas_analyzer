use super::*;

use std::cell::RefCell;

/// Front end for the most interesting FBAS analyses.
/// Among other things, it does ID space shrinking (which improves memory and performance when
/// using bit sets) and caches the results of long-running computations.
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
            fbas_original: fbas.clone(),
            fbas_shrunken: RefCell::new(fbas_shrunken),
            shrink_manager: RefCell::new(shrink_manager),
            hqi_cache: RefCell::new(None),
            mq_shrunken_cache: RefCell::new(None),
            mbs_shrunken_cache: RefCell::new(None),
            mss_shrunken_cache: RefCell::new(None),
        }
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
    /// Regular quorum intersection check via finding all minimal quorums.
    /// Algorithm inspired by [Lachowski 2019](https://arxiv.org/abs/1902.06493)).
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
    /// Top tier - the set of nodes exclusively relevant when determining minimal blocking sets and
    /// minimal splitting sets.
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

    fn has_quorum_intersection_from_shrunken(&self) -> bool {
        self.cached_computation(
            &self.hqi_cache,
            || {
                let quorums = self.minimal_quorums_shrunken();
                !quorums.is_empty() && all_intersect(&quorums)
            },
            "has quorum intersection",
            false,
        )
    }
    fn minimal_quorums_shrunken(&self) -> Vec<NodeIdSet> {
        self.cached_computation_from_fbas_shrunken(
            &self.mq_shrunken_cache,
            find_minimal_quorums,
            "minimal quorums",
            true,
        )
    }
    fn minimal_blocking_sets_shrunken(&self) -> Vec<NodeIdSet> {
        self.cached_computation_from_fbas_shrunken(
            &self.mbs_shrunken_cache,
            find_minimal_blocking_sets,
            "minimal blocking sets",
            true,
        )
    }
    fn minimal_splitting_sets_shrunken(&self) -> Vec<NodeIdSet> {
        let minimal_quorums = self.minimal_quorums_shrunken();
        self.cached_computation_from_fbas_shrunken(
            &self.mss_shrunken_cache,
            |fbas| {
                if self.has_quorum_intersection() {
                    find_minimal_splitting_sets(fbas, &minimal_quorums)
                } else {
                    vec![bitset![]]
                }
            },
            "minimal splitting sets",
            false,
        )
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
        result_defines_top_tier: bool,
    ) -> R
    where
        R: Clone,
        F: Fn(&Fbas) -> R,
    {
        self.cached_computation(
            cache,
            || computation(&self.fbas_shrunken.borrow()),
            log_name,
            result_defines_top_tier,
        )
    }
    fn cached_computation<R, F>(
        &self,
        cache: &RefCell<Option<R>>,
        computation: F,
        log_name: &str,
        result_defines_top_tier: bool,
    ) -> R
    where
        R: Clone,
        F: Fn() -> R,
    {
        let cache_is_empty = cache.borrow().is_none();
        if cache_is_empty {
            let should_shrink = if result_defines_top_tier {
                // top tier not defined yet
                self.mq_shrunken_cache.borrow().is_none()
                    && self.mbs_shrunken_cache.borrow().is_none()
            } else {
                false
            };

            info!("Computing {}...", log_name);
            let result = computation();
            cache.replace(Some(result));
            if should_shrink {
                self.shrink_id_space_to_top_tier();
            }
        } else {
            info!("Using cached {}.", log_name);
        }
        cache.borrow().clone().unwrap()
    }

    #[rustfmt::skip]
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
        let mq_shrunken_cache = self.mq_shrunken_cache.borrow().clone().map(|mq_shrunken| {
            new_shrink_manager.reshrink_sets(&mq_shrunken, &self.shrink_manager.borrow())
        });
        let mbs_shrunken_cache = self.mbs_shrunken_cache.borrow().clone().map(|mbs_shrunken| {
            new_shrink_manager.reshrink_sets(&mbs_shrunken, &self.shrink_manager.borrow())
        });
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
