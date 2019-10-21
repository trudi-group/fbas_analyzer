use super::*;

use std::cmp;

/// Dummy Quorum Set Configurator.
///
/// Creates empty quorum sets.
#[derive(Default)]
pub struct DummyQsc;
impl QuorumSetConfigurator for DummyQsc {
    fn build_new(&self, _: &Fbas) -> QuorumSet {
        QuorumSet::new()
    }
}

/// Super simple Quorum Set Configurator priorizing FBAS liveness.
///
/// Creates threshold=1 quorum sets containing all nodes in the FBAS
///
/// ```
/// #[macro_use] extern crate fbas_analyzer;
/// use fbas_analyzer::{Fbas, Simulator};
/// use fbas_analyzer::quorum_set_configurators::SuperLiveQsc;
/// use fbas_analyzer::monitors::DummyMonitor;
/// use std::rc::Rc;
///
/// let mut simulator = Simulator::new(
///     Fbas::new(),
///     Rc::new(SuperLiveQsc),
///     Rc::new(DummyMonitor),
/// );
/// simulator.simulate_growth(3);
///
/// let fbas = simulator.finalize();
/// assert!(fbas.is_quorum(&bitset![0]));
/// assert!(fbas.is_quorum(&bitset![1]));
/// assert!(fbas.is_quorum(&bitset![2]));
/// assert!(!fbas.has_quorum_intersection());
/// ```
#[derive(Default)]
pub struct SuperLiveQsc;
impl QuorumSetConfigurator for SuperLiveQsc {
    /// Also includes the "next" node (most likely the one currently being created). This solves
    /// the bootstrapping problem that the first node otherwise doesn't have a valid quorum.
    fn build_new(&self, fbas: &Fbas) -> QuorumSet {
        let threshold = 1;
        let validators = (0..fbas.nodes.len()).collect();
        let inner_quorum_sets = vec![];
        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
    }
}

/// Super simple Quorum Set Configurator priorizing FBAS safety.
///
/// Creates threshold=n quorum sets containing all n nodes in the FBAS.
///
/// ```
/// #[macro_use] extern crate fbas_analyzer;
/// use fbas_analyzer::{Fbas, Simulator};
/// use fbas_analyzer::quorum_set_configurators::SuperSafeQsc;
/// use fbas_analyzer::monitors::DummyMonitor;
/// use std::rc::Rc;
///
/// let mut simulator = Simulator::new(
///     Fbas::new(),
///     Rc::new(SuperSafeQsc),
///     Rc::new(DummyMonitor),
/// );
/// simulator.simulate_growth(3);
///
/// let fbas = simulator.finalize();
/// assert!(fbas.is_quorum(&bitset![0, 1, 2]));
/// assert!(fbas.has_quorum_intersection());
/// ```
#[derive(Default)]
pub struct SuperSafeQsc;
impl QuorumSetConfigurator for SuperSafeQsc {
    fn build_new(&self, fbas: &Fbas) -> QuorumSet {
        let n = fbas.nodes.len();
        let threshold = n;
        let validators = (0..n).collect();
        let inner_quorum_sets = vec![];
        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
    }
}

pub struct SimpleRandomNoChangeQsc {
    k: usize,
    threshold: usize,
}
impl QuorumSetConfigurator for SimpleRandomNoChangeQsc {
    fn build_new(&self, fbas: &Fbas) -> QuorumSet {
        let k = cmp::min(self.k, fbas.nodes.len());

        let threshold = cmp::min(k, self.threshold);

        let node_ids: Vec<NodeId> = (0..fbas.nodes.len()).collect();
        let validators: Vec<NodeId> = node_ids
            .choose_multiple(&mut thread_rng(), k)
            .copied()
            .collect();
        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets: vec![],
        }
    }
    fn change_existing(&self, _: NodeId, _: &mut Fbas) -> ChangeEffect {
        NoChange
    }
}

#[cfg(test)]
mod tests {
    use super::monitors::*;
    use super::*;

    #[test]
    fn super_live_fbas_has_quorums() {
        let mut simulator =
            Simulator::new(Fbas::new(), Rc::new(SuperLiveQsc), Rc::new(DummyMonitor));
        simulator.simulate_growth(3);
        assert!(simulator.fbas.is_quorum(&bitset![0]));
        assert!(simulator.fbas.is_quorum(&bitset![1]));
        assert!(simulator.fbas.is_quorum(&bitset![2]));
        assert!(simulator.fbas.is_quorum(&bitset![0, 1, 2]));
    }

    #[test]
    fn super_safe_fbas_has_a_quorum() {
        let mut simulator =
            Simulator::new(Fbas::new(), Rc::new(SuperSafeQsc), Rc::new(DummyMonitor));
        simulator.simulate_growth(3);
        assert!(simulator.fbas.is_quorum(&bitset![0, 1, 2]));
    }

    #[test]
    fn super_live_fbas_has_no_quorum_intersection() {
        let mut simulator =
            Simulator::new(Fbas::new(), Rc::new(SuperLiveQsc), Rc::new(DummyMonitor));
        simulator.simulate_growth(3);
        assert!(!simulator.fbas.has_quorum_intersection());
    }

    #[test]
    fn super_safe_fbas_has_quorum_intersection() {
        let mut simulator =
            Simulator::new(Fbas::new(), Rc::new(SuperSafeQsc), Rc::new(DummyMonitor));
        simulator.simulate_growth(8);
        assert!(simulator.fbas.has_quorum_intersection());
    }
}
