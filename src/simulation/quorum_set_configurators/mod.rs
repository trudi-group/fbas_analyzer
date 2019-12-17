use super::*;

use std::cmp;

mod random;
pub use random::*;
mod graph;
pub use graph::*;

/// Dummy Quorum Set Configurator.
///
/// Creates empty quorum sets.
#[derive(Default)]
pub struct DummyQsc;
impl QuorumSetConfigurator for DummyQsc {
    fn configure(&self, _: NodeId, _: &mut Fbas) -> ChangeEffect {
        NoChange
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
/// simulator.simulate_growth(4);
///
/// let fbas = simulator.finalize();
/// assert!(fbas.is_quorum(&bitset![0, 1, 2, 3]));
/// assert!(!fbas.is_quorum(&bitset![0, 1, 2]));
/// assert!(fbas.has_quorum_intersection());
/// ```
#[derive(Default)]
pub struct SuperSafeQsc;
impl QuorumSetConfigurator for SuperSafeQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let candidate = Self::build_new_configuration(fbas);
        let existing = &mut fbas.nodes[node_id].quorum_set;
        if candidate == *existing {
            NoChange
        } else {
            *existing = candidate;
            Change
        }
    }
}
impl SuperSafeQsc {
    pub fn new() -> Self {
        SuperSafeQsc {}
    }
    fn build_new_configuration(fbas: &Fbas) -> QuorumSet {
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

/// Simple Quorum Set Configurator that builds an optimal global configuration
/// (if everybody plays along and there are no sybils attackers).
///
/// Builds quorum sets containing all n nodes in the FBAS, with thresholds chosen such that
/// a maximum of f nodes can fail, where (n-1) < (3f+1) <= n.
///
/// ```
/// #[macro_use] extern crate fbas_analyzer;
/// use fbas_analyzer::{Fbas, Simulator};
/// use fbas_analyzer::quorum_set_configurators::IdealQsc;
/// use fbas_analyzer::monitors::DummyMonitor;
/// use std::rc::Rc;
///
/// let mut simulator = Simulator::new(
///     Fbas::new(),
///     Rc::new(IdealQsc),
///     Rc::new(DummyMonitor),
/// );
/// simulator.simulate_growth(4);
///
/// let fbas = simulator.finalize();
/// assert!(fbas.is_quorum(&bitset![0, 1, 2]));
/// assert!(!fbas.is_quorum(&bitset![0, 1]));
/// assert!(fbas.has_quorum_intersection());
/// ```
#[derive(Default)]
pub struct IdealQsc;
impl QuorumSetConfigurator for IdealQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let candidate = Self::build_new_configuration(fbas);
        let existing = &mut fbas.nodes[node_id].quorum_set;
        if candidate == *existing {
            NoChange
        } else {
            *existing = candidate;
            Change
        }
    }
}
impl IdealQsc {
    pub fn new() -> Self {
        IdealQsc {}
    }
    fn build_new_configuration(fbas: &Fbas) -> QuorumSet {
        let n = fbas.nodes.len();
        let threshold: usize = get_67p_threshold(n);
        let validators = (0..n).collect();
        let inner_quorum_sets = vec![];
        QuorumSet {
            threshold,
            validators,
            inner_quorum_sets,
        }
    }
}

/// t = ceil((2n+1)/3) => n >= 3f+1
fn get_67p_threshold(n: usize) -> usize {
    // reformed for more robustness against floating point errors
    n - ((n as f64 - 1.) / 3.).floor() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[macro_export]
    macro_rules! assert_is_67p_threshold {
        ($t:expr, $n:expr) => {
            assert!(3 * $t >= 2 * $n + 1, "Not a 67% threshold!")
        };
    }
    #[macro_export]
    macro_rules! assert_has_67p_threshold {
        ($qset:expr) => {
            assert_is_67p_threshold!(
                $qset.threshold,
                $qset.validators.len() + $qset.inner_quorum_sets.len()
            );
        };
    }
    #[macro_export]
    macro_rules! simulate {
        ($qsc:expr, $n:expr) => {{
            let mut simulator =
                Simulator::new(Fbas::new(), Rc::new($qsc), Rc::new(monitors::DummyMonitor));
            simulator.simulate_growth($n);
            simulator.finalize()
        }};
    }

    #[test]
    fn get_67p_threshold_test() {
        for n in 1..20 {
            assert_is_67p_threshold!(get_67p_threshold(n), n);
        }
    }

    #[test]
    fn super_safe_qsc_makes_a_quorum() {
        let fbas = simulate!(SuperSafeQsc::new(), 5);
        assert!(fbas.is_quorum(&bitset![0, 1, 2, 3, 4]));
    }

    #[test]
    fn super_safe_qsc_makes_no_small_quorum() {
        let fbas = simulate!(SuperSafeQsc::new(), 5);
        assert!(!fbas.is_quorum(&bitset![0, 1, 2, 3]));
    }

    #[test]
    fn super_safe_qsc_makes_quorum_intersection() {
        let fbas = simulate!(SuperSafeQsc::new(), 8);
        assert!(fbas.has_quorum_intersection());
    }

    #[test]
    fn ideal_qsc_makes_ideal_fbas() {
        let f = 1;
        let n = 3 * f + 1;
        let fbas = simulate!(IdealQsc::new(), n);

        let mut analysis = Analysis::new(&fbas);
        let actual = analysis.minimal_quorums();
        let expected = bitsetvec![{0, 1, 2}, {0, 1, 3}, {0, 2, 3}, {1, 2, 3}];
        assert_eq!(expected, actual);
    }
}