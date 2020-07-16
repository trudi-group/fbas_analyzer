use super::*;

/// Super simple Quorum Set Configurator priorizing FBAS safety.
///
/// Creates threshold=n quorum sets containing all n nodes in the FBAS.
///
/// ```
/// use fbas_analyzer::{Fbas, Analysis, bitset};
/// use fbas_analyzer::simulation::{Simulator, qsc, monitors};
/// use std::rc::Rc;
///
/// let mut simulator = Simulator::new(
///     Fbas::new(),
///     Rc::new(qsc::SuperSafeQsc),
///     Rc::new(monitors::DummyMonitor),
/// );
/// simulator.simulate_growth(4);
///
/// let fbas = simulator.finalize();
/// assert!(fbas.is_quorum(&bitset![0, 1, 2, 3]));
/// assert!(!fbas.is_quorum(&bitset![0, 1, 2]));
/// assert!(Analysis::new(&fbas).has_quorum_intersection());
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(Analysis::new(&fbas).has_quorum_intersection());
    }
}
