use super::*;

/// Simple Quorum Set Configurator that builds an optimal global configuration
/// (if everybody plays along and there are no sybils attackers).
///
/// Builds quorum sets containing all n nodes in the FBAS, with thresholds chosen such that
/// a maximum of f nodes can fail, where (n-1) < (3f+1) <= n.
///
/// ```
/// use fbas_analyzer::{Fbas, Analysis, bitset};
/// use fbas_analyzer::simulation::{Simulator, qsc, monitors};
/// use std::rc::Rc;
///
/// let mut simulator = Simulator::new(
///     Fbas::new(),
///     Rc::new(qsc::IdealQsc),
///     Rc::new(monitors::DummyMonitor),
/// );
/// simulator.simulate_growth(4);
///
/// let fbas = simulator.finalize();
/// assert!(fbas.is_quorum(&bitset![0, 1, 2]));
/// assert!(!fbas.is_quorum(&bitset![0, 1]));
/// assert!(Analysis::new(&fbas).has_quorum_intersection());
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
        let threshold: usize = calculate_67p_threshold(n);
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
    fn ideal_qsc_makes_ideal_fbas() {
        let f = 1;
        let n = 3 * f + 1;
        let fbas = simulate!(IdealQsc::new(), n);

        let analysis = Analysis::new(&fbas);
        let actual = analysis.minimal_quorums().unwrap();
        let expected = bitsetvec![{0, 1, 2}, {0, 1, 3}, {0, 2, 3}, {1, 2, 3}];
        assert_eq!(expected, actual);
    }
}
