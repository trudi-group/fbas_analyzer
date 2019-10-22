use super::*;

use std::cmp;

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
/// simulator.simulate_growth(3);
///
/// let fbas = simulator.finalize();
/// assert!(fbas.is_quorum(&bitset![0, 1, 2]));
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

pub struct SimpleRandomNoChangeQsc {
    quorum_set_size: usize,
    threshold: usize,
}
impl SimpleRandomNoChangeQsc {
    pub fn new(quorum_set_size: usize, threshold: usize) -> Self {
        SimpleRandomNoChangeQsc {
            quorum_set_size,
            threshold,
        }
    }
}
impl QuorumSetConfigurator for SimpleRandomNoChangeQsc {
    fn configure(&self, node_id: NodeId, fbas: &mut Fbas) -> ChangeEffect {
        let n = fbas.nodes.len();
        let quorum_set = &mut fbas.nodes[node_id].quorum_set;

        if *quorum_set == Default::default() {
            // an unconfigured quorum set

            let quorum_set_size = cmp::min(self.quorum_set_size, n);

            let threshold = cmp::min(quorum_set_size, self.threshold);

            let node_ids: Vec<NodeId> = (0..n).collect();
            let validators: Vec<NodeId> = node_ids
                .choose_multiple(&mut thread_rng(), quorum_set_size)
                .copied()
                .collect();
            *quorum_set = QuorumSet {
                threshold,
                validators,
                inner_quorum_sets: vec![],
            };
            Change
        } else {
            NoChange
        }
    }
}

#[cfg(test)]
mod tests {
    use super::monitors::*;
    use super::*;

    #[test]
    fn super_safe_fbas_has_a_quorum() {
        let mut simulator =
            Simulator::new(Fbas::new(), Rc::new(SuperSafeQsc), Rc::new(DummyMonitor));
        simulator.simulate_growth(3);
        assert!(simulator.fbas.is_quorum(&bitset![0, 1, 2]));
    }

    #[test]
    fn super_safe_fbas_has_quorum_intersection() {
        let mut simulator =
            Simulator::new(Fbas::new(), Rc::new(SuperSafeQsc), Rc::new(DummyMonitor));
        simulator.simulate_growth(8);
        assert!(simulator.fbas.has_quorum_intersection());
    }

    #[test]
    fn random_fbas_has_a_quorum() {
        let mut simulator = Simulator::new(
            Fbas::new(),
            Rc::new(SimpleRandomNoChangeQsc::new(2, 1)),
            Rc::new(DummyMonitor),
        );
        simulator.simulate_growth(3);
        assert!(simulator.fbas.is_quorum(&bitset![0, 1, 2]));
    }
}
