use super::*;

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

/// Basic Quorum Set Configurator priorizing FBAS liveness.
///
/// Creates threshold=1 quorum sets containing all nodes in the FBAS
///
/// ```
/// #[macro_use] extern crate fbas_analyzer;
/// use fbas_analyzer::Fbas;
/// use fbas_analyzer::quorum_set_configurators::SuperLiveQsc;
///
/// let mut fbas = Fbas::new();
/// let qsc = SuperLiveQsc;
/// fbas.simulate_growth(3, &qsc);
///
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

/// Basic Quorum Set Configurator priorizing FBAS safety.
///
/// Creates threshold=n quorum sets containing all n nodes in the FBAS.
///
/// ```
/// #[macro_use] extern crate fbas_analyzer;
/// use fbas_analyzer::Fbas;
/// use fbas_analyzer::quorum_set_configurators::SuperSafeQsc;
///
/// let mut fbas = Fbas::new();
/// let qsc = SuperSafeQsc;
/// fbas.simulate_growth(3, &qsc);
///
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn super_live_fbas_has_quorums() {
        let mut fbas = Fbas::new();
        let qsc = SuperLiveQsc;
        fbas.simulate_growth(3, &qsc);
        assert!(fbas.is_quorum(&bitset![0]));
        assert!(fbas.is_quorum(&bitset![1]));
        assert!(fbas.is_quorum(&bitset![2]));
        assert!(fbas.is_quorum(&bitset![0, 1, 2]));
    }

    #[test]
    fn super_safe_fbas_has_a_quorum() {
        let mut fbas = Fbas::new();
        let qsc = SuperSafeQsc;
        fbas.simulate_growth(3, &qsc);
        assert!(fbas.is_quorum(&bitset![0, 1, 2]));
    }

    #[test]
    fn super_live_fbas_has_no_quorum_intersection() {
        let mut fbas = Fbas::new();
        let qsc = SuperLiveQsc;
        fbas.simulate_growth(3, &qsc);
        assert!(!fbas.has_quorum_intersection());
    }

    #[test]
    fn super_safe_fbas_has_quorum_intersection() {
        let mut fbas = Fbas::new();
        let qsc = SuperSafeQsc;
        fbas.simulate_growth(8, &qsc);
        assert!(fbas.has_quorum_intersection());
    }
}
