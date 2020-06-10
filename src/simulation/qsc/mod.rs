use super::*;

use std::cmp;

mod graph_based;
mod ideal;
mod random;
mod super_safe;

pub use graph_based::*;
pub use ideal::*;
pub use random::*;
pub use super_safe::*;

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

fn calculate_threshold(n: usize, relative_threshold: Option<f64>) -> usize {
    if let Some(x) = relative_threshold {
        calculate_x_threshold(n, x)
    } else {
        calculate_67p_threshold(n)
    }
}

/// t = ceil((2n+1)/3) => n >= 3f+1
fn calculate_67p_threshold(n: usize) -> usize {
    // reformed for more robustness against floating point errors
    n - ((n as f64 - 1.) / 3.).floor() as usize
}

/// t = max(1, ceil(nx))
fn calculate_x_threshold(n: usize, x: f64) -> usize {
    // t >= 1 so that we behave like calculate_67p_threshold and not confuse simulation logic
    cmp::max(1, (x * n as f64).ceil() as usize)
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
    fn calculate_67p_threshold_up_to_20() {
        for n in 1..20 {
            assert_is_67p_threshold!(calculate_67p_threshold(n), n);
        }
    }

    #[test]
    fn calculate_relative_threshold_is_at_least_1() {
        assert_eq!(calculate_x_threshold(0, 0.51), 1);
        assert_eq!(calculate_x_threshold(100, 0.), 1);
    }
}
