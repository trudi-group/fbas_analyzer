use super::*;

/// Dummy Simulation Monitor
///
/// Does nothing.
#[derive(Default)]
pub struct DummyMonitor;
impl SimulationMonitor for DummyMonitor {
    fn register_event(&mut self, event: Event) {}
}
