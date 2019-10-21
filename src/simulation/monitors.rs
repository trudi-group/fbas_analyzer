use super::*;
use std::cell::{Ref, RefCell};

/// Does nothing.
#[derive(Default)]
pub struct DummyMonitor;
impl SimulationMonitor for DummyMonitor {
    fn register_event(&self, _: Event) {}
}

/// Stores all events in `recorded_events` for later analysis
#[derive(Default)]
pub struct DebugMonitor {
    recorded_events: RefCell<Vec<Event>>,
}
impl DebugMonitor {
    pub fn new() -> Self {
        DebugMonitor {
            recorded_events: RefCell::new(vec![]),
        }
    }
    pub fn events(&self) -> Ref<Vec<Event>> {
        self.recorded_events.borrow()
    }
}
impl SimulationMonitor for DebugMonitor {
    fn register_event(&self, event: Event) {
        self.recorded_events.borrow_mut().push(event);
    }
}
