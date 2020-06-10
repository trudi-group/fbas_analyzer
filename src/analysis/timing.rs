pub use std::time::{Duration, Instant};

/// Measure the time it takes for an operation to complete (as `Duration`).
#[macro_export]
macro_rules! timed {
    ($operation:expr) => {{
        let measurement_start = Instant::now();
        let return_value = $operation;
        let duration = measurement_start.elapsed();
        (return_value, duration)
    }};
}

/// Measure the time it takes for an operation to complete (in seconds, as `f64`).
#[macro_export]
macro_rules! timed_secs {
    ($operation:expr) => {{
        let (return_value, duration) = timed!($operation);
        (return_value, duration.as_secs_f64())
    }};
}

