pub use std::time::{Duration, Instant};

/// Measure the time it takes for an operation to complete (as `Duration`).
///
/// ## Example
/// ```
/// use fbas_analyzer::*;
/// use std::thread::sleep;
///
/// let ten_millis = Duration::from_millis(10);
/// let eleven_millis = Duration::from_millis(11);
///
/// let (return_value, duration) = timed!({
///     sleep(ten_millis);
///     1 + 2
/// });
///
/// assert_eq!(3, return_value);
/// assert!(ten_millis < duration);
/// assert!(duration < eleven_millis);
/// ```
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
///
/// ## Example
/// ```
/// use fbas_analyzer::*;
/// use std::thread::sleep;
///
/// let ten_millis = 0.01;
/// let eleven_millis = 0.11;
///
/// let (return_value, duration) = timed_secs!({
///     sleep(Duration::from_secs_f64(ten_millis));
///     3 + 5
/// });
///
/// assert_eq!(8, return_value);
/// assert!(ten_millis < duration);
/// assert!(duration < eleven_millis);
/// ```
#[macro_export]
macro_rules! timed_secs {
    ($operation:expr) => {{
        let (return_value, duration) = timed!($operation);
        (return_value, duration.as_secs_f64())
    }};
}

