use std::path::Path;
use std::{fmt, fs, io};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::*;

macro_rules! read_or_panic {
    ($path:expr) => {{
        fs::read_to_string($path).unwrap_or_else(|_| panic!("Error reading file {:?}", $path))
    }};
}

mod core_types;
use core_types::*;

mod groupings;

mod filtered_nodes;
pub use filtered_nodes::FilteredNodes;

mod results;
pub use results::*;

#[cfg(feature = "qsc-simulation")]
mod graph;
