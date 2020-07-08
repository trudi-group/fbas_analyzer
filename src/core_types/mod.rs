use super::*;

pub use bit_set::BitSet;
pub use std::collections::BTreeSet;
pub use std::collections::HashMap;
pub use std::collections::HashSet;
pub use std::collections::VecDeque;

use serde::Serialize;

pub type NodeId = usize; // internal and possibly different between runs
pub type PublicKey = String;

pub type NodeIdSet = BitSet;
pub type NodeIdDeque = VecDeque<NodeId>;

mod fbas;
mod organizations;
mod quorum_set;
mod set_helpers;
mod shrinking;

pub use fbas::*;
pub use organizations::*;
pub use quorum_set::*;
pub use set_helpers::*;
pub use shrinking::*;
