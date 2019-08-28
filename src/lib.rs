mod types;
mod io;
mod quorums; // <- most of the magic happens here

use types::*;
pub use types::Network;

pub use io::{format_node_set, format_node_sets, format_node_sets_raw};

pub use quorums::{
    all_node_sets_interesect, get_minimal_blocking_sets, get_minimal_quorums,
    has_quorum_intersection,
};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
