mod analyses;
mod io;
mod types;

pub use types::Fbas;
use types::*;

pub use io::{to_json_str_using_node_ids, to_json_str_using_public_keys};

pub use analyses::{all_interesect, find_minimal_blocking_sets, find_minimal_quorums};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
