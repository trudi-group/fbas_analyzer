type NodeID = usize; // internal and possibly different between runs
type PublicKey = String;

#[derive(Debug, PartialEq)]
pub struct Network {
    nodes: Vec<Node>,
}
#[derive(Debug, PartialEq)]
struct Node {
    public_key: PublicKey,
    quorum_set: QuorumSet,
}
#[derive(Clone, Debug, Default, PartialEq)]
struct QuorumSet {
    threshold: usize,
    validators: Vec<NodeID>,
    inner_quorum_sets: Vec<QuorumSet>,
}
mod io;
mod quorums;

pub use quorums::{all_node_sets_interesect, get_minimal_quorums, has_quorum_intersection};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
