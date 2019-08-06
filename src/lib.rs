type NodeID = usize; // internal and possibly different between runs
type PublicKey = String;

#[derive(Debug, PartialEq)]
struct Network {
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
