use bit_set::BitSet;

type NodeID = usize; // internal and possibly different between runs
type PublicKey = String;

struct Network {
    nodes: Vec<Node>,
}
struct Node {
    pk: PublicKey,
    qset: QuorumSet,
}
struct QuorumSet {
    threshold: usize,
    validators: BitSet<NodeID>,
    inner_quorum_sets: Vec<QuorumSet>
}

mod io;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
