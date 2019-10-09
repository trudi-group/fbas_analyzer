# FBAS analyzer

This is an experimental library and tool for analyzing the quorum structure of federated byzantine agreement systems like [Stellar](https://www.stellar.org/papers/stellar-consensus-protocol.pdf).

Among other things, it currently can:

- read node and quorum data from [stellarbeat](https://www.stellarbeat.io/)
- find all minimal quorums in the network (minimal here means that each existing quorum is a superset of one of the minimal quorums)
- find all minimal blocking sets in the network (a blocking set here is a set intersecting each existing quorum in at least one node)
- determine quorum intersection and list all minimal quorum intersections (gives a clue about what you need to do to compromise safety)

## Compilation and usage

1. [Install Rust](https://www.rust-lang.org/learn/get-started)
2. Run tests
```
cargo test
```
3. Build
```
cargo build --release
```
4. Run tool using data from stellarbeat
```
target/release/fbas_analyzer test_data/stellarbeat_nodes_2019-09-17.json -o test_data/stellarbeat_organizations_2019-09-17.json -a -p
```
5. Play around some more
```
target/release/fbas_analyzer -h
```

## Resources

- Definitions of FBAS and other terms: [The Stellar Consensus Protocol](https://www.stellar.org/papers/stellar-consensus-protocol.pdf)
- Algorithms for finding minimal quorums and determining some FBAS properties: [Lachowski 2019](https://arxiv.org/abs/1902.06493), related [repo](https://github.com/fixxxedpoint/quorum_intersection)
