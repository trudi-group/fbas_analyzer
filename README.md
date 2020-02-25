# FBAS analyzer

A library and tool for analyzing the quorum structure of federated byzantine agreement systems (FBASs) like [Stellar](https://www.stellar.org/).
Related research paper: [The Sum of Its Parts: Analysis of Federated Byzantine Agreement Systems](https://arxiv.org/abs/2002.08101).

Among other things, the implementation here can:

- read node and quorum set data in [stellarbeat](https://www.stellarbeat.io/) format
- find all minimal quorums (minimal here means that each existing quorum is a superset of one of the minimal quorums)
- find all minimal blocking sets (minimal indispensable sets for liveness)
- determine quorum intersection and find all minimal splitting sets (minimal indispensable sets for safety)
- simulate different quorum set configuration policies, yielding synthetic FBASs for further analysis

**This is an intermediate snapshot, expect heavy refactoring in the coming weeks and months. Neither the API nor the CLI should be considered stable in any way!**

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
4. Try tool using older data from stellarbeat
```
target/release/fbas_analyzer test_data/stellarbeat_nodes_2019-09-17.json -o test_data/stellarbeat_organizations_2019-09-17.json -a -p
```
5. Get some new data from stellarbeat (if their API didn't change too much...)
```
scripts/get_latest_stellarbeat_data.sh
```
6. Play around some more
```
target/release/fbas_analyzer -h
target/release/qsc_sim -h
```

## Acknowledgements

- The code for finding minimal quorums is inspired by [Lachowski 2019](https://arxiv.org/abs/1902.06493), respectively this [implementation](https://github.com/fixxxedpoint/quorum_intersection).
