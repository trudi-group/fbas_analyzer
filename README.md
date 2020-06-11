# FBAS analyzer

Library and tools for analyzing the quorum structure of Federated Byzantine Agreement Systems (FBASs) like [Stellar](https://www.stellar.org/).
Related research paper [here](https://arxiv.org/abs/2002.08101).

Among other things, the implementation here can:

- read node and organizations data in [stellarbeat](https://stellarbeat.io/)'s JSON format
- determine quorum intersection
- find all minimal quorums (minimal here means that each existing quorum is a superset of one of the minimal quorums)
- find all minimal blocking sets (minimal indispensable sets for liveness)
- find all minimal splitting sets (minimal indispensable sets for safety)
- simulate different quorum set configuration policies, yielding synthetic FBASs for further analysis

Powers our [Stellar Network Analysis](https://stellar.weizenbaum.net/analysis.html).

## Usage as tools

1. [Install Rust](https://www.rust-lang.org/learn/get-started)
2. (optional) Run all kinds of tests
```
scripts/tests.py
```
3. Build
```
cargo build --release
```
4. Try tool using older data from stellarbeat
```
target/release/fbas_analyzer test_data/stellarbeat_nodes_2019-09-17.json -m test_data/stellarbeat_organizations_2019-09-17.json -a -p
```
5. Get some new data from stellarbeat
```
scripts/get_latest_stellarbeat_data.sh
```
6. Play around some more
```
target/release/fbas_analyzer -h
target/release/bulk_fbas_analyzer -h
target/release/qsc_simulator -h
target/release/graph_generator -h
```

## Usage as Rust library

Add this to your `Cargo.toml`:
```
[dependencies]
fbas_analyzer = { version = "0.3", default-features = false }
```
Or this, if you need simulation functionality:
```
[dependencies]
fbas_analyzer = { version = "0.3", default-features = false, features = ["qsc_simulation"] }
```

Then check out the [API Reference](https://docs.rs/fbas_analyzer/)
and how the API is used by the tools `src/bin/`.

## See also / Acknowledgements

- The algorithms for determining quorum intersection and finding minimal quorums is inspired by [Lachowski 2019](https://arxiv.org/abs/1902.06493), respectively this [implementation](https://github.com/fixxxedpoint/quorum_intersection).
- [Stellar Observatory](https://github.com/andrenarchy/stellar-observatory) - a different set of FBAS analyses.
- [nodejs_fbas_analyzer](https://github.com/stellarbeat/nodejs_fbas_analyzer/)

...and of course the awesome [stellarbeat.io](http://stellarbeat.io) :)
