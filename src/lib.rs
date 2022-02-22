//! A library and tool for analyzing the quorum structure of federated byzantine agreement systems
//! (FBASs) like [Stellar](https://www.stellar.org/). Related research paper
//! [here](https://arxiv.org/abs/2002.08101).
//!
//! We recommend using the [`Analysis`](struct.Analysis.html) struct for doing analyses.
//!
//! # Example analysis
//! ```
//! use fbas_analyzer::{Fbas, Analysis, bitset, bitsetvec};
//!
//! let fbas = Fbas::from_json_file(std::path::Path::new("test_data/correct.json"));
//! let mut analysis = Analysis::new(&fbas);
//!
//! assert!(analysis.has_quorum_intersection());
//!
//! // "Unwrapping" analysis results gives us their internal representation, with node IDs
//! // corresponding to node indices in the input JSON.
//! assert_eq!(bitsetvec![{0,1},{0,10},{1,10}], analysis.minimal_blocking_sets().unwrap());
//!
//! // You can directly transform results into vectors of public keys or organization names...
//! let mss_pretty = analysis.minimal_splitting_sets().into_pretty_vec_vec(&fbas, None);
//! assert_eq!(vec!["GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH"], mss_pretty[0]);
//!
//! // ...or serialize them using serde.
//! assert_eq!("[0,1,10]", serde_json::to_string(&analysis.top_tier()).unwrap());
//!
//! // You can also post-process results. Let's say we believe that node 0 has crashed...
//! let remaining_mbs = analysis.minimal_blocking_sets().without_nodes(&[0]).minimal_sets();
//! assert_eq!(bitsetvec![{1},{10}], remaining_mbs.unwrap()); // Yikes!
//! ```
//!
//! # More examples...
//!
//! ...can be found in the `src/bin` and `examples` folders...

#![doc(html_root_url = "https://docs.rs/fbas_analyzer/0.6.0")]

mod analysis;
mod core_types;
mod io;

pub use analysis::*;
pub use core_types::{Fbas, Groupings, NodeId, NodeIdSet, QuorumSet};
pub use io::{AnalysisResult, FilteredNodes, PrettyQuorumSet};

use core_types::*;

use log::{debug, info, warn};

#[cfg(feature = "qsc-simulation")]
pub mod simulation;
