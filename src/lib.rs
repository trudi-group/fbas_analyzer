//! A library and tool for analyzing the quorum structure of federated byzantine agreement systems
//! (FBASs) like [Stellar](https://www.stellar.org/). Related research paper
//! [here](https://arxiv.org/abs/2002.08101).
//!
//! We recommend using the [`Analysis`](struct.Analysis.html) struct for doing analyses.
//!
//! # Example analysis
//! We will load a simple FBAS from the `test_data` folder. We will not use an organizations file;
//! analyses will be based on raw nodes.
//! ```
//! use fbas_analyzer::{Fbas, Analysis, bitset, bitsetvec};
//!
//! let fbas = Fbas::from_json_file(std::path::Path::new("test_data/correct.json"));
//! let analysis = Analysis::new(&fbas);
//!
//! assert!(analysis.has_quorum_intersection());
//!
//! // "Unwrapping" analysis results gives us their internal representation, with node IDs
//! // corresponding to node indices in the input JSON.
//! assert_eq!(bitsetvec![{0,1},{0,10},{1,10}], analysis.minimal_blocking_sets().unwrap());
//!
//! // You can also directly transform results into vectors of public keys or organization names...
//! let splitting_sets_pretty = analysis.minimal_splitting_sets().into_pretty_vec_vec(&fbas, None);
//! assert_eq!(vec!["GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH"], splitting_sets_pretty[0]);
//!
//! // ...or serialize them using serde.
//! assert_eq!("[0,1,10]", serde_json::to_string(&analysis.top_tier()).unwrap());
//!
//! // You can post-process results. Let's say we believe that node 0 has crashed...
//! let mut blocking_sets = analysis.minimal_blocking_sets();
//! blocking_sets.remove_nodes_by_id(&[0]);
//! assert_eq!(bitsetvec![{1},{10}], blocking_sets.minimal_sets().unwrap()); // Yikes!
//! ```

#![doc(html_root_url = "https://docs.rs/fbas_analyzer/0.3.0")]

mod analysis;
mod core_types;
mod io;

pub use analysis::*;
pub use core_types::{Fbas, NodeId, NodeIdSet, Organizations, QuorumSet};
pub use io::AnalysisResult;

use core_types::*;

use log::{debug, info, warn};

#[cfg(feature = "qsc-simulation")]
pub mod simulation;
