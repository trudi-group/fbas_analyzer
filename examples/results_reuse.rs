//! In this example we show how analysis results can be reused, i.e.:
//!
//! 1. How analysis results can be used for some further analysis, such as checking what would
//!    happen when some relevant nodes become faulty.
//! 2. Transforming FBASs into a "standard form" that enables the effective caching of analysis
//!    results.

use fbas_analyzer::*;
use std::collections::HashMap;
use std::path::Path;

use hex;
use sha3::{Digest, Sha3_256};

pub fn main() {
    let nodes_json = Path::new("test_data/stellarbeat_nodes_2019-09-17.json");

    let fbas = Fbas::from_json_file(nodes_json);

    // We can remove nodes from the FBAS before starting any analyses. For example, the below code
    // filters out nodes marked "inactive" in the source JSON. You can filter by other predicates
    // as well!
    let inactive_nodes = FilteredNodes::from_json_file(nodes_json, |v| v["active"] == false);
    let fbas = fbas.without_nodes_pretty(&inactive_nodes.into_pretty_vec());

    // This changes the order of nodes (sorts them by public key) and hence renumbers all IDs!
    // This also discard all nodes that are not part of a strongly connected component, as they
    // are irrelevant for analysis.
    let fbas = fbas.to_standard_form();

    // Now we can calculate a hash of the FBAS such that, if two FBASs have the same hash, their
    // analysis results will be the same!
    let fbas_hash = hex::encode(Sha3_256::digest(&fbas.to_json_string().into_bytes()));
    println!(
        "SHA3 hash of FBAS in standard form (when converted to JSON): {}",
        fbas_hash
    );

    // Now we only need to `do_analysis` when something significant changes in the quorum set
    // configuration!
    let mut results_cache: HashMap<Fbas, CustomResultsStruct> = HashMap::new();
    let analysis_results = if let Some(cached_results) = results_cache.get(&fbas) {
        cached_results.clone()
    } else {
        let new_results = do_analysis(&fbas);
        results_cache.insert(fbas.clone(), new_results.clone());
        new_results
    };

    // PART 2: post-analysis

    // Let's look at blocking sets for example...
    let mbs: NodeIdSetVecResult = analysis_results.minimal_blocking_sets;

    // Make it into a vector of vectors of public keys - you could also do that in `do_analysis`
    // already. `fbas` is where the public key strings are taken from. Add `Some(orgs)` if you want
    // to use organization names instead of public keys.
    let mbs_pretty: Vec<Vec<String>> = mbs.clone().into_pretty_vec_vec(&fbas, None);

    // Collections of minimal sets are usually sorted by increasing size, so is we want a smallest
    // set, we can just get the first.
    println!("Example smallest minimal blocking set: {:?}", mbs_pretty[0]);

    // Merge the results by organization so that each organization is counted as one node.
    let organizations = Groupings::organizations_from_json_file(
        Path::new("test_data/stellarbeat_organizations_2019-09-17.json"),
        &fbas, // It doesn't matter that `fbas` has been transformed into standard form.
    );
    let mbs_by_orgs: NodeIdSetVecResult = mbs.merged_by_group(&organizations).minimal_sets(); // we need this to make it into a collection of minimal sets again

    println!(
        "In the worst case, {} nodes across {} organizations are enough to compromise liveness.",
        mbs.min(),
        mbs_by_orgs.min()
    );

    // Let's say we observe that some nodes are failing...
    let failing_nodes = vec![
        String::from("GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH"),
        String::from("GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ"),
    ];
    let remaining_mbs = mbs
        .without_nodes_pretty(&failing_nodes, &fbas, None)
        .minimal_sets();

    println!(
        "After removing failing nodes, {} more node failures can cause a loss in liveness.",
        remaining_mbs.min()
    );

    // What about quorum intersection?
    let evil_nodes = vec![0, 1, 2, 3, 4, 5, 6]; // random node IDs w.r.t. to FBAS standard form
    let remaining_mss = analysis_results
        .minimal_splitting_sets
        .without_nodes(&evil_nodes)
        .minimal_sets();

    if remaining_mss.contains_empty_set() {
        println!("The FBAS lacks quorum intersection despite evil nodes!");
    } else {
        println!(
            "{} more nodes need to become evil for safety to become endangered.",
            remaining_mss.min()
        );
    }
}

fn do_analysis(fbas: &Fbas) -> CustomResultsStruct {
    let analysis = Analysis::new(fbas);
    CustomResultsStruct {
        minimal_blocking_sets: analysis.minimal_blocking_sets(),
        minimal_splitting_sets: analysis.minimal_splitting_sets(),
        top_tier: analysis.top_tier(),
        has_quorum_intersection: analysis.has_quorum_intersection(),
    }
}

#[derive(Debug, Clone)]
struct CustomResultsStruct {
    minimal_blocking_sets: NodeIdSetVecResult,
    minimal_splitting_sets: NodeIdSetVecResult,
    top_tier: NodeIdSetResult,
    has_quorum_intersection: bool,
}
