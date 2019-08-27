extern crate fbas_analyzer;

use fbas_analyzer::*;

use quicli::prelude::*;
use structopt::StructOpt;

/// Learn things about a given FBAS
#[derive(Debug, StructOpt)]
struct Cli {
    /// Path to JSON file describing the FBAS in stellarbeat.org format
    path: String,

    /// Output (and find) minimal quorums
    #[structopt(short = "q", long = "minimal-quorums")]
    minimal_quorums: bool,

    /// Output (and find) minimal blocking sets
    #[structopt(short = "b", long = "minimal-blocking-sets")]
    minimal_blocking_sets: bool,

    /// Check for quorum intersection, output result
    #[structopt(short = "i", long = "quorum-intersection")]
    quorum_intersection: bool,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let network = Network::from_json_file(&args.path);

    let (q, b, i) = (
        args.minimal_quorums,
        args.minimal_blocking_sets,
        args.quorum_intersection,
    );
    // no flags set => output everything
    let (q, b, i) = if (q, b, i) == (false, false, false) {
        (true, true, true)
    } else {
        (q, b, i)
    };

    if q || b {
        println!(
            "(In all following dumps, nodes are identified by their index in the input JSON.)\n"
        );
    }
    if q || b || i {
        let minimal_quorums = get_minimal_quorums(&network);

        if q {
            println!("We found {} minimal quorums:", minimal_quorums.len());
            println!("\n{}\n", node_sets_to_json(&minimal_quorums));
        }
        if b {
            let minimal_blocking_sets = get_minimal_blocking_sets(&minimal_quorums);
            println!(
                "We found {} minimal blocking sets:",
                minimal_blocking_sets.len()
            );
            println!("\n{}\n", node_sets_to_json(&minimal_blocking_sets));
            println!(
                "Control over any of these node sets is sufficient to compromise liveliness and \
                 censor future transactions.\n"
            );
        }
        if i {
            if all_node_sets_interesect(&minimal_quorums) {
                println!("All quorums intersect.");
            } else {
                println!(
                    "Some quorums don't intersect - safety severely threatened for some nodes!"
                );
            }
        }
    }
    Ok(())
}
