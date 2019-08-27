extern crate fbas_analyzer;

use fbas_analyzer::*;

use quicli::prelude::*;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Cli {
    /// Path to JSON file describing the FBAS in stellarbeat.org format.
    path: String,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

fn main() -> CliResult {

    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let network = Network::from_json_file(&args.path);

    println!(
        "(In all following dumps, nodes are identified by their index in the input JSON.)\n"
    );

    let minimal_quorums = get_minimal_quorums(&network);
    println!("We found {} minimal quorums:", minimal_quorums.len());
    println!("\n{}\n", node_sets_to_json(&minimal_quorums));

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

    if all_node_sets_interesect(&minimal_quorums) {
        println!("All quorums intersect.");
    } else {
        println!("Some quorums don't intersect - safety severely threatened for some nodes!");
    }
    println!();

    Ok(())
}
