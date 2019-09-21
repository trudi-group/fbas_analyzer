extern crate fbas_analyzer;

use fbas_analyzer::*;

use quicli::prelude::*;
use structopt::StructOpt;

use std::path::PathBuf;

/// Learn things about a given FBAS
#[derive(Debug, StructOpt)]
struct Cli {
    /// Path to JSON file describing the FBAS in stellarbeat.org "nodes" format
    nodes_path: PathBuf,

    /// Output (and find) minimal quorums
    #[structopt(short = "q", long = "minimal-quorums")]
    minimal_quorums: bool,

    /// Output (and find) minimal blocking sets
    #[structopt(short = "b", long = "minimal-blocking-sets")]
    minimal_blocking_sets: bool,

    /// Check for quorum intersection, output result
    #[structopt(short = "i", long = "quorum-intersection")]
    quorum_intersection: bool,

    /// Collapse nodes by organization - nodes from the same organization are handled as one;
    /// you must provide the path to a stellarbeat.org "organizations" JSON file
    #[structopt(short = "o", long = "organizations")]
    organizations_path: Option<PathBuf>,

    /// In output, identify nodes by their pretty name (public key, or organization if -o is set);
    /// default is to print nodes' index in the input JSON
    #[structopt(short = "p", long = "output-pretty")]
    output_pretty: bool,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let fbas = Fbas::from_json_file(&args.nodes_path);

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

    let organizations = if let Some(organizations_path) = args.organizations_path {
        Some(Organizations::from_json_file(&organizations_path, &fbas))
    } else {
        None
    };
    let maybe_collapse = |x| {
        if let Some(ref orgs) = organizations {
            remove_non_minimal_node_sets(orgs.collapse_node_sets(x))
        } else {
            x
        }
    };

    let p = args.output_pretty;

    let format = |x| {
        if p {
            if let Some(ref orgs) = organizations {
                String::from("not implemented yet")
            } else {
                to_json_str_using_public_keys(x, &fbas)
            }
        } else {
            to_json_str_using_node_ids(x)
        }
    };

    if !p && (q || b) {
        println!(
            "(In the following dumps, nodes are identified by their index in the input JSON.)\n"
        );
    }
    if q || b || i {
        let minimal_quorums = maybe_collapse(find_minimal_quorums(&fbas));

        if q {
            println!("We found {} minimal quorums:", minimal_quorums.len());
            println!("\n{}\n", format(&minimal_quorums));
        }
        if b {
            let minimal_blocking_sets =
                maybe_collapse(find_minimal_blocking_sets(&minimal_quorums));
            println!(
                "We found {} minimal blocking sets:",
                minimal_blocking_sets.len()
            );
            println!("\n{}\n", format(&minimal_blocking_sets));
            println!(
                "Control over any of these sets is sufficient to compromise liveliness and \
                 censor future transactions.\n"
            );
        }
        if i {
            if all_interesect(&minimal_quorums) {
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
