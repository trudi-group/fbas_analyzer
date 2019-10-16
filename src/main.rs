extern crate fbas_analyzer;

use fbas_analyzer::*;

use quicli::prelude::*;
use structopt::StructOpt;

use std::path::PathBuf;

/// Learn things about a given FBAS (parses data from stellarbeat.org)
#[derive(Debug, StructOpt)]
struct Cli {
    /// Path to JSON file describing the FBAS in stellarbeat.org "nodes" format
    nodes_path: PathBuf,

    /// Output (and find) minimal quorums
    #[structopt(short = "q", long = "get-minimal-quorums")]
    minimal_quorums: bool,

    /// Output (and find) minimal blocking sets (minimal indispensable sets for global liveness)
    #[structopt(short = "b", long = "get-minimal-blocking-sets")]
    minimal_blocking_sets: bool,

    /// Check for quorum intersection, output result
    #[structopt(short = "c", long = "check-quorum-intersection")]
    check_quorum_intersection: bool,

    /// Output minimal quorum intersections (minimal indispensable sets for safety)
    #[structopt(short = "i", long = "get-minimal-intersections")]
    minimal_intersections: bool,

    /// Output (and find) everything we can (use -vv for outputting even more); this is the default
    #[structopt(short = "a", long = "all")]
    all: bool,

    /// Collapse nodes by organization - nodes from the same organization are handled as one;
    /// you must provide the path to a stellarbeat.org "organizations" JSON file
    #[structopt(short = "o", long = "use-organizations")]
    organizations_path: Option<PathBuf>,

    /// In output, identify nodes by their pretty name (public key, or organization if -o is set);
    /// default is to print nodes' index in the input JSON
    #[structopt(short = "p", long = "output-pretty")]
    output_pretty: bool,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

#[allow(clippy::many_single_char_names)]
fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let fbas = Fbas::from_json_file(&args.nodes_path);

    let (q, b, c, i, a) = (
        args.minimal_quorums,
        args.minimal_blocking_sets,
        args.check_quorum_intersection || args.minimal_intersections,
        args.minimal_intersections,
        args.all,
    );
    // no flags set => output everything
    let (q, b, c, i) = if a || (q, b, c, i) == (false, false, false, false) {
        (true, true, true, true)
    } else {
        (q, b, c, i)
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
                to_json_str_using_organization_names(x, &fbas, &orgs)
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
    if q || b || c {
        let minimal_quorums = maybe_collapse(find_minimal_quorums(&fbas));
        let minimal_blocking_sets = if b {
            maybe_collapse(find_minimal_blocking_sets(&minimal_quorums))
        } else {
            vec![]
        };
        let minimal_intersections = if i {
            maybe_collapse(find_minimal_intersections(&minimal_quorums))
        } else {
            vec![]
        };

        if q {
            println!("We found {} minimal quorums:", minimal_quorums.len());
            println!("\n{}\n", format(&minimal_quorums));
        }
        if b {
            println!(
                "We found {} minimal blocking sets (minimal indispensable sets for global liveness):",
                minimal_blocking_sets.len()
            );
            println!("\n{}\n", format(&minimal_blocking_sets));
            println!(
                "Control over any of these sets is sufficient to compromise the liveness of all \
                 nodes and to censor future transactions.\n"
            );
        }
        if c {
            if all_interesect(&minimal_quorums) {
                println!("All quorums intersect.\n");
                if i {
                    println!(
                        "We found {} minimal quorum intersections (minimal indispensable sets for safety):",
                        minimal_intersections.len()
                    );
                    println!("\n{}\n", format(&minimal_intersections));
                    println!(
                        "Control over any of these sets is sufficient to compromise safety by \
                         undermining the quorum intersection of at least two quorums.\n"
                    );
                }
            } else {
                println!(
                    "Some quorums don't intersect - safety severely threatened for some nodes!"
                );
            }
        }
        if q || b || i {
            let mut all_sets = vec![];
            all_sets.extend_from_slice(&minimal_quorums);
            all_sets.extend_from_slice(&minimal_blocking_sets);
            all_sets.extend_from_slice(&minimal_intersections);
            let all_nodes = involved_nodes(&all_sets);
            println!(
                "There is a total of {} distinct nodes involved in all of these sets:",
                all_nodes.len()
            );
            println!("\n{}\n", format(&[all_nodes]));
        }
    }
    Ok(())
}
