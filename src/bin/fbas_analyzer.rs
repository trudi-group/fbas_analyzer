extern crate fbas_analyzer;

use fbas_analyzer::*;

use quicli::prelude::*;
use structopt::StructOpt;

use std::path::PathBuf;

/// Learn things about a given FBAS (parses data from stellarbeat.org)
#[derive(Debug, StructOpt)]
struct Cli {
    /// Path to JSON file describing the FBAS in stellarbeat.org "nodes" format
    nodes_path: Option<PathBuf>,

    /// Output (and find) minimal quorums
    #[structopt(short = "q", long = "get-minimal-quorums")]
    minimal_quorums: bool,

    /// Check for quorum intersection, output result
    #[structopt(short = "c", long = "check-quorum-intersection")]
    check_quorum_intersection: bool,

    /// Output (and find) minimal blocking sets (minimal indispensable sets for global liveness)
    #[structopt(short = "b", long = "get-minimal-blocking-sets")]
    minimal_blocking_sets: bool,

    /// Output minimal quorum intersections (minimal indispensable sets for safety)
    #[structopt(short = "i", long = "get-minimal-intersections")]
    minimal_intersections: bool,

    /// Output (and find) everything we can (use -vv for outputting even more)
    #[structopt(short = "a", long = "all")]
    all: bool,

    /// Output metrics instead of node lists
    #[structopt(short = "d", long = "describe")]
    describe: bool,

    /// Silence the commentary about what is what and what it means
    #[structopt(short = "s", long = "silent")]
    silent: bool,

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

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let fbas = if let Some(nodes_path) = args.nodes_path {
        eprintln!("Reading FBAS JSON from file...");
        Fbas::from_json_file(&nodes_path)
    } else {
        eprintln!("Reading FBAS JSON from STDIN...");
        Fbas::from_json_stdin()
    };
    let organizations = if let Some(organizations_path) = args.organizations_path {
        eprintln!("Will collapse by organization; reading organizations JSON from file...");
        Some(Organizations::from_json_file(&organizations_path, &fbas))
    } else {
        None
    };

    let mut analysis = if organizations.is_some() {
        Analysis::new_with_collapsing_by_organization(&fbas, organizations.as_ref().unwrap())
    } else {
        Analysis::new(&fbas)
    };

    let (q, c, b, i) = (
        args.minimal_quorums,
        args.check_quorum_intersection || args.minimal_intersections,
        args.minimal_blocking_sets,
        args.minimal_intersections,
    );
    // -a  => output everything
    let (q, c, b, i) = if args.all {
        (true, true, true, true)
    } else {
        (q, c, b, i)
    };

    let silent = args.silent;
    // silenceable println
    macro_rules! silprintln {
        ($($tt:tt)*) => ({
            if !silent {
                println!($($tt)*);
            }
        })
    }
    let output_pretty = args.output_pretty;
    let desc = args.describe;
    macro_rules! print_sets_result {
        ($result_name:expr, $result:expr) => {
            println!(
                "{}: {}",
                $result_name,
                format_node_id_sets($result, &fbas, &organizations, desc, output_pretty)
            );
        };
    }
    macro_rules! print_ids_result {
        ($result_name:expr, $result:expr) => {
            println!(
                "{}: {}",
                $result_name,
                format_node_ids($result, &fbas, &organizations, desc, output_pretty)
            );
        };
    }

    if (q, c, b, i) == (false, false, false, false) {
        eprintln!("Nothing to do... (try the -a flag?)");
    } else if !desc && !output_pretty {
        silprintln!(
            "In the following dumps, nodes are identified by their index in the input JSON."
        );
    } else if desc {
        silprintln!(
            "Set list descriptions have the format \
            (number_of_sets, min_set_size, max_set_size, mean_set_size, number_of_distinct_nodes)."
        );
    }

    let unsatisfiable_nodes = analysis.unsatisfiable_nodes();
    silprintln!(
        "Found {} unsatisfiable nodes (will be ignored in the following).",
        unsatisfiable_nodes.len()
    );
    print_ids_result!("unsatisfiable_nodes", &unsatisfiable_nodes);

    if q {
        silprintln!(
            "\nWe found {} minimal quorums.\n",
            analysis.minimal_quorums().len()
        );
        print_sets_result!("minimal_quorums", analysis.minimal_quorums());
    }
    if c {
        if analysis.has_quorum_intersection() {
            silprintln!("\nAll quorums intersect 👍\n");
            println!("has_quorum_intersection: true");
        } else {
            silprintln!(
                "\nSome quorums don't intersect - safety severely threatened for some nodes!\n"
            );
            println!("quorum_intersection: false");
        }
    }
    if b {
        silprintln!(
            "\nWe found {} minimal blocking sets (minimal indispensable sets for global liveness). \
            Control over any of these sets is sufficient to compromise the liveness of all nodes \
            and to censor future transactions.\n",
            analysis.minimal_blocking_sets().len()
        );
        print_sets_result!("minimal_blocking_sets", analysis.minimal_blocking_sets());
    }
    if i {
        silprintln!(
            "\nWe found {} minimal quorum intersections \
             (minimal indispensable sets for safety). \
             Control over any of these sets is sufficient to compromise safety by \
             undermining the quorum intersection of at least two quorums.\n",
            analysis.minimal_intersections().len()
        );
        print_sets_result!("minimal_intersections", analysis.minimal_intersections());
    }
    if q || b || i {
        let all_nodes = analysis.involved_nodes();
        silprintln!(
            "\nThere is a total of {} distinct nodes involved in all of these sets.\n",
            all_nodes.len()
        );
        if desc {
            println!("involved_nodes: {}", all_nodes.len());
        } else {
            print_ids_result!("involved_nodes", &all_nodes);
        }
    }
    silprintln!();
    Ok(())
}
