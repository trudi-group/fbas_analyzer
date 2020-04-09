extern crate fbas_analyzer;

use fbas_analyzer::*;

use quicli::prelude::*;
use structopt::StructOpt;

use std::path::PathBuf;
use std::time::Instant;

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

    /// Use quorum finding algorithm that works faster for FBASs that do not enjoy quorum
    /// intersection. In case that there is, indeed, no quorum intersection, outputs two
    /// non-intersecting quorums.
    #[structopt(long = "expect-no-intersection")]
    expect_no_intersection: bool,

    // TODO: describe this; also into -a ?
    #[structopt(long = "symmetric-clusters")]
    symmetric_clusters: bool,

    /// Output (and find) minimal blocking sets (minimal indispensable sets for global liveness)
    #[structopt(short = "b", long = "get-minimal-blocking-sets")]
    minimal_blocking_sets: bool,

    /// Output minimal splitting sets (minimal indispensable sets for safety)
    #[structopt(short = "i", long = "get-minimal-splitting-sets")]
    minimal_splitting_sets: bool,

    /// Output (and find) everything we can (use -vv for outputting even more)
    #[structopt(short = "a", long = "all")]
    all: bool,

    /// Output metrics instead of lists of node lists
    #[structopt(short = "d", long = "describe")]
    describe: bool,

    /// In output, identify nodes by their pretty name (public key, or organization if -o is set);
    /// default is to use node IDs corresponding to indices in the input file
    #[structopt(short = "p", long = "pretty")]
    output_pretty: bool,

    /// Silence the commentary about what is what and what it means
    #[structopt(short = "s", long = "silent")]
    silent: bool,

    /// Merge nodes by organization - nodes from the same organization are handled as one;
    /// you must provide the path to a stellarbeat.org "organizations" JSON file
    #[structopt(short = "o", long = "organizations")]
    organizations_path: Option<PathBuf>,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    // load relevant files
    let fbas = if let Some(nodes_path) = args.nodes_path {
        eprintln!("Reading FBAS JSON from file...");
        Fbas::from_json_file(&nodes_path)
    } else {
        eprintln!("Reading FBAS JSON from STDIN...");
        Fbas::from_json_stdin()
    };
    eprintln!("Loaded FBAS with {} nodes.", fbas.number_of_nodes());
    let organizations = if let Some(organizations_path) = args.organizations_path {
        eprintln!("Will merge nodes by organization; reading organizations JSON from file...");
        let orgs = Organizations::from_json_file(&organizations_path, &fbas);
        eprintln!("Loaded {} organizations.", orgs.number_of_organizations());
        Some(orgs)
    } else {
        None
    };
    let mut analysis = Analysis::new(&fbas, organizations.as_ref());

    // pre-process command line arguments
    let (q, c, b, i) = (
        args.minimal_quorums,
        args.check_quorum_intersection || args.minimal_splitting_sets,
        args.minimal_blocking_sets,
        args.minimal_splitting_sets,
    );
    // -a  => output everything
    let (q, c, b, i) = if args.all {
        (true, true, true, true)
    } else {
        (q, c, b, i)
    };
    let silent = args.silent;
    let output_pretty = args.output_pretty;
    let desc = args.describe;
    assert!(
        !output_pretty || !desc,
        r#"Please choose either "--pretty" or "--describe""#
    ); // TODO make error message prettier

    // (output) helpers
    /// silenceable println
    macro_rules! silprintln {
        ($($tt:tt)*) => ({
            if !silent {
                println!($($tt)*);
            }
        })
    }
    macro_rules! print_result {
        ($result_name:expr, $result:expr) => {
            let result_string = if desc {
                $result.into_describe_string()
            } else if output_pretty {
                $result.into_pretty_string(&fbas, &organizations)
            } else {
                $result.into_id_string()
            };
            println!("{}: {}", $result_name, result_string,);
        };
    }
    macro_rules! time_measured {
        ($operation:expr) => {{
            let measurement_start = Instant::now();
            let return_value = $operation;
            let duration = measurement_start.elapsed();
            (return_value, duration)
        }};
    }
    macro_rules! do_and_report {
        ($result_name:expr, $operation:expr) => {{
            let (result, duration) = time_measured!($operation);
            print_result!($result_name, result);
            println!(
                "{}_analysis_duration: {}s",
                $result_name,
                duration.as_secs_f64()
            );
        }};
    }

    if !desc && !output_pretty {
        silprintln!(
            "In the following dumps, nodes are identified by \
            node IDs corresponding to their index in the input file."
        );
    } else if desc {
        silprintln!(
            "Set list descriptions have the format \
            (number_of_sets, number_of_distinct_nodes, (min_set_size, max_set_size, mean_set_size), \
            [ #members with size 0, #members with size 1, ... , #members with maximum size ]"
        );
    }

    silprintln!("FBAS has {} nodes...", analysis.all_nodes().len());
    // print_result!("all_nodes", analysis.all_nodes());
    if organizations.is_some() {
        silprintln!(
            "(Nodes belonging to the same organization are merged into one; there are {} physical nodes.)",
            analysis.all_physical_nodes().len(),
        );
    }
    silprintln!();

    if args.symmetric_clusters {
        silprintln!("\nLooking for symmetric quorum clusters...\n");
        // TODO: print this prettily too
        println!(
            "symmetric_quorum_clusters: {:?}",
            analysis.symmetric_quorum_clusters()
        );
    }

    if q {
        do_and_report!("minimal_quorums", analysis.minimal_quorums());
        silprintln!(
            "\nWe found {} minimal quorums.\n",
            analysis.minimal_quorums().len()
        );
    }
    if c {
        let has_quorum_intersection = if args.expect_no_intersection {
            let ((has_quorum_intersection, quorums), duration) =
                time_measured!(analysis.has_quorum_intersection_via_alternative_check());
            print_result!("has_quorum_intersection", has_quorum_intersection);
            println!("has_quorum_intersection_analysis_duration: {:?}", duration);
            if let Some(nonintersecting_quorums) = quorums {
                print_result!("nonintersecting_quorums", nonintersecting_quorums);
            }
            has_quorum_intersection
        } else {
            do_and_report!(
                "has_quorum_intersection",
                analysis.has_quorum_intersection()
            );
            analysis.has_quorum_intersection() // from cache
        };
        if has_quorum_intersection {
            silprintln!("\nAll quorums intersect 👍\n");
        } else {
            silprintln!(
                "\nSome quorums don't intersect 👎 Safety severely threatened for some nodes!\n\
                 (Also, the remaining results here might not make much sense.)\n"
            );
        }
    }
    if b {
        do_and_report!("minimal_blocking_sets", analysis.minimal_blocking_sets());
        silprintln!(
            "\nWe found {} minimal blocking sets (minimal indispensable sets for global liveness). \
            Control over any of these sets is sufficient to compromise the liveness of all nodes \
            and to censor future transactions.\n",
            analysis.minimal_blocking_sets().len()
        );
    }
    if i {
        do_and_report!("minimal_splitting_sets", analysis.minimal_splitting_sets());
        silprintln!(
            "\nWe found {} minimal splitting sets \
             (minimal indispensable sets for safety). \
             Control over any of these sets is sufficient to compromise safety by \
             undermining the quorum intersection of at least two quorums.\n",
            analysis.minimal_splitting_sets().len()
        );
    }
    if q || b || i {
        do_and_report!("top_tier", analysis.top_tier());
        silprintln!(
            "\nThere is a total of {} distinct nodes involved in all of these sets (this is the \"top tier\").\n",
            analysis.top_tier().len()
        );
    }
    Ok(())
}
