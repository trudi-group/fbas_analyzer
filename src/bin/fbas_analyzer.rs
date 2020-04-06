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

    /// Use quorum finding algorithm that works faster for FBASs that do not enjoy quorum
    /// intersection. In case that there is, indeed, no quorum intersection, outputs two
    /// non-intersecting quorums.
    #[structopt(long = "expect-no-intersection")]
    expect_no_intersection: bool,

    /// TODO: describe this
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

    /// Output a histogram of sizes instead of lists of node lists
    #[structopt(short = "H", long = "histogram")]
    histogram: bool,

    /// Silence the commentary about what is what and what it means
    #[structopt(short = "s", long = "silent")]
    silent: bool,

    /// Merge nodes by organization - nodes from the same organization are handled as one;
    /// you must provide the path to a stellarbeat.org "organizations" JSON file
    #[structopt(short = "o", long = "use-organizations")]
    organizations_path: Option<PathBuf>,

    /// In output, identify nodes by their pretty name (public key, or organization if -o is set);
    /// default is to use node IDs corresponding to indices in the input file
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
    // silenceable println
    macro_rules! silprintln {
        ($($tt:tt)*) => ({
            if !silent {
                println!($($tt)*);
            }
        })
    }
    // TODO make sure that only one of those can be chosen
    let output_pretty = args.output_pretty;
    let desc = args.describe;
    let hist = args.histogram;
    macro_rules! print_result {
        ($result_name:expr, $result:expr) => {
            let result_string = if hist {
                $result.into_long_describe_string()
            } else if desc {
                $result.into_describe_string()
            } else if output_pretty {
                $result.into_pretty_string(&fbas, &organizations)
            } else {
                $result.into_id_string()
            };
            println!("{}: {}", $result_name, result_string,);
        };
    }
    if (q, c, b, i) == (false, false, false, false) {
        // FIXME
        eprintln!("Nothing to do... (try the -a flag?)");
    } else if !desc && !output_pretty {
        silprintln!(
            "In the following dumps, nodes are identified by \
            node IDs corresponding to their index in the input file."
        );
    } else if desc {
        silprintln!(
            "Set list descriptions have the format \
            (number_of_sets, number_of_distinct_nodes, (min_set_size, max_set_size, mean_set_size))."
        );
    } else if hist {
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
            "(Nodes belonging to the same organization are merged into one; there are {} physical nodes.\n",
            analysis.all_physical_nodes().len(),
        );
    }

    if args.symmetric_clusters {
        silprintln!("\nLooking for symmetric quorum clusters...\n");
        // TODO: print this prettily too
        println!(
            "symmetric_quorum_clusters: {:?}",
            analysis.symmetric_quorum_clusters()
        );
    }

    if c {
        let has_quorum_intersection = if args.expect_no_intersection {
            let (has_quorum_intersection, quorums) =
                analysis.has_quorum_intersection_via_alternative_check();
            if let Some(nonintersecting_quorums) = quorums {
                print_result!("nonintersecting_quorums", nonintersecting_quorums);
            }
            has_quorum_intersection
        } else {
            analysis.has_quorum_intersection()
        };
        if has_quorum_intersection {
            silprintln!("\nAll quorums intersect 👍\n");
        } else {
            silprintln!(
                "\nSome quorums don't intersect 👎 Safety severely threatened for some nodes!\n\
                 (Also, the remaining results here might not make much sense.)\n"
            );
        }
        print_result!("has_quorum_intersection", has_quorum_intersection);
    }
    if q {
        silprintln!(
            "\nWe found {} minimal quorums.\n",
            analysis.minimal_quorums().len()
        );
        print_result!("minimal_quorums", analysis.minimal_quorums());
    }
    if b {
        silprintln!(
            "\nWe found {} minimal blocking sets (minimal indispensable sets for global liveness). \
            Control over any of these sets is sufficient to compromise the liveness of all nodes \
            and to censor future transactions.\n",
            analysis.minimal_blocking_sets().len()
        );
        print_result!("minimal_blocking_sets", analysis.minimal_blocking_sets());
    }
    if i {
        silprintln!(
            "\nWe found {} minimal splitting sets \
             (minimal indispensable sets for safety). \
             Control over any of these sets is sufficient to compromise safety by \
             undermining the quorum intersection of at least two quorums.\n",
            analysis.minimal_splitting_sets().len()
        );
        print_result!("minimal_splitting_sets", analysis.minimal_splitting_sets());
    }
    if q || b || i {
        let top_tier = analysis.top_tier();
        silprintln!(
            "\nThere is a total of {} distinct nodes involved in all of these sets (this is the \"top tier\").\n",
            top_tier.len()
        );
        if !desc {
            print_result!("top_tier", top_tier);
        }
    }
    silprintln!();
    Ok(())
}
