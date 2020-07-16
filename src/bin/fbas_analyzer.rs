extern crate fbas_analyzer;

use fbas_analyzer::*;

use quicli::prelude::*;
use structopt::StructOpt;

use std::path::PathBuf;

/// Learn things about a given FBAS (parses data from stellarbeat.org)
#[derive(Debug, StructOpt)]
struct Cli {
    /// Path to JSON file describing the FBAS in stellarbeat.org "nodes" format.
    /// Will use STDIN if omitted.
    nodes_path: Option<PathBuf>,

    /// Output (and find) minimal quorums.
    #[structopt(short = "q", long = "minimal-quorums")]
    minimal_quorums: bool,

    /// Output (and find) minimal blocking sets (minimal indispensable sets for global liveness).
    #[structopt(short = "b", long = "minimal-blocking-sets")]
    minimal_blocking_sets: bool,

    /// Output (and find) minimal splitting sets (minimal indispensable sets for safety).
    #[structopt(short = "s", long = "minimal-splitting-sets")]
    minimal_splitting_sets: bool,

    /// Output (and find) all minimal quorums, minimal blocking sets and minimal splitting sets.
    #[structopt(short = "a", long = "all")]
    all: bool,

    /// Use quorum finding algorithm that works faster for FBASs that do not enjoy quorum
    /// intersection. In case that there is, indeed, no quorum intersection, outputs two
    /// non-intersecting quorums.
    #[structopt(long = "expect-no-intersection")]
    expect_no_intersection: bool,

    /// Don't check quorum intersection.
    #[structopt(long = "dont-check-quorum-intersection")]
    dont_check_quorum_intersection: bool,

    /// Output metrics instead of lists of node lists.
    #[structopt(short = "d", long = "describe")]
    describe: bool,

    /// In output, identify nodes by their pretty name (public key, or organization if -o is set);
    /// default is to use node IDs corresponding to indices in the input file.
    #[structopt(short = "p", long = "pretty")]
    output_pretty: bool,

    /// Silence the commentary about what is what and what it means.
    #[structopt(long = "results-only")]
    results_only: bool,

    /// Merge nodes by organization - nodes from the same organization are handled as one;
    /// you must provide the path to a stellarbeat.org "organizations" JSON file.
    #[structopt(short = "m", long = "merge-by-org")]
    organizations_path: Option<PathBuf>,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let fbas = load_fbas(args.nodes_path.as_ref());
    let organizations = maybe_load_organizations(args.organizations_path.as_ref(), &fbas);
    let analysis = Analysis::new(&fbas);

    let (q, b, s) = extract_main_todos(&args);
    let output = Output::init(&args, &fbas, &organizations);

    report_overview(&analysis, &organizations, &output);
    output.comment_newline();

    find_and_report_symmetric_clusters(&analysis, &organizations, &output);

    if q {
        find_and_report_minimal_quorums(&analysis, &organizations, &output);
    }

    if !args.dont_check_quorum_intersection {
        check_and_report_if_has_quorum_intersection(
            &analysis,
            &output,
            args.expect_no_intersection,
        );
    }

    if b {
        find_and_report_minimal_blocking_sets(&analysis, &organizations, &output);
    }
    if s {
        find_and_report_minimal_splitting_sets(&analysis, &organizations, &output);
    }
    if q || b || s {
        report_top_tier_uncondensed(&analysis, &organizations, &output);
    }
    Ok(())
}

fn load_fbas(o_nodes_path: Option<&PathBuf>) -> Fbas {
    let fbas = if let Some(nodes_path) = o_nodes_path {
        eprintln!("Reading FBAS JSON from file...");
        Fbas::from_json_file(nodes_path)
    } else {
        eprintln!("Reading FBAS JSON from STDIN...");
        Fbas::from_json_stdin()
    };
    eprintln!("Loaded FBAS with {} nodes.", fbas.number_of_nodes());
    fbas
}
fn maybe_load_organizations<'a>(
    o_organizations_path: Option<&PathBuf>,
    fbas: &'a Fbas,
) -> Option<Organizations<'a>> {
    if let Some(organizations_path) = o_organizations_path {
        eprintln!("Will merge nodes by organization; reading organizations JSON from file...");
        let orgs = Organizations::from_json_file(organizations_path, fbas);
        eprintln!("Loaded {} organizations.", orgs.number_of_organizations());
        Some(orgs)
    } else {
        None
    }
}
fn extract_main_todos(args: &Cli) -> (bool, bool, bool) {
    if args.all {
        (true, true, true)
    } else {
        (
            args.minimal_quorums,
            args.minimal_blocking_sets,
            args.minimal_splitting_sets,
        )
    }
}

macro_rules! do_time_and_report {
    ($result_name:expr, $operation:expr, $output:expr) => {{
        let (result, duration) = timed!($operation);
        $output.timed_result($result_name, result, duration);
    }};
}
macro_rules! do_time_maybe_merge_and_report {
    ($result_name:expr, $operation:expr, $organizations:expr, $output:expr) => {{
        let (mut result, duration) = timed!($operation);
        if let Some(ref orgs) = $organizations {
            result = result.merged_by_org(orgs).minimal_sets();
        }
        $output.timed_result($result_name, result, duration);
    }};
}

fn report_overview(analysis: &Analysis, organizations: &Option<Organizations>, output: &Output) {
    output.result("nodes_total", analysis.all_nodes().len());
    if let Some(ref orgs) = organizations {
        output.result(
            "nodes_total_merged",
            analysis.all_nodes().merged_by_org(orgs).len(),
        );
        output.comment("(Nodes belonging to the same organization will be counted as one.)");
    }
}
fn check_and_report_if_has_quorum_intersection(
    analysis: &Analysis,
    output: &Output,
    alternative_check: bool,
) {
    let has_quorum_intersection = if alternative_check {
        output.comment("Alternative quorum intersection check...");
        let ((has_quorum_intersection, quorums), duration) =
            timed!(analysis.has_quorum_intersection_via_alternative_check());
        output.timed_result("has_quorum_intersection", has_quorum_intersection, duration);
        if let Some(nonintersecting_quorums) = quorums {
            output.result("nonintersecting_quorums", nonintersecting_quorums);
        }
        has_quorum_intersection
    } else {
        do_time_and_report!(
            "has_quorum_intersection",
            analysis.has_quorum_intersection(),
            output
        );
        analysis.has_quorum_intersection() // from cache
    };
    if has_quorum_intersection {
        output.comment("\nAll quorums intersect 👍\n");
    } else {
        output.comment(
            "\nSome quorums don't intersect 👎 Safety severely threatened for some nodes!\n\
                 (Also, the remaining results here might not make much sense.)\n",
        );
    }
}
fn find_and_report_symmetric_clusters(
    analysis: &Analysis,
    organizations: &Option<Organizations>,
    output: &Output,
) {
    let mut output_uncondensed = output.clone();
    output_uncondensed.describe = false;
    do_time_and_report!(
        "symmetric_clusters",
        if let Some(ref orgs) = organizations {
            orgs.merge_quorum_sets(analysis.symmetric_clusters())
        } else {
            analysis.symmetric_clusters()
        },
        output_uncondensed
    );
    output.comment_newline();
}
fn find_and_report_minimal_quorums(
    analysis: &Analysis,
    organizations: &Option<Organizations>,
    output: &Output,
) {
    do_time_maybe_merge_and_report!(
        "minimal_quorums",
        analysis.minimal_quorums(),
        organizations,
        output
    );
    output.comment(&format!(
        "\nWe found {} minimal quorums.\n",
        analysis.minimal_quorums().len()
    ));
}
fn find_and_report_minimal_blocking_sets(
    analysis: &Analysis,
    organizations: &Option<Organizations>,
    output: &Output,
) {
    do_time_maybe_merge_and_report!(
        "minimal_blocking_sets",
        analysis.minimal_blocking_sets(),
        organizations,
        output
    );
    output.comment(&format!(
        "\nWe found {} minimal blocking sets (minimal indispensable sets for global liveness). \
            Control over any of these sets is sufficient to compromise the liveness of all nodes \
            and to censor future transactions.\n",
        analysis.minimal_blocking_sets().len()
    ));
}
fn find_and_report_minimal_splitting_sets(
    analysis: &Analysis,
    organizations: &Option<Organizations>,
    output: &Output,
) {
    do_time_maybe_merge_and_report!(
        "minimal_splitting_sets",
        analysis.minimal_splitting_sets(),
        organizations,
        output
    );
    output.comment(&format!(
        "\nWe found {} minimal splitting sets \
             (minimal indispensable sets for safety). \
             Control over any of these sets is sufficient to compromise safety by \
             undermining the quorum intersection of at least two quorums.\n",
        analysis.minimal_splitting_sets().len()
    ));
}
fn report_top_tier_uncondensed(
    analysis: &Analysis,
    organizations: &Option<Organizations>,
    output: &Output,
) {
    let mut top_tier = analysis.top_tier();
    if let Some(ref orgs) = organizations {
        top_tier = top_tier.merged_by_org(orgs);
    }
    output.result_uncondensed("top_tier", top_tier.clone());
    output.comment(
        &format!(
            "\nThere is a total of {} distinct nodes involved in all of these sets (this is the \"top tier\").\n",
            top_tier.len()
        )
    );
}

#[derive(Clone)]
struct Output<'a> {
    results_only: bool,
    output_pretty: bool,
    describe: bool,
    fbas: &'a Fbas,
    organizations: &'a Option<Organizations<'a>>,
}
impl<'a> Output<'a> {
    fn init(args: &Cli, fbas: &'a Fbas, organizations: &'a Option<Organizations>) -> Self {
        let results_only = args.results_only;
        let output_pretty = args.output_pretty;
        let describe = args.describe;
        if !results_only {
            if !output_pretty {
                println!(
                    "In the following dumps, nodes are identified by \
                    node IDs corresponding to their index in the input file."
                );
            }
            if describe {
                println!(
                    "\"Set of sets\"-type results are described as: \
                    [#sets, #distinct_nodes, [min_set_size, max_set_size, mean_set_size], \
                    [#sets_with_size_0, #sets_with_size_1, ..., #sets_with_max_set_size]]"
                );
            }
        }
        Self {
            results_only,
            output_pretty,
            describe,
            fbas,
            organizations,
        }
    }
    fn comment(&self, comment: &str) {
        if !self.results_only {
            println!("{}", comment);
        }
    }
    fn comment_newline(&self) {
        if !self.results_only {
            println!();
        }
    }
    fn timed_result(&self, result_name: &str, result: impl AnalysisResult, duration: Duration) {
        self.result(result_name, result);
        println!(
            "{}_analysis_duration: {}s",
            result_name,
            duration.as_secs_f64()
        );
    }
    fn result(&self, result_name: &str, result: impl AnalysisResult) {
        if self.describe {
            println!("{}: {}", result_name, result.into_describe_string());
        } else {
            self.result_uncondensed(result_name, result);
        }
    }
    fn result_uncondensed(&self, result_name: &str, result: impl AnalysisResult) {
        let result_string = if self.output_pretty {
            result.into_pretty_string(self.fbas, self.organizations.as_ref())
        } else {
            result.into_id_string()
        };
        println!("{}: {}", result_name, result_string);
    }
}
