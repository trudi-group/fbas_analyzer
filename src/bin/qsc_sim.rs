extern crate fbas_analyzer;

use fbas_analyzer::*;

use quicli::prelude::*;
use structopt::StructOpt;

use std::rc::Rc;

/// FBAS quorum set configuration (QSC) simulation sandbox.
/// QSC strategies selected via SUBCOMMAND.
#[derive(Debug, StructOpt)]
struct Cli {
    /// Initial size of the simulated FBAS. Default is 0.
    #[structopt(short = "i", long = "initial", default_value = "0")]
    initial_n: usize,

    /// If set, adds the specified number of nodes via simulated organic growth.
    #[structopt(short = "g", long = "grow-by", default_value = "0")]
    grow_by_n: usize,

    /// Quorum set configuration strategy to simulate
    #[structopt(subcommand)]
    qscc: QuorumSetConfiguratorConfig,

    #[structopt(flatten)]
    verbosity: Verbosity,
}
#[derive(Debug, StructOpt)]
enum QuorumSetConfiguratorConfig {
    /// Creates threshold=n quorum sets containing all n nodes in the FBAS
    SuperSafe,
    /// Builds quorum sets containing all n nodes in the FBAS, with thresholds chosen such that
    /// a maximum of f nodes can fail, where (n-1) < (3f+1) <= n
    Ideal,
    /// Creates random quorum sets of the given size and threshold
    SimpleRandom {
        desired_quorum_set_size: usize,
        desired_threshold: usize,
    },
    /// Creates random quorum sets of the given size and threshold. Validators are picked based on
    /// their node degree in a scale free graph ("famousness")
    FameWeightedRandom {
        desired_quorum_set_size: usize,
        desired_threshold: usize,
        graph_size: Option<usize>,
    },
    /// Chooses quorum sets based on a synthetic scale-free graph (BA with m0=m=2) and a relative
    /// threshold. All graph neighbors are validators, independent of node existence, quorum
    /// intersection or anything else.
    SimpleScaleFree {
        relative_threshold: f64,
        graph_size: Option<usize>,
    },
    /// Chooses quorum sets based on a synthetic small world graph (Watts-Strogatz with beta = 0.05)
    /// and a relative threshold. All graph neighbors are validators, independent of node
    /// existence, quorum intersection or anything else.
    SimpleSmallWorld {
        mean_degree: usize,
        relative_threshold: f64,
        graph_size: Option<usize>,
    },
}

fn parse_qscc(
    qscc: QuorumSetConfiguratorConfig,
    fbas_size: usize,
) -> Rc<dyn QuorumSetConfigurator> {
    use quorum_set_configurators::*;
    use QuorumSetConfiguratorConfig::*;
    match qscc {
        SuperSafe => Rc::new(SuperSafeQsc::new()),
        Ideal => Rc::new(IdealQsc::new()),
        SimpleRandom {
            desired_quorum_set_size,
            desired_threshold,
        } => Rc::new(RandomQsc::new_simple(
            desired_quorum_set_size,
            desired_threshold,
        )),
        FameWeightedRandom {
            desired_quorum_set_size,
            desired_threshold,
            graph_size,
        } => Rc::new(RandomQsc::new(
            desired_quorum_set_size,
            desired_threshold,
            Graph::new_random_scale_free(graph_size.unwrap_or(fbas_size * 100), 2, 2)
                .shuffled()
                .get_node_degrees(),
        )),
        SimpleScaleFree {
            graph_size,
            relative_threshold,
        } => Rc::new(SimpleGraphQsc::new(
            Graph::new_random_scale_free(graph_size.unwrap_or(fbas_size), 2, 2).shuffled(),
            // shuffled because fbas join order shouldn't be correlated with importance in graph
            relative_threshold,
        )),
        SimpleSmallWorld {
            graph_size,
            mean_degree,
            relative_threshold,
        } => Rc::new(SimpleGraphQsc::new(
            Graph::new_random_small_world(graph_size.unwrap_or(fbas_size), mean_degree, 0.05)
                .shuffled(),
            // shuffled because fbas join order shouldn't be correlated with importance in graph
            relative_threshold,
        )),
    }
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let n = args.initial_n + args.grow_by_n;

    let qsc = parse_qscc(args.qscc, n);
    let monitor = Rc::new(monitors::DebugMonitor::new());

    let mut simulator = Simulator::new(
        Fbas::new_generic_unconfigured(args.initial_n),
        qsc,
        Rc::clone(&monitor) as Rc<dyn SimulationMonitor>,
    );
    eprintln!("Starting simulation...");
    simulator.simulate_global_reevaluation(args.initial_n);
    simulator.simulate_growth(args.grow_by_n);
    let fbas = simulator.finalize();
    eprintln!("Finished simulation, dumping FBAS...");
    println!("{}", fbas.to_json_string_pretty());
    Ok(())
}
