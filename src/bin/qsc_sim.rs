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
    /// Creates random quorum sets of the given size, using 67% thresholds as in "Ideal".
    SimpleRandom {
        desired_quorum_set_size: usize,
    },
    /// Creates random quorum sets of the given size and threshold. The probability of picking a
    /// node as a validator is weighted by that node's degree in a scale free graph ("famousness")
    /// If threshold is ommitted, uses as 67% threshold as in "Ideal".
    FameWeightedRandom {
        desired_quorum_set_size: usize,
        desired_threshold: Option<usize>,
        graph_size: Option<usize>,
    },
    /// Chooses quorum sets based on a synthetic scale-free graph (BA with m0=m=mean_degree/2) and
    /// a relative threshold. All graph neighbors are validators, independent of node existence,
    /// quorum intersection or anything else.
    /// If threshold is ommitted, uses as 67% threshold as in "Ideal".
    SimpleScaleFree {
        mean_degree: usize,
        relative_threshold: Option<f64>,
        graph_size: Option<usize>,
    },
    /// Chooses quorum sets based on a synthetic small world graph (Watts-Strogatz with beta = 0.05)
    /// and a relative threshold. All graph neighbors are validators, independent of node
    /// existence, quorum intersection or anything else.
    /// If threshold is ommitted, uses as 67% threshold as in "Ideal".
    SimpleSmallWorld {
        mean_degree: usize,
        relative_threshold: Option<f64>,
        graph_size: Option<usize>,
    },
    /// TODO - might be removed again soon
    QualityAware { graph_size: Option<usize> },
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
        } => Rc::new(RandomQsc::new_simple(
            desired_quorum_set_size,
        )),
        FameWeightedRandom {
            desired_quorum_set_size,
            desired_threshold,
            graph_size,
        } => Rc::new(RandomQsc::new(
            desired_quorum_set_size,
            desired_threshold,
            Some(Graph::new_random_scale_free(graph_size.unwrap_or(fbas_size * 100), 2, 2)
                .shuffled()
                .get_in_degrees()),
        )),
        SimpleScaleFree {
            mean_degree,
            relative_threshold,
            graph_size,
        } => {
            let n = graph_size.unwrap_or(fbas_size);
            let m = mean_degree / 2;
            let m0 = m;
            // shuffled because fbas join order shouldn't be correlated with importance in graph
            let graph = Graph::new_random_scale_free(n, m, m0).shuffled();
            Rc::new(SimpleGraphQsc::new(graph, relative_threshold))
        },
        SimpleSmallWorld {
            mean_degree,
            relative_threshold,
            graph_size,
        } => {
            let n = graph_size.unwrap_or(fbas_size);
            let k = mean_degree;
            // shuffled because fbas join order shouldn't be correlated with importance in graph
            let graph = Graph::new_random_small_world(n, k, 0.05).shuffled();
            Rc::new(SimpleGraphQsc::new(graph, relative_threshold))
        },
        QualityAware { graph_size } => Rc::new(QualityAwareGraphQsc::new(
            // shuffled because fbas join order shouldn't be correlated with importance in graph
            Graph::new_random_scale_free(graph_size.unwrap_or(fbas_size), 2, 2).shuffled(),
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
