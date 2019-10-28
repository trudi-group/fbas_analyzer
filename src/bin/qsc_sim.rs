extern crate fbas_analyzer;

use fbas_analyzer::*;

use quicli::prelude::*;
use structopt::StructOpt;

use std::rc::Rc;

/// FBAS quorum set configuration (QSC) simulation sandbox.
/// QSC strategies selected via SUBCOMMAND.
#[derive(Debug, StructOpt)]
struct Cli {
    /// Nodes to spawn / final size of the simulated FBAS
    #[structopt(short = "n")]
    number_of_nodes: usize,

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
    /// Creates random quorum sets of the given size and threshold, never adapts them
    SimpleRandom {
        desired_quorum_set_size: usize,
        desired_threshold: usize,
    },
    /// Chooses quorum sets based on a synthetic scale-free graph (BA with m0=m=2) and a relative
    /// threshold. All graph neighbors are validators, independent of node existence, quorum
    /// intersection or anything else.
    SimpleGraph {
        graph_size: usize,
        relative_threshold: f64,
    },
}

fn parse_qscc(qscc: QuorumSetConfiguratorConfig) -> Rc<dyn QuorumSetConfigurator> {
    use quorum_set_configurators::*;
    use QuorumSetConfiguratorConfig::*;
    match qscc {
        SuperSafe => Rc::new(SuperSafeQsc::new()),
        SimpleRandom {
            desired_quorum_set_size,
            desired_threshold,
        } => Rc::new(SimpleRandomQsc::new(
            desired_quorum_set_size,
            desired_threshold,
        )),
        SimpleGraph {
            graph_size,
            relative_threshold,
        } => Rc::new(SimpleGraphQsc::new(
            Graph::new_random_scale_free(graph_size, 2, 2).shuffled(),
            // shuffled because fbas join order shouldn't be correlated with importance in graph
            relative_threshold,
        )),
    }
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let qsc = parse_qscc(args.qscc);
    let monitor = Rc::new(monitors::DebugMonitor::new());

    let mut simulator = Simulator::new(
        Fbas::new(),
        qsc,
        Rc::clone(&monitor) as Rc<dyn SimulationMonitor>,
    );
    simulator.simulate_growth(args.number_of_nodes);
    eprintln!("Starting simulation...");
    let fbas = simulator.finalize();
    eprintln!("Finished simulation, dumping FBAS...");
    println!("{}", fbas.to_json_string_pretty());
    Ok(())
}
