extern crate fbas_analyzer;

use fbas_analyzer::*;

use quicli::prelude::*;
use structopt::StructOpt;

use std::io::{self, Read};
use std::path::PathBuf;
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
    SimpleRandom { desired_quorum_set_size: usize },
    /// Creates random quorum sets of the given size and threshold. The probability of picking a
    /// node as a validator is weighted by that node's degree in a scale free graph ("famousness")
    /// If threshold is ommitted, uses as 67% threshold as in "Ideal".
    FameWeightedRandom {
        desired_quorum_set_size: usize,
        graph_data_path: PathBuf,
        desired_threshold: Option<usize>,
    },
    /// Chooses quorum sets based on a relative threshold. All graph neighbors
    /// are validators, independent of node existence, quorum intersection or
    /// anything else.
    /// If threshold is ommitted, uses as 67% threshold as in "Ideal".
    SimpleQsc {
        graph_data_path: PathBuf,
        relative_threshold: Option<f64>,
    },
    /// Docstring -> TODO
    HigherTierQsc {
        graph_data_path: PathBuf,
        make_symmetric_top_tier: bool,
        relative_threshold: Option<f64>,
    },
    /// Docstring -> TODO
    GlobalRankASGraph {
        graph_data_path: PathBuf,
        relative_threshold: Option<f64>,
    },
    /// TODO - might be removed again soon
    QualityAware { graph_data_path: PathBuf },
}

fn parse_graph_path(graph_data_path: PathBuf) -> (Graph, usize) {
    let piped = graph_data_path.to_str().unwrap();
    let graph = if piped == "-" {
        eprintln!("Reading graph from STDIN...");
        let mut buf = String::new();
        io::stdin()
            .read_to_string(&mut buf)
            .expect("Error reading from STDIN");
        Graph::from_as_rel_string(&buf)
    } else {
        eprintln!("Reading graph from file...");
        Graph::from_as_rel_file(&graph_data_path)
    };

    eprintln!("Read graph with {} nodes.", graph.number_of_nodes());
    let nr_of_nodes = &graph.number_of_nodes();
    (graph, *nr_of_nodes)
}

fn parse_qscc(qscc: QuorumSetConfiguratorConfig) -> (Rc<dyn QuorumSetConfigurator>, usize) {
    use quorum_set_configurators::*;
    use QuorumSetConfiguratorConfig::*;
    match qscc {
        SuperSafe => (Rc::new(SuperSafeQsc::new()), 0),
        Ideal => (Rc::new(IdealQsc::new()), 0),
        SimpleRandom {
            desired_quorum_set_size,
        } => (Rc::new(RandomQsc::new_simple(desired_quorum_set_size)), 0),
        FameWeightedRandom {
            desired_quorum_set_size,
            desired_threshold,
            graph_data_path,
        } => {
            let (graph, nodes) = parse_graph_path(graph_data_path);
            (
                Rc::new(RandomQsc::new(
                    desired_quorum_set_size,
                    desired_threshold,
                    Some(graph.get_in_degrees()),
                )),
                nodes,
            )
        }
        SimpleQsc {
            relative_threshold,
            graph_data_path,
        } => {
            let (graph, nodes) = parse_graph_path(graph_data_path);
            (
                Rc::new(SimpleGraphQsc::new(graph, relative_threshold)),
                nodes,
            )
        }
        HigherTierQsc {
            graph_data_path,
            make_symmetric_top_tier,
            relative_threshold,
        } => {
            let (graph, nodes) = parse_graph_path(graph_data_path);
            (
                Rc::new(HigherTiersGraphQsc::new(
                    graph,
                    relative_threshold,
                    make_symmetric_top_tier,
                )),
                nodes,
            )
        }
        GlobalRankASGraph {
            graph_data_path,
            relative_threshold,
        } => {
            let (graph, nodes) = parse_graph_path(graph_data_path);
            (
                Rc::new(GlobalRankGraphQsc::new(graph, relative_threshold)),
                nodes,
            )
        }
        QualityAware { graph_data_path } => {
            let (graph, nodes) = parse_graph_path(graph_data_path);
            (Rc::new(QualityAwareGraphQsc::new(graph)), nodes)
        }
    }
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let (qsc, nodes_in_graph) = parse_qscc(args.qscc);

    let initial_n = if args.initial_n > 0 || args.grow_by_n > 0 {
        args.initial_n
    } else {
        nodes_in_graph
    };
    let grow_by_n = args.grow_by_n;

    let monitor = Rc::new(monitors::DebugMonitor::new());

    let mut simulator = Simulator::new(
        Fbas::new_generic_unconfigured(initial_n),
        qsc,
        Rc::clone(&monitor) as Rc<dyn SimulationMonitor>,
    );
    eprintln!("Starting simulation...");
    simulator.simulate_global_reevaluation(initial_n);
    simulator.simulate_growth(grow_by_n);
    let fbas = simulator.finalize();
    eprintln!("Finished simulation, dumping FBAS...");
    println!("{}", fbas.to_json_string_pretty());
    Ok(())
}
