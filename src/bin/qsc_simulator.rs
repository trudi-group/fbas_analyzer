extern crate fbas_analyzer;

use fbas_analyzer::simulation::*;
use fbas_analyzer::Fbas;

use quicli::prelude::*;
use structopt::StructOpt;

use std::io::{self, Read};
use std::path::PathBuf;
use std::rc::Rc;

/// FBAS quorum set configuration (QSC) simulation sandbox.
/// QSC policies selected via SUBCOMMAND.
#[derive(Debug, StructOpt)]
struct Cli {
    /// Initial size of the simulated FBAS. Default is 0 or, for graph-based QSC policies, the size
    /// of the input graph.
    #[structopt(short = "i", long = "initial", default_value = "0")]
    initial_n: usize,

    /// If set, adds the specified number of nodes via simulated organic growth.
    #[structopt(short = "g", long = "grow-by", default_value = "0")]
    grow_by_n: usize,

    /// Quorum set configuration policy to simulate
    #[structopt(subcommand)]
    qsc_config: QuorumSetConfiguratorConfig,

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
    Random { desired_quorum_set_size: usize },
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
    AllNeighbors {
        graph_data_path: PathBuf,
        relative_threshold: Option<f64>,
    },
    /// Only use neighbors perceived as higher-tier as validators, or only nodes perceived as
    /// same-tier, if there are no higher-tier neighbors.
    HigherTierNeighbors {
        graph_data_path: PathBuf,
        relative_threshold: Option<f64>,
    },
    /// Like `HigherTierNeighbors`, but top-tier nodes mirror each others' quorum sets, turning the
    /// top tier into a symmetric cluster.
    SymmetryEnforcingHigherTierNeighbors {
        graph_data_path: PathBuf,
        relative_threshold: Option<f64>,
    },
    /// Uses all nodes with above-average global rank.
    GlobalRank {
        graph_data_path: PathBuf,
        relative_threshold: Option<f64>,
    },
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

fn parse_qsc_config(
    qsc_config: QuorumSetConfiguratorConfig,
) -> (Rc<dyn QuorumSetConfigurator>, usize) {
    use qsc::*;
    use QuorumSetConfiguratorConfig::*;
    match qsc_config {
        SuperSafe => (Rc::new(SuperSafeQsc::new()), 0),
        Ideal => (Rc::new(IdealQsc::new()), 0),
        Random {
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
        AllNeighbors {
            relative_threshold,
            graph_data_path,
        } => {
            let (graph, nodes) = parse_graph_path(graph_data_path);
            (
                Rc::new(AllNeighborsQsc::new(graph, relative_threshold)),
                nodes,
            )
        }
        HigherTierNeighbors {
            graph_data_path,
            relative_threshold,
        } => {
            let (graph, nodes) = parse_graph_path(graph_data_path);
            (
                Rc::new(HigherTierNeighborsQsc::new(
                    graph,
                    relative_threshold,
                    false,
                )),
                nodes,
            )
        }
        SymmetryEnforcingHigherTierNeighbors {
            graph_data_path,
            relative_threshold,
        } => {
            let (graph, nodes) = parse_graph_path(graph_data_path);
            (
                Rc::new(HigherTierNeighborsQsc::new(graph, relative_threshold, true)),
                nodes,
            )
        }
        GlobalRank {
            graph_data_path,
            relative_threshold,
        } => {
            let (graph, nodes) = parse_graph_path(graph_data_path);
            (
                Rc::new(GlobalRankQsc::new(graph, relative_threshold)),
                nodes,
            )
        }
    }
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let (qsc, nodes_in_graph) = parse_qsc_config(args.qsc_config);

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
