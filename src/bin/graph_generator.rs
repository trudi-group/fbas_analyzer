extern crate fbas_analyzer;
use fbas_analyzer::simulation::Graph;

use std::path::PathBuf;

use quicli::prelude::*;
use structopt::StructOpt;

/// Graph generation algorithm options selected via SUBCOMMAND.
#[derive(Debug, StructOpt)]
struct Cli {
    /// Graph generation algorithm and parametrization
    #[structopt(subcommand)]
    algorithm_config: GraphGenerationAlgorithmConfig,

    /// The path to the file where the graph should be the written to
    /// (will output to STDOUT if omitted).
    #[structopt(short = "o", long = "output")]
    path: Option<PathBuf>,

    /// If passed, node IDs will not be shuffled and it is possible that node degrees are
    /// correlated with the numeric value of node IDs.
    #[structopt(long = "dont-shuffle")]
    dont_shuffle: bool,

    #[structopt(flatten)]
    verbosity: Verbosity,
}
#[derive(Debug, StructOpt)]
pub enum GraphGenerationAlgorithmConfig {
    /// Generates a random scale-free graph using the Barabasi-Albert model.
    /// The required arguments are the number of nodes in the graph, the initial connections per node, and the number of attachments per node.
    BarabasiAlbert { n: usize, m0: usize, m: usize },
    /// Generates a random small world graph using the Watts-Strogatz model. The required
    /// arguments are the number of nodes in the graph, the number of neighbours each node should
    /// have, and the probability of rewiring the neighbours.
    WattsStrogatz { n: usize, k: usize, beta: f64 },
}

pub fn apply_graph_gen_alg(algorithm_config: &GraphGenerationAlgorithmConfig) -> Graph {
    use GraphGenerationAlgorithmConfig::*;
    match algorithm_config {
        BarabasiAlbert { n, m0, m } => Graph::new_random_scale_free(*n, *m0, *m),
        WattsStrogatz { n, k, beta } => Graph::new_random_small_world(*n, *k, *beta),
    }
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("graph_generator")?;

    let path = args.path;
    let algorithm_config = args.algorithm_config;
    let dont_shuffle = args.dont_shuffle;

    let graph = if dont_shuffle {
        apply_graph_gen_alg(&algorithm_config)
    } else {
        apply_graph_gen_alg(&algorithm_config).shuffled()
    };

    let head_comment = format!("Graph generated using {:?}", &algorithm_config);
    if let Some(is_path) = &path {
        Graph::to_as_rel_file(&graph, &is_path, Some(&head_comment))?;
    } else {
        let graph_as_string = Graph::to_as_rel_string(&graph, Some(&head_comment)).unwrap();
        eprintln!("Printing graph with {} nodes...", graph.number_of_nodes());
        println!("{}", graph_as_string);
    };
    Ok(())
}
