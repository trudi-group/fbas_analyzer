extern crate fbas_analyzer;
use fbas_analyzer::*;

use std::path::{Path, PathBuf};

use quicli::prelude::*;
use structopt::StructOpt;

/// Graph generation algorithm options selected via SUBCOMMAND.
#[derive(Debug, StructOpt)]
struct Cli {
    /// Graph generation strategy to apply
    #[structopt(subcommand)]
    gga: GraphGenerationAlgorithm,

    /// The path to the file where the graph should be the written to
    #[structopt(short = "o", long = "output")]
    path: Option<PathBuf>,

    /// Is set by default and the graph will be printed to stdout in the AS relationship format.
    /// Can be used in combination with writing a graph to a file
    #[structopt(long = "stdout")]
    stdout: bool,

    #[structopt(flatten)]
    verbosity: Verbosity,
}
#[derive(Debug, StructOpt)]
pub enum GraphGenerationAlgorithm {
    /// Generates a random scale-free graph using the Barabasi-Albert model.
    /// The required arguments are the number of nodes in the graph, the initial connections per node, and the number of attachments per node.
    BarabasiAlbert { n: usize, m0: usize, m: usize },
    /// Generates a random small world graph using the Watts-Strogatz model. The required
    /// arguments are the number of nodes in the graph, the number of neighbours each node should
    /// have, and the probability of rewiring the neighbours.
    WattsStrogatz { n: usize, k: usize, beta: f64 },
    /// Reads a graph in the AS relationship format from a file given the path to the file.
    ReadGraphFromFile { file_path: PathBuf },
}

pub fn apply_graph_gen_alg(gga: &GraphGenerationAlgorithm) -> Graph {
    use GraphGenerationAlgorithm::*;
    match gga {
        BarabasiAlbert { n, m0, m } => Graph::new_random_scale_free(*n, *m0, *m),
        WattsStrogatz { n, k, beta } => Graph::new_random_small_world(*n, *k, *beta),
        ReadGraphFromFile { file_path } => Graph::from_as_rel_file(&file_path),
    }
}

pub fn write_graph_to_file(graph: &Graph, path: &Path) -> std::io::Result<()> {
    Graph::to_as_rel_file(graph, path)
}

fn main() -> CliResult {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("graph_generator")?;

    let path = args.path;
    let stdout = args.stdout;
    let gga = args.gga;

    let graph = apply_graph_gen_alg(&gga);

    if stdout || path.is_none() {
        let graph_as_string = fbas_analyzer::Graph::to_as_rel_string(&graph);
        eprintln!("Printing graph with {} nodes...", graph.number_of_nodes());
        println!("Graph generated using {:?}", &gga);
        for line in graph_as_string.iter() {
            println!("{:?}", line);
        }
    } else if path.is_some() {
        write_graph_to_file(&graph, &path.unwrap())?;
    }
    Ok(())
}
