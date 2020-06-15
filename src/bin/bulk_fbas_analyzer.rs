extern crate fbas_analyzer;
use fbas_analyzer::*;

extern crate csv;
extern crate serde;

use quicli::prelude::*;
use structopt::StructOpt;

use csv::{Reader, Writer};
use std::io;

use std::error::Error;
use std::path::PathBuf;

use par_map::ParMap;

use std::collections::BTreeMap;

/// Bulk analyze multiple FBASs (in stellarbeat.org JSON format)
#[derive(Debug, StructOpt)]
struct Cli {
    /// Paths to JSON files describing FBASs and organizations in stellarbeat.org "nodes" format.
    /// Files folowing the naming scheme `(X_)organizations(_Y).json` are interpreted as
    /// organizations for a `X_Y.json` or `X_nodes_Y.json` file with matching `X`/`Y` contents.
    /// A data point label is extracted from the file name by removing `(_)nodes(_)` and
    /// `(_)organizations(_)` substrings and the string supplied as `--ignore-for-label`
    /// (e.g., `2020-06-03_stellarbeat_nodes.json` gets the label `2020-06-03`).
    input_paths: Vec<PathBuf>,

    /// Output CSV file (will output to STDOUT if omitted)
    #[structopt(short = "o", long = "out")]
    output_path: Option<PathBuf>,

    /// Update output file with missing results (doesn't repeat analyses for existing results).
    #[structopt(short = "u", long = "update")]
    update: bool,

    /// Filter out this string when constructing data point labels from file names.
    #[structopt(short = "i", long = "ignore-for-label", default_value = "stellarbeat")]
    ignore_for_label: String,

    /// Number of threads to use. Defaults to number of CPUs available.
    #[structopt(short = "j", long = "jobs")]
    jobs: Option<usize>,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let inputs: Vec<InputDataPoint> = extract_inputs(&args.input_paths, &args.ignore_for_label)?;

    let existing_outputs = if args.update {
        load_existing_outputs(&args.output_path)?
    } else {
        BTreeMap::new()
    };

    let tasks = make_sorted_tasklist(inputs, existing_outputs);

    let output_iterator = bulk_do(tasks, args.jobs);

    write_csv(output_iterator, &args.output_path, args.update)?;
    Ok(())
}

#[derive(Debug)]
struct InputDataPoint {
    label: String,
    nodes_path: PathBuf,
    organizations_path: Option<PathBuf>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutputDataPoint {
    label: String,
    merged_by_organizations: bool,
    has_quorum_intersection: bool,
    top_tier_size: usize,
    mbs_min: usize,
    mbs_max: usize,
    mbs_mean: f64,
    mss_min: usize,
    mss_max: usize,
    mss_mean: f64,
    mq_min: usize,
    mq_max: usize,
    mq_mean: f64,
    analysis_duration_mq: f64,
    analysis_duration_mbs: f64,
    analysis_duration_mss: f64,
    analysis_duration_total: f64,
}
#[derive(Debug)]
enum Task {
    Reuse(OutputDataPoint),
    Analyze(InputDataPoint),
}
use Task::*;
impl Task {
    fn label(&self) -> String {
        match self {
            Reuse(output) => output.label.clone(),
            Analyze(input) => input.label.clone(),
        }
    }
}

fn extract_inputs(
    input_paths: &[PathBuf],
    substring_to_ignore_for_label: &str,
) -> Result<Vec<InputDataPoint>, io::Error> {
    let nodes_paths = extract_nodes_paths(input_paths);
    let organizations_paths_by_label =
        extract_organizations_paths_by_label(input_paths, substring_to_ignore_for_label);

    if nodes_paths.len() + organizations_paths_by_label.keys().len() < input_paths.len() {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Some input files could not be recognized based on their file name; \
                input file names must end with `.json`.",
        ))
    } else {
        Ok(build_inputs(
            nodes_paths,
            organizations_paths_by_label,
            substring_to_ignore_for_label,
        ))
    }
}

fn load_existing_outputs(
    path: &Option<PathBuf>,
) -> Result<BTreeMap<String, OutputDataPoint>, Box<dyn Error>> {
    if let Some(path) = path {
        let data_points = read_csv_from_file(path)?;
        let data_points_map = data_points
            .into_iter()
            .map(|d| (d.label.clone(), d))
            .collect();
        Ok(data_points_map)
    } else {
        Ok(BTreeMap::new())
    }
}

fn make_sorted_tasklist(
    inputs: Vec<InputDataPoint>,
    existing_outputs: BTreeMap<String, OutputDataPoint>,
) -> Vec<Task> {
    let mut tasks: Vec<Task> = inputs
        .into_iter()
        .map(|input| {
            if let Some(output) = existing_outputs.get(&input.label) {
                Reuse(output.clone())
            } else {
                Analyze(input)
            }
        })
        .collect();
    tasks.sort_by_cached_key(|t| t.label());
    tasks
}

fn bulk_do(
    tasks: Vec<Task>,
    number_of_threads: Option<usize>,
) -> impl Iterator<Item = OutputDataPoint> {
    if let Some(n) = number_of_threads {
        tasks
            .into_iter()
            .with_nb_threads(n)
            .par_map(analyze_or_reuse)
    } else {
        tasks.into_iter().par_map(analyze_or_reuse)
    }
}
fn analyze_or_reuse(task: Task) -> OutputDataPoint {
    match task {
        Task::Reuse(output) => {
            eprintln!("Reusing existing analysis results for {}.", output.label);
            output
        }
        Task::Analyze(input) => analyze(input),
    }
}
fn analyze(input: InputDataPoint) -> OutputDataPoint {
    let (result_without_total_duration, analysis_duration_total) = timed_secs!({
        let fbas = load_fbas(&input.nodes_path);
        let organizations = maybe_load_organizations(input.organizations_path.as_ref(), &fbas);
        let analysis = Analysis::new(&fbas, organizations.as_ref());

        let label = input.label.clone();
        let merged_by_organizations = input.organizations_path.is_some();

        let ((mq_min, mq_max, mq_mean), analysis_duration_mq) =
            timed_secs!(analysis.minimal_quorums().minmaxmean());

        let has_quorum_intersection = analysis.has_quorum_intersection();
        let top_tier_size = analysis.top_tier().len();

        let ((mbs_min, mbs_max, mbs_mean), analysis_duration_mbs) =
            timed_secs!(analysis.minimal_blocking_sets().minmaxmean());

        let ((mss_min, mss_max, mss_mean), analysis_duration_mss) =
            timed_secs!(analysis.minimal_splitting_sets().minmaxmean());

        OutputDataPoint {
            label,
            merged_by_organizations,
            has_quorum_intersection,
            top_tier_size,
            mbs_min,
            mbs_max,
            mbs_mean,
            mss_min,
            mss_max,
            mss_mean,
            mq_min,
            mq_max,
            mq_mean,
            analysis_duration_mq,
            analysis_duration_mbs,
            analysis_duration_mss,
            analysis_duration_total: 0.,
        }
    });
    OutputDataPoint {
        analysis_duration_total,
        ..result_without_total_duration
    }
}

fn write_csv(
    data_points: impl IntoIterator<Item = impl serde::Serialize>,
    output_path: &Option<PathBuf>,
    overwrite_allowed: bool,
) -> Result<(), Box<dyn Error>> {
    if let Some(path) = output_path {
        if !overwrite_allowed && path.exists() {
            Err(Box::new(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Output file exists, refusing to overwrite.",
            )))
        } else {
            write_csv_to_file(data_points, path)
        }
    } else {
        write_csv_to_stdout(data_points)
    }
}

fn extract_nodes_paths(input_paths: &[PathBuf]) -> Vec<PathBuf> {
    input_paths
        .iter()
        .filter(|&p| extract_file_name(p).ends_with(".json"))
        .filter(|&p| !extract_file_name(p).contains("organizations"))
        .cloned()
        .collect()
}
fn extract_organizations_paths_by_label(
    input_paths: &[PathBuf],
    substring_to_ignore_for_label: &str,
) -> BTreeMap<String, PathBuf> {
    input_paths
        .iter()
        .filter(|&p| extract_file_name(p).ends_with(".json"))
        .filter(|&p| extract_file_name(p).contains("organizations"))
        .map(|p| (extract_label(&p, substring_to_ignore_for_label), p.clone()))
        .collect()
}
fn build_inputs(
    nodes_paths: Vec<PathBuf>,
    organizations_paths_by_label: BTreeMap<String, PathBuf>,
    substring_to_ignore_for_label: &str,
) -> Vec<InputDataPoint> {
    nodes_paths
        .into_iter()
        .map(|p| {
            let label = extract_label(&p, substring_to_ignore_for_label);
            let nodes_path = p;
            let organizations_path = organizations_paths_by_label.get(&label).cloned();
            InputDataPoint {
                label,
                nodes_path,
                organizations_path,
            }
        })
        .collect()
}
fn extract_file_name(path: &PathBuf) -> String {
    path.file_name()
        .unwrap()
        .to_os_string()
        .into_string()
        .unwrap()
}
fn extract_label(path: &PathBuf, substring_to_ignore_for_label: &str) -> String {
    let ignore_list = vec!["nodes", "organizations", substring_to_ignore_for_label];
    let label_parts: Vec<String> = extract_file_name(&path)
        .replace(".json", "")
        .split_terminator('_')
        .filter(|s| !ignore_list.contains(s))
        .map(|s| s.to_string())
        .collect();
    label_parts.join("_")
}

fn load_fbas(nodes_path: &PathBuf) -> Fbas {
    Fbas::from_json_file(nodes_path)
}
fn maybe_load_organizations<'a>(
    o_organizations_path: Option<&PathBuf>,
    fbas: &'a Fbas,
) -> Option<Organizations<'a>> {
    if let Some(organizations_path) = o_organizations_path {
        Some(Organizations::from_json_file(organizations_path, fbas))
    } else {
        None
    }
}

fn read_csv_from_file(path: &PathBuf) -> Result<Vec<OutputDataPoint>, Box<dyn Error>> {
    let mut reader = Reader::from_path(path)?;
    let mut result = vec![];
    for line in reader.deserialize() {
        result.push(line?);
    }
    Ok(result)
}
fn write_csv_to_file(
    data_points: impl IntoIterator<Item = impl serde::Serialize>,
    path: &PathBuf,
) -> Result<(), Box<dyn Error>> {
    let writer = Writer::from_path(path)?;
    write_csv_via_writer(data_points, writer)
}
fn write_csv_to_stdout(
    data_points: impl IntoIterator<Item = impl serde::Serialize>,
) -> Result<(), Box<dyn Error>> {
    let writer = Writer::from_writer(io::stdout());
    write_csv_via_writer(data_points, writer)
}
fn write_csv_via_writer(
    data_points: impl IntoIterator<Item = impl serde::Serialize>,
    mut writer: Writer<impl io::Write>,
) -> Result<(), Box<dyn Error>> {
    for data_point in data_points.into_iter() {
        writer.serialize(data_point)?;
        writer.flush()?;
    }
    Ok(())
}
