extern crate fbas_analyzer;
use fbas_analyzer::*;

extern crate csv;
extern crate serde;

use quicli::prelude::*;
use structopt::StructOpt;

use csv::Writer;
use std::io;

use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

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

    #[structopt(short = "i", long = "ignore-for-label", default_value = "stellarbeat")]
    ignore_for_label: String,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let mut inputs: Vec<InputDataPoint> =
        extract_inputs(&args.input_paths, &args.ignore_for_label)?;
    inputs.sort();

    let outputs = inputs.into_iter().par_map(analyze);

    write_csv(outputs, args.output_path)?;
    Ok(())
}

#[derive(Debug, Eq, PartialEq, PartialOrd)]
struct InputDataPoint {
    label: String,
    nodes_path: PathBuf,
    organizations_path: Option<PathBuf>,
}
impl Ord for InputDataPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.label.cmp(&other.label)
    }
}

#[derive(Debug, Serialize)]
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
    analysis_duration: f64,
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

fn analyze(input: InputDataPoint) -> OutputDataPoint {
    let time_measurement_start = Instant::now();

    let fbas = load_fbas(&input.nodes_path);
    let organizations = maybe_load_organizations(input.organizations_path.as_ref(), &fbas);
    let mut analysis = Analysis::new(&fbas, organizations.as_ref());

    let label = input.label.clone();
    let merged_by_organizations = input.organizations_path.is_some();
    let has_quorum_intersection = analysis.has_quorum_intersection();
    let top_tier_size = analysis.top_tier().len();

    let (mbs_min, mbs_max, mbs_mean) = analysis.minimal_blocking_sets().minmaxmean();
    let (mss_min, mss_max, mss_mean) = analysis.minimal_splitting_sets().minmaxmean();
    let (mq_min, mq_max, mq_mean) = analysis.minimal_quorums().minmaxmean();

    let analysis_duration = time_measurement_start.elapsed().as_secs_f64();

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
        analysis_duration,
    }
}

fn write_csv(
    data_points: impl IntoIterator<Item = impl serde::Serialize>,
    output_path: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    if let Some(path) = output_path {
        if path.exists() {
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

fn write_csv_to_file(
    data_points: impl IntoIterator<Item = impl serde::Serialize>,
    path: PathBuf,
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
