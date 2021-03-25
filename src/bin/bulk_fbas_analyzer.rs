extern crate fbas_analyzer;
use fbas_analyzer::*;

extern crate csv;
extern crate serde;

use quicli::prelude::*;
use structopt::StructOpt;

use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};

use csv::{Reader, Writer};
use par_map::ParMap;
use sha3::{Digest, Sha3_256};

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

    /// Number of threads to use. Defaults to 1.
    #[structopt(short = "j", long = "jobs", default_value = "1")]
    jobs: usize,

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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct OutputDataPoint {
    label: String,
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
    orgs_top_tier_size: Option<usize>,
    orgs_mbs_min: Option<usize>,
    orgs_mbs_max: Option<usize>,
    orgs_mbs_mean: Option<f64>,
    orgs_mss_min: Option<usize>,
    orgs_mss_max: Option<usize>,
    orgs_mss_mean: Option<f64>,
    orgs_mq_min: Option<usize>,
    orgs_mq_max: Option<usize>,
    orgs_mq_mean: Option<f64>,
    isps_top_tier_size: Option<usize>,
    isps_mbs_min: Option<usize>,
    isps_mbs_max: Option<usize>,
    isps_mbs_mean: Option<f64>,
    isps_mss_min: Option<usize>,
    isps_mss_max: Option<usize>,
    isps_mss_mean: Option<f64>,
    isps_mq_min: Option<usize>,
    isps_mq_max: Option<usize>,
    isps_mq_mean: Option<f64>,
    ctries_top_tier_size: Option<usize>,
    ctries_mbs_min: Option<usize>,
    ctries_mbs_max: Option<usize>,
    ctries_mbs_mean: Option<f64>,
    ctries_mss_min: Option<usize>,
    ctries_mss_max: Option<usize>,
    ctries_mss_mean: Option<f64>,
    ctries_mq_min: Option<usize>,
    ctries_mq_max: Option<usize>,
    ctries_mq_mean: Option<f64>,
    standard_form_hash: String,
    analysis_duration_mq: f64,
    analysis_duration_mbs: f64,
    analysis_duration_mss: f64,
    analysis_duration_total: f64,
}
type AnalysisResults = (
    Option<usize>,
    Option<usize>,
    Option<usize>,
    Option<f64>,
    Option<usize>,
    Option<usize>,
    Option<f64>,
    Option<usize>,
    Option<usize>,
    Option<f64>,
);
macro_rules! extend_output_data_point {
    ($output: tt, $($id: ident: $e: expr), *) => {{
        OutputDataPoint {
            $($id: $e), * ,
            ..$output
        }
    }};
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
        .filter_map(|input| {
            if !existing_outputs.contains_key(&input.label) {
                Some(Analyze(input))
            } else {
                None
            }
        })
        .chain(existing_outputs.values().cloned().map(Reuse))
        .collect();
    tasks.sort_by_cached_key(|t| t.label());
    tasks
}

fn bulk_do(tasks: Vec<Task>, jobs: usize) -> impl Iterator<Item = OutputDataPoint> {
    tasks
        .into_iter()
        .with_nb_threads(jobs)
        .par_map(analyze_or_reuse)
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
        let isps = maybe_load_isps(&input.nodes_path, &fbas);
        let countries = maybe_load_countries(&input.nodes_path, &fbas);
        let analysis = Analysis::new(&fbas);

        let label = input.label.clone();

        let ((mq_min, mq_max, mq_mean), analysis_duration_mq) =
            timed_secs!(analysis.minimal_quorums().minmaxmean());

        let has_quorum_intersection = analysis.has_quorum_intersection();
        let top_tier_size = analysis.top_tier().len();

        let ((mbs_min, mbs_max, mbs_mean), analysis_duration_mbs) =
            timed_secs!(analysis.minimal_blocking_sets().minmaxmean());

        let ((mss_min, mss_max, mss_mean), analysis_duration_mss) =
            timed_secs!(analysis.minimal_splitting_sets().minmaxmean());

        let orgs_output = maybe_merge_sets(&analysis, organizations);
        let isps_output = maybe_merge_sets(&analysis, isps);
        let ctries_output = maybe_merge_sets(&analysis, countries);
        let standard_form_hash = hex::encode(Sha3_256::digest(
            &fbas.to_standard_form().to_json_string().into_bytes(),
        ));
        let mut output = OutputDataPoint {
            label,
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
            ..Default::default()
        };
        output = extend_output_with_orgs_results(&orgs_output, output);
        output = extend_output_with_isps_results(&isps_output, output);
        output = extend_output_with_ctries_results(&ctries_output, output);
        OutputDataPoint {
            standard_form_hash,
            analysis_duration_mq,
            analysis_duration_mbs,
            analysis_duration_mss,
            analysis_duration_total: 0.0,
            ..output
        }
    });
    OutputDataPoint {
        analysis_duration_total,
        ..result_without_total_duration
    }
}

fn maybe_merge_sets(analysis: &Analysis, grouping: Option<Groupings>) -> AnalysisResults {
    if let Some(ref group) = grouping {
        let merge_fix = |sets: NodeIdSetVecResult| {
            let (min, max, mean) = sets.merged_by_group(group).minimal_sets().minmaxmean();
            (Some(min), Some(max), Some(mean))
        };
        let (
            top_tier_size,
            (mq_min, mq_max, mq_mean),
            (mbs_min, mbs_max, mbs_mean),
            (mss_min, mss_max, mss_mean),
        ) = (
            Some(analysis.top_tier().merged_by_group(group).len()),
            merge_fix(analysis.minimal_quorums()),
            merge_fix(analysis.minimal_blocking_sets()),
            merge_fix(analysis.minimal_splitting_sets()),
        );
        (
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
        )
    } else {
        (None, None, None, None, None, None, None, None, None, None)
    }
}

fn extend_output_with_orgs_results(
    orgs: &AnalysisResults,
    output: OutputDataPoint,
) -> OutputDataPoint {
    extend_output_data_point!(output, orgs_top_tier_size: orgs.0,
            orgs_mbs_min: orgs.1, orgs_mbs_max: orgs.2, orgs_mbs_mean: orgs.3,
            orgs_mss_min: orgs.4, orgs_mss_max: orgs.5, orgs_mss_mean: orgs.6,
            orgs_mq_min: orgs.7, orgs_mq_max: orgs.8, orgs_mq_mean: orgs.9)
}
fn extend_output_with_isps_results(
    isps: &AnalysisResults,
    output: OutputDataPoint,
) -> OutputDataPoint {
    extend_output_data_point!(output, isps_top_tier_size: isps.0,
            isps_mbs_min: isps.1, isps_mbs_max: isps.2, isps_mbs_mean: isps.3,
            isps_mss_min: isps.4, isps_mss_max: isps.5, isps_mss_mean: isps.6,
            isps_mq_min: isps.7, isps_mq_max: isps.8, isps_mq_mean: isps.9)
}
fn extend_output_with_ctries_results(
    ctries: &AnalysisResults,
    output: OutputDataPoint,
) -> OutputDataPoint {
    extend_output_data_point!(output, ctries_top_tier_size: ctries.0,
            ctries_mbs_min: ctries.1, ctries_mbs_max: ctries.2, ctries_mbs_mean:
            ctries.3, ctries_mss_min: ctries.4, ctries_mss_max: ctries.5,
            ctries_mss_mean: ctries.6, ctries_mq_min: ctries.7, ctries_mq_max:
            ctries.8, ctries_mq_mean: ctries.9)
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
fn extract_file_name(path: &Path) -> String {
    path.file_name()
        .unwrap()
        .to_os_string()
        .into_string()
        .unwrap()
}
fn extract_label(path: &Path, substring_to_ignore_for_label: &str) -> String {
    let ignore_list = vec!["nodes", "organizations", substring_to_ignore_for_label];
    let label_parts: Vec<String> = extract_file_name(&path)
        .replace(".json", "")
        .split_terminator('_')
        .filter(|s| !ignore_list.contains(s))
        .map(|s| s.to_string())
        .collect();
    label_parts.join("_")
}

fn load_fbas(nodes_path: &Path) -> Fbas {
    Fbas::from_json_file(nodes_path)
}
fn maybe_load_organizations<'a>(
    o_organizations_path: Option<&PathBuf>,
    fbas: &'a Fbas,
) -> Option<Groupings<'a>> {
    if let Some(organizations_path) = o_organizations_path {
        Some(Groupings::organizations_from_json_file(
            organizations_path,
            fbas,
        ))
    } else {
        None
    }
}
fn maybe_load_isps<'a>(nodes_path: &Path, fbas: &'a Fbas) -> Option<Groupings<'a>> {
    let isps = Groupings::isps_from_json_file(nodes_path, &fbas);
    if isps.number_of_groupings() != 0 {
        Some(isps)
    } else {
        None
    }
}
fn maybe_load_countries<'a>(nodes_path: &Path, fbas: &'a Fbas) -> Option<Groupings<'a>> {
    let countries = Groupings::countries_from_json_file(nodes_path, &fbas);
    if countries.number_of_groupings() != 0 {
        Some(countries)
    } else {
        None
    }
}

fn read_csv_from_file(path: &Path) -> Result<Vec<OutputDataPoint>, Box<dyn Error>> {
    let mut reader = Reader::from_path(path)?;
    let mut result = vec![];
    for line in reader.deserialize() {
        result.push(line?);
    }
    Ok(result)
}
fn write_csv_to_file(
    data_points: impl IntoIterator<Item = impl serde::Serialize>,
    path: &Path,
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
