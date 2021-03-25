extern crate fbas_analyzer;
use fbas_analyzer::*;

extern crate csv;
extern crate serde;

use quicli::prelude::*;
use structopt::StructOpt;

use csv::{Reader, Writer};
use std::io;

use std::error::Error;
use std::path::{Path, PathBuf};

use par_map::ParMap;

use std::collections::BTreeMap;

/// Measure analysis duration for increasingly bigger FBASs.
/// Minimal quorums analysis is done always.
/// Checking for quorum intersection takes negligible time once minimal quorums are found.
#[derive(Debug, StructOpt)]
struct Cli {
    /// Output CSV file (will output to STDOUT if omitted).
    #[structopt(short = "o", long = "out")]
    output_path: Option<PathBuf>,

    /// Largest FBAS to analyze, measured in number of top-tier nodes.
    #[structopt(short = "m", long = "max-top-tier-size")]
    max_top_tier_size: usize,

    /// Make FBAS that looks like Stellar's top tier: every 3 top-tier nodes are organized as an
    /// inner_quorum set of the top-tier quorum set.
    #[structopt(long = "stellar-like")]
    stellar_like: bool,

    /// Update output file with missing results (doesn't repeat analyses for existing lines).
    #[structopt(short = "u", long = "update")]
    update: bool,

    /// Number of analysis runs per FBAS size.
    #[structopt(short = "r", long = "runs")]
    runs: usize,

    /// Number of threads to use. Defaults to 1.
    #[structopt(short = "j", long = "jobs", default_value = "1")]
    jobs: usize,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let inputs: Vec<InputDataPoint> =
        generate_inputs(args.max_top_tier_size, args.runs, args.stellar_like);

    let existing_outputs = if args.update {
        load_existing_outputs(&args.output_path)?
    } else {
        BTreeMap::new()
    };

    let tasks = make_sorted_tasklist(inputs, existing_outputs);

    let output_iterator = bulk_do(tasks, args.jobs, args.stellar_like);

    write_csv(output_iterator, &args.output_path, args.update)?;
    Ok(())
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
struct InputDataPoint {
    top_tier_size: usize,
    run: usize,
}
impl InputDataPoint {
    fn from_output_data_point(d: &OutputDataPoint) -> Self {
        Self {
            top_tier_size: d.top_tier_size,
            run: d.run,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutputDataPoint {
    top_tier_size: usize,
    run: usize,
    mq_number: usize,
    mbs_number: usize,
    mss_number: usize,
    mq_mean: f64,
    mbs_mean: f64,
    mss_mean: f64,
    analysis_duration_mq: f64,
    analysis_duration_mbs: f64,
    analysis_duration_mss: f64,
    analysis_duration_hqi_after_mq: f64,
    analysis_duration_hqi_alt_check: f64,
    analysis_duration_total: f64,
}
#[derive(Debug)]
enum Task {
    Reuse(OutputDataPoint),
    Analyze(InputDataPoint),
}
use Task::*;
impl Task {
    fn label(&self) -> usize {
        match self {
            Reuse(output) => output.top_tier_size,
            Analyze(input) => input.top_tier_size,
        }
    }
}

fn generate_inputs(
    max_top_tier_size: usize,
    runs: usize,
    make_stellarlike_fbas: bool,
) -> Vec<InputDataPoint> {
    let mut inputs = vec![];
    for top_tier_size in (1..max_top_tier_size + 1).filter(|m| !make_stellarlike_fbas || m % 3 == 0)
    {
        for run in 0..runs {
            inputs.push(InputDataPoint { top_tier_size, run });
        }
    }
    inputs
}

fn load_existing_outputs(
    path: &Option<PathBuf>,
) -> Result<BTreeMap<InputDataPoint, OutputDataPoint>, Box<dyn Error>> {
    if let Some(path) = path {
        let data_points = read_csv_from_file(path)?;
        let data_points_map = data_points
            .into_iter()
            .map(|d| (InputDataPoint::from_output_data_point(&d), d))
            .collect();
        Ok(data_points_map)
    } else {
        Ok(BTreeMap::new())
    }
}

fn make_sorted_tasklist(
    inputs: Vec<InputDataPoint>,
    existing_outputs: BTreeMap<InputDataPoint, OutputDataPoint>,
) -> Vec<Task> {
    let mut tasks: Vec<Task> = inputs
        .into_iter()
        .filter_map(|input| {
            if !existing_outputs.contains_key(&input) {
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

fn bulk_do(
    tasks: Vec<Task>,
    jobs: usize,
    make_stellarlike_fbas: bool,
) -> impl Iterator<Item = OutputDataPoint> {
    tasks
        .into_iter()
        .with_nb_threads(jobs)
        .par_map(move |task| analyze_or_reuse(task, make_stellarlike_fbas))
}
fn analyze_or_reuse(task: Task, make_stellarlike_fbas: bool) -> OutputDataPoint {
    match task {
        Task::Reuse(output) => {
            eprintln!(
                "Reusing existing analysis results for m={}, run={}.",
                output.top_tier_size, output.run
            );
            output
        }
        Task::Analyze(input) => analyze(input, make_stellarlike_fbas),
    }
}
fn analyze(input: InputDataPoint, make_stellarlike_fbas: bool) -> OutputDataPoint {
    let fbas = if make_stellarlike_fbas {
        make_almost_ideal_stellarlike_fbas(input.top_tier_size)
    } else {
        make_almost_ideal_fbas(input.top_tier_size)
    };
    assert!(fbas.number_of_nodes() == input.top_tier_size);
    let (result_without_total_duration, analysis_duration_total) = timed_secs!({
        let analysis = Analysis::new(&fbas);

        let top_tier_size = input.top_tier_size;
        let run = input.run;

        let ((mq_number, mq_mean), analysis_duration_mq) = timed_secs!({
            let mq = analysis.minimal_quorums();
            (mq.len(), mq.mean())
        });

        let (_, analysis_duration_hqi_after_mq) =
            timed_secs!(assert!(analysis.has_quorum_intersection()));
        let (_, analysis_duration_hqi_alt_check) = timed_secs!(assert_eq!(
            (true, None),
            analysis.has_quorum_intersection_via_alternative_check()
        ));

        let ((mbs_number, mbs_mean), analysis_duration_mbs) = timed_secs!({
            let mbs = analysis.minimal_blocking_sets();
            (mbs.len(), mbs.mean())
        });

        let ((mss_number, mss_mean), analysis_duration_mss) = timed_secs!({
            let mss = analysis.minimal_splitting_sets();
            (mss.len(), mss.mean())
        });

        OutputDataPoint {
            top_tier_size,
            run,
            mq_number,
            mbs_number,
            mss_number,
            mq_mean,
            mbs_mean,
            mss_mean,
            analysis_duration_mq,
            analysis_duration_mbs,
            analysis_duration_mss,
            analysis_duration_hqi_after_mq,
            analysis_duration_hqi_alt_check,
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

fn make_almost_ideal_fbas(top_tier_size: usize) -> Fbas {
    let mut quorum_set = QuorumSet {
        validators: (0..top_tier_size).collect(),
        threshold: simulation::qsc::calculate_67p_threshold(top_tier_size),
        inner_quorum_sets: vec![],
    };
    let mut fbas = Fbas::new();
    for _ in 0..top_tier_size {
        fbas.add_generic_node(quorum_set.clone());
    }

    // we change node 0's quorum set slightly, with 0 implications for analysis except that
    // symmetric top tier optimizations won't be triggered
    quorum_set.validators.push(0);
    quorum_set.threshold += 1;
    fbas.swap_quorum_set(0, quorum_set);

    fbas
}

fn make_almost_ideal_stellarlike_fbas(top_tier_size: usize) -> Fbas {
    assert!(
        top_tier_size % 3 == 0,
        "Nodes in the Stellar network top tier always come in groups of (at least) 3..."
    );
    let mut quorum_set = QuorumSet::new();
    for org_id in 0..top_tier_size / 3 {
        let validators = vec![org_id * 3, org_id * 3 + 1, org_id * 3 + 2];
        quorum_set.inner_quorum_sets.push(QuorumSet {
            validators,
            threshold: 2,
            inner_quorum_sets: vec![],
        });
    }
    quorum_set.threshold = simulation::qsc::calculate_67p_threshold(top_tier_size / 3);
    let mut fbas = Fbas::new();
    for _ in 0..top_tier_size {
        fbas.add_generic_node(quorum_set.clone());
    }

    // we change node 0's quorum set slightly, with 0 implications for analysis except that
    // symmetric top tier optimizations won't be triggered
    quorum_set.validators.push(0);
    quorum_set.threshold += 1;
    fbas.swap_quorum_set(0, quorum_set);

    fbas
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
