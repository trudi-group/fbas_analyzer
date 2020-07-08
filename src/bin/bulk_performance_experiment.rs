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
use std::rc::Rc;

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

    /// Largest FBAS to analyze.
    #[structopt(short = "m", long = "max-top-tier-size")]
    max_top_tier_size: usize,

    /// Update output file with missing results (doesn't repeat analyses for existing lines).
    #[structopt(short = "u", long = "update")]
    update: bool,

    /// Number of analysis runs per FBAS size.
    #[structopt(short = "r", long = "runs")]
    runs: usize,

    /// Number of threads to use. Defaults to 1.
    #[structopt(short = "j", long = "jobs", default_value = "1")]
    jobs: usize,

    /// Do (and time) minimal blocking sets analysis.
    #[structopt(short = "b", long = "minimal-blocking-sets")]
    do_mbs_analysis: bool,

    /// Do (and time) minimal splitting sets analysis.
    #[structopt(short = "s", long = "minimal-splitting-sets")]
    do_mss_analysis: bool,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::from_args();
    args.verbosity.setup_env_logger("fbas_analyzer")?;

    let inputs: Vec<InputDataPoint> = generate_inputs(args.max_top_tier_size, args.runs);

    let existing_outputs = if args.update {
        load_existing_outputs(&args.output_path)?
    } else {
        BTreeMap::new()
    };

    let tasks = make_sorted_tasklist(
        inputs,
        existing_outputs,
        args.do_mbs_analysis,
        args.do_mss_analysis,
    );

    let output_iterator = bulk_do(tasks, args.jobs);

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
    mbs_number: Option<usize>,
    mss_number: Option<usize>,
    mq_mean: f64,
    mbs_mean: Option<f64>,
    mss_mean: Option<f64>,
    analysis_duration_mq: f64,
    analysis_duration_mbs: Option<f64>,
    analysis_duration_mss: Option<f64>,
    analysis_duration_total: f64,
}
#[derive(Debug)]
enum Task {
    Reuse(OutputDataPoint),
    Analyze(InputDataPoint, bool, bool),
}
use Task::*;
impl Task {
    fn label(&self) -> usize {
        match self {
            Reuse(output) => output.top_tier_size,
            Analyze(input, _, _) => input.top_tier_size,
        }
    }
}

fn generate_inputs(max_top_tier_size: usize, runs: usize) -> Vec<InputDataPoint> {
    let mut inputs = vec![];
    for top_tier_size in 1..max_top_tier_size + 1 {
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
    do_mbs_analysis: bool,
    do_mss_analysis: bool,
) -> Vec<Task> {
    let mut tasks: Vec<Task> = inputs
        .into_iter()
        .filter_map(|input| {
            if !existing_outputs.contains_key(&input) {
                Some(Analyze(input, do_mbs_analysis, do_mss_analysis))
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
            eprintln!(
                "Reusing existing analysis results for m={}, run={}.",
                output.top_tier_size, output.run
            );
            output
        }
        Task::Analyze(input, do_mbs_analysis, do_mss_analysis) => {
            analyze(input, do_mbs_analysis, do_mss_analysis)
        }
    }
}
fn analyze(input: InputDataPoint, do_mbs_analysis: bool, do_mss_analysis: bool) -> OutputDataPoint {
    let fbas = make_almost_ideal_fbas(input.top_tier_size);
    let (result_without_total_duration, analysis_duration_total) = timed_secs!({
        let analysis = Analysis::new(&fbas, None);

        let top_tier_size = input.top_tier_size;
        let run = input.run;

        let ((mq_number, mq_mean), analysis_duration_mq) = timed_secs!({
            let mq = analysis.minimal_quorums();
            (mq.len(), mq.mean())
        });

        let (mbs_number, mbs_mean, analysis_duration_mbs) = {
            if do_mbs_analysis {
                let ((mbs_number, mbs_mean), analysis_duration_mbs) = timed_secs!({
                    let mbs = analysis.minimal_blocking_sets();
                    (mbs.len(), mbs.mean())
                });
                (
                    Some(mbs_number),
                    Some(mbs_mean),
                    Some(analysis_duration_mbs),
                )
            } else {
                (None, None, None)
            }
        };

        let (mss_number, mss_mean, analysis_duration_mss) = {
            if do_mss_analysis {
                let ((mss_number, mss_mean), analysis_duration_mss) = timed_secs!({
                    let mss = analysis.minimal_splitting_sets();
                    (mss.len(), mss.mean())
                });
                (
                    Some(mss_number),
                    Some(mss_mean),
                    Some(analysis_duration_mss),
                )
            } else {
                (None, None, None)
            }
        };

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
    let mut simulator = simulation::Simulator::new(
        Fbas::new(),
        Rc::new(simulation::qsc::IdealQsc),
        Rc::new(simulation::monitors::DummyMonitor),
    );
    simulator.simulate_growth(top_tier_size);
    let mut fbas = simulator.finalize();

    // change one quorum set so that symmetric cluster optimisations don't trigger during analysis
    let mut quorum_set = fbas.get_quorum_set(0).unwrap();
    quorum_set.validators.push(0); // doesn't change analysis results; 0 is already a validator
    quorum_set.threshold += 1;
    fbas.swap_quorum_set(0, quorum_set);
    fbas
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
