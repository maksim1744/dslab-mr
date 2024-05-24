use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

use clap::Parser;
use dslab_dfs::{
    replication_strategies::random::{ChunkDistribution, RandomReplicationStrategy},
    replication_strategy::ReplicationStrategy,
};
use dslab_mr::{
    experiment::{Experiment, Plan, RunResult},
    placement_strategies::{
        locality_aware::LocalityAwareStrategy, packing_scheduler::PackingScheduler, random::RandomPlacementStrategy,
    },
    placement_strategy::DynamicPlacementStrategy,
    run_stats::RunStats,
    system::SystemConfig,
};
use env_logger::Builder;
use serde::Deserialize;

#[derive(Deserialize)]
struct ConfigPlan {
    plan_path: PathBuf,
    dags_path: PathBuf,
}

#[derive(Deserialize)]
struct Config {
    plans: Vec<ConfigPlan>,
    systems: Vec<PathBuf>,
    replication_strategies: Vec<String>,
    placement_strategies: Vec<String>,
}

/// Runs batch experiment.
#[derive(Parser, Debug)]
struct Args {
    /// Path to config.
    #[arg(short, long)]
    config: PathBuf,

    /// Path to folder with traces.
    #[arg(short, long, default_value = None)]
    traces: Option<PathBuf>,

    /// Path to file with results.
    #[arg(short, long)]
    output: PathBuf,

    /// Do not run experiments, just read results from --output.
    #[arg(long)]
    precalculated: bool,

    /// Number of threads.
    #[arg(long, default_value_t = std::thread::available_parallelism().unwrap().get())]
    threads: usize,
}

fn filename(path: impl AsRef<Path>) -> String {
    path.as_ref()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .split('.')
        .next()
        .unwrap()
        .to_string()
}

fn read_name(name: &str) -> (&str, HashMap<&str, &str>) {
    let open = name.find('[');
    if open.is_none() {
        return (name, HashMap::new());
    }
    (
        &name[..open.unwrap()],
        name[open.unwrap() + 1..name.len() - 1]
            .split(',')
            .map(|s| s.split('=').collect::<Vec<_>>())
            .map(|v| (v[0], v[1]))
            .collect(),
    )
}

fn replication_strategy_resolver(name: &str) -> Box<dyn ReplicationStrategy> {
    let (name, args) = read_name(name);
    match name {
        "Random" => Box::new(RandomReplicationStrategy::new(
            args["replication_factor"].parse().unwrap(),
            match args["chunk_distribution"] {
                "AllowEverything" => ChunkDistribution::AllowEverything,
                "ProhibitSameRack" => ChunkDistribution::ProhibitSameRack,
                x => panic!("Unknownk chunk distribution {}", x),
            },
        )),
        x => panic!("Unkwnon replication strategy {}", x),
    }
}

fn placement_strategy_resolver(name: &str) -> Box<dyn DynamicPlacementStrategy> {
    let (name, args) = read_name(name);
    match name {
        "Random" => Box::new(RandomPlacementStrategy::new()),
        "LocalityAware" => Box::new(LocalityAwareStrategy::new()),
        "PackingScheduler" => Box::new(PackingScheduler::new(
            args["other_rack_input_penalty"].parse().unwrap(),
            args["other_host_input_penalty"].parse().unwrap(),
        )),
        x => panic!("Unkwnon placement strategy {}", x),
    }
}

struct ResultRow {
    name: String,
    avg_slowdown: f64,
    max_slowdown: f64,
    avg_total_slowdown: f64,
    r2r_traffic: f64,
    h2h_traffic: f64,
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();
    let config: Config = serde_yaml::from_str(&std::fs::read_to_string(args.config).expect("Can't read config file"))
        .expect("Can't parse config file");

    let result: Vec<RunResult> = if args.precalculated {
        serde_json::from_str(&std::fs::read_to_string(args.output).expect("Can't read file with result"))
            .expect("Can't parse file with result")
    } else {
        let experiment = Experiment::new(
            123,
            config
                .plans
                .into_iter()
                .enumerate()
                .map(|(i, plan)| Plan {
                    name: format!("{}_{}", i, filename(&plan.plan_path)),
                    plan_path: plan.plan_path,
                    dags_path: plan.dags_path,
                })
                .collect(),
            config
                .systems
                .into_iter()
                .enumerate()
                .map(|(i, path)| (format!("{}_{}", i, filename(&path)), SystemConfig::from_yaml(path)))
                .collect(),
            config.replication_strategies,
            config.placement_strategies,
            replication_strategy_resolver,
            placement_strategy_resolver,
            args.traces,
        );

        let result = experiment.run(args.threads);
        File::create(args.output)
            .expect("Can't create output file")
            .write_all(serde_json::to_string_pretty(&result).unwrap().as_bytes())
            .expect("Can't write to output file");
        result
    };

    type ExperimentSetup = (String, String, String);
    let mut best_placement_algs: HashMap<ExperimentSetup, Vec<(String, RunStats)>> = HashMap::new();
    let mut alg_runs: HashMap<String, Vec<RunStats>> = HashMap::new();
    for run in result.into_iter() {
        best_placement_algs
            .entry((run.plan.clone(), run.system.clone(), run.replication_strategy.clone()))
            .or_default()
            .push((run.placement_strategy.clone(), run.run_stats.clone()));
        alg_runs
            .entry(run.placement_strategy.clone())
            .or_default()
            .push(run.run_stats);
    }

    let mut places: BTreeMap<String, Vec<(usize, f64, f64)>> = BTreeMap::new();
    for (_key, mut values) in best_placement_algs.into_iter() {
        values.sort_by(|a, b| a.1.average_dag_makespan.total_cmp(&b.1.average_dag_makespan));
        let best_makespan = values[0].1.average_dag_makespan;
        let best_total_makespan = values
            .iter()
            .map(|v| v.1.total_makespan)
            .min_by(|a, b| a.total_cmp(b))
            .unwrap();
        for (i, (alg, run_stats)) in values.into_iter().enumerate() {
            places.entry(alg).or_default().push((
                i + 1,
                run_stats.average_dag_makespan / best_makespan,
                run_stats.total_makespan / best_total_makespan,
            ));
        }
    }

    let mut result = places
        .into_iter()
        .map(|(alg, places)| ResultRow {
            name: alg.clone(),
            avg_slowdown: places.iter().map(|x| x.1).sum::<f64>() / places.len() as f64,
            max_slowdown: places.iter().map(|x| x.1).max_by(|a, b| a.total_cmp(b)).unwrap(),
            avg_total_slowdown: places.iter().map(|x| x.2).sum::<f64>() / places.len() as f64,
            r2r_traffic: alg_runs[&alg]
                .iter()
                .map(|run| run.network_traffic_between_racks / run.total_network_traffic * 100.)
                .sum::<f64>()
                / places.len() as f64,
            h2h_traffic: alg_runs[&alg]
                .iter()
                .map(|run| run.network_traffic_between_hosts / run.total_network_traffic * 100.)
                .sum::<f64>()
                / places.len() as f64,
        })
        .collect::<Vec<_>>();

    result.sort_by(|a, b| a.avg_slowdown.total_cmp(&b.avg_slowdown).then(a.name.cmp(&b.name)));

    let width = result.iter().map(|x| x.name.len()).max().unwrap();
    println!(
        "| {: <width$} | avg slowdown | max slowdown | avg total slowdown | r2r traffic | h2h traffic |",
        "algorithm",
        width = width
    );
    println!(
        "|-{:-<width$}-|--------------|--------------|--------------------|-------------|-------------|",
        "",
        width = width
    );
    for row in result.into_iter() {
        println!(
            "| {: <width$} | {: >12.3} | {: >12.3} | {: >18.3} | {: >10.2}% | {: >10.2}% |",
            row.name,
            row.avg_slowdown,
            row.max_slowdown,
            row.avg_total_slowdown,
            row.r2r_traffic,
            row.h2h_traffic,
            width = width
        );
    }
}
