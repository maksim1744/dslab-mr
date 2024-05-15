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
    experiment::{Experiment, Plan},
    placement_strategies::{locality_aware::LocalityAwareStrategy, random::RandomPlacementStrategy},
    placement_strategy::DynamicPlacementStrategy,
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
    match name {
        "Random" => Box::new(RandomPlacementStrategy::new()),
        "LocalityAware" => Box::new(LocalityAwareStrategy::new()),
        x => panic!("Unkwnon placement strategy {}", x),
    }
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();
    let config: Config = serde_yaml::from_str(&std::fs::read_to_string(args.config).expect("Can't read config file"))
        .expect("Can't parse config file");
    let experiment = Experiment::new(
        123,
        config
            .plans
            .into_iter()
            .map(|plan| Plan {
                name: filename(&plan.plan_path),
                plan_path: plan.plan_path,
                dags_path: plan.dags_path,
            })
            .collect(),
        config
            .systems
            .into_iter()
            .map(|path| (filename(&path), SystemConfig::from_yaml(path)))
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

    let mut best_placement_algs: HashMap<(String, String, String), Vec<(f64, String)>> = HashMap::new();
    for run in result.iter() {
        best_placement_algs
            .entry((run.plan.clone(), run.system.clone(), run.replication_strategy.clone()))
            .or_default()
            .push((run.run_stats.average_dag_makespan, run.placement_strategy.clone()));
    }

    let mut places: BTreeMap<String, Vec<(usize, f64)>> = BTreeMap::new();
    for (_key, mut values) in best_placement_algs.into_iter() {
        values.sort_by(|a, b| a.0.total_cmp(&b.0));
        let best_makespan = values[0].0;
        for (i, (makespan, alg)) in values.into_iter().enumerate() {
            places.entry(alg).or_default().push((i + 1, makespan / best_makespan));
        }
    }

    let mut result = places
        .into_iter()
        .map(|(alg, places)| {
            (
                alg,
                places.iter().map(|x| x.0).sum::<usize>() as f64 / places.len() as f64,
                places.iter().map(|x| x.1).sum::<f64>() / places.len() as f64,
            )
        })
        .collect::<Vec<_>>();

    result.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.2.total_cmp(&b.2)));

    println!("| algorithm                                | avg place | avg slowdown |");
    println!("|------------------------------------------|-----------|--------------|");
    for (alg, place, slowdown) in result.into_iter() {
        println!("| {: <40} | {: >9.3} | {: >12.3} |", alg, place, slowdown);
    }
}
