use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

use clap::Parser;
use dslab_dfs::replication_strategy::ReplicationStrategy;
use dslab_mr::{
    experiment::{Experiment, Plan},
    placement_strategy::DynamicPlacementStrategy,
    system::SystemConfig,
};
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
    #[arg(short, long, default_value_t = std::thread::available_parallelism().unwrap().get())]
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

fn replication_strategy_resolver(_name: &str) -> Box<dyn ReplicationStrategy> {
    unimplemented!()
}

fn placement_strategy_resolver(_name: &str) -> Box<dyn DynamicPlacementStrategy> {
    unimplemented!()
}

fn main() {
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
}
