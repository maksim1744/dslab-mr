use std::{
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use dslab_dfs::replication_strategy::ReplicationStrategy;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use threadpool::ThreadPool;

use crate::{
    cluster_simulation::{ClusterSimulation, SimulationPlan},
    placement_strategy::DynamicPlacementStrategy,
    run_stats::RunStats,
    system::SystemConfig,
};

#[derive(Clone)]
pub struct Plan {
    pub name: String,
    pub plan_path: PathBuf,
    pub dags_path: PathBuf,
}

struct Run {
    plan: Plan,
    system: (String, SystemConfig),
    replication_strategy: String,
    placement_strategy: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunResult {
    plan: String,
    system: String,
    replication_strategy: String,
    placement_strategy: String,
    run_stats: RunStats,
}

pub struct Experiment {
    seed: u64,
    plans: Vec<Plan>,
    systems: Vec<(String, SystemConfig)>,
    replication_strategies: Vec<String>,
    placement_strategies: Vec<String>,
    replication_strategy_resolver: fn(&str) -> Box<dyn ReplicationStrategy>,
    placement_strategy_resolver: fn(&str) -> Box<dyn DynamicPlacementStrategy>,
    traces_folder: Option<PathBuf>,
}

impl Experiment {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        seed: u64,
        plans: Vec<Plan>,
        systems: Vec<(String, SystemConfig)>,
        replication_strategies: Vec<String>,
        placement_strategies: Vec<String>,
        replication_strategy_resolver: fn(&str) -> Box<dyn ReplicationStrategy>,
        placement_strategy_resolver: fn(&str) -> Box<dyn DynamicPlacementStrategy>,
        traces_folder: Option<PathBuf>,
    ) -> Self {
        Self {
            seed,
            plans,
            systems,
            replication_strategies,
            placement_strategies,
            replication_strategy_resolver,
            placement_strategy_resolver,
            traces_folder,
        }
    }

    pub fn run(self, threads: usize) -> Vec<RunResult> {
        if let Some(dir) = &self.traces_folder {
            std::fs::create_dir_all(dir).unwrap();
        }

        let runs = self
            .plans
            .into_iter()
            .cartesian_product(self.systems)
            .cartesian_product(self.replication_strategies)
            .cartesian_product(self.placement_strategies)
            .map(|(((plan, system), replication_strategy), placement_strategy)| Run {
                plan,
                system,
                replication_strategy,
                placement_strategy,
            })
            .collect::<Vec<_>>();

        let total_runs = runs.len();

        let finished_run_atomic = Arc::new(AtomicUsize::new(0));
        let results = Arc::new(Mutex::new(Vec::new()));

        let pool = ThreadPool::new(threads);
        let start_time = Instant::now();
        for run in runs.into_iter() {
            let finished_run_atomic = finished_run_atomic.clone();
            let results = results.clone();
            let traces_folder = self.traces_folder.clone();
            pool.execute(move || {
                let replication_strategy = (self.replication_strategy_resolver)(&run.replication_strategy);
                let placement_strategy = (self.placement_strategy_resolver)(&run.placement_strategy);

                let sim = ClusterSimulation::new(
                    self.seed,
                    SimulationPlan::from_yaml(run.plan.plan_path, run.plan.dags_path),
                    run.system.1,
                    replication_strategy,
                    placement_strategy,
                    traces_folder.map(|folder| {
                        folder.join(format!(
                            "{}_{}_{}_{}.json",
                            run.plan.name, run.system.0, run.replication_strategy, run.placement_strategy
                        ))
                    }),
                );

                let run_stats = sim.run();

                results.lock().unwrap().push(RunResult {
                    plan: run.plan.name,
                    system: run.system.0,
                    replication_strategy: run.replication_strategy,
                    placement_strategy: run.placement_strategy,
                    run_stats,
                });

                finished_run_atomic.fetch_add(1, Ordering::SeqCst);
                let finished_runs = finished_run_atomic.load(Ordering::SeqCst);

                let elapsed = start_time.elapsed();
                let remaining = Duration::from_secs_f64(
                    elapsed.as_secs_f64() / finished_runs as f64 * (total_runs - finished_runs) as f64,
                );
                print!("\r{}", " ".repeat(70));
                print!(
                    "\rFinished {}/{} [{}%] runs in {:.2?}, remaining time: {:.2?}",
                    finished_runs,
                    total_runs,
                    (finished_runs as f64 * 100. / total_runs as f64).round() as i32,
                    elapsed,
                    remaining
                );
                std::io::stdout().flush().unwrap();
            });
        }

        pool.join();

        print!("\r{}", " ".repeat(70));
        println!("\rFinished {} runs in {:.2?}", total_runs, start_time.elapsed());

        let mut results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
        results.sort_by_cached_key(|run| {
            (
                run.plan.clone(),
                run.system.clone(),
                run.replication_strategy.clone(),
                run.placement_strategy.clone(),
            )
        });
        results
    }
}
