use std::{cell::RefCell, io::Write, rc::Rc, time::Instant};

use env_logger::Builder;

use dslab_dfs::replication_strategies::random::{ChunkDistribution, RandomReplicationStrategy};
use dslab_mr::{
    cluster_simulation::{ClusterSimulation, DagPlan, InputPlan, SimulationPlan},
    dag::{Dag, SimpleTask, Task, UniformShuffle},
    placement_strategies::packing_scheduler::PackingScheduler,
    system::SystemConfig,
};

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut dag = Dag::new();
    dag.add_stage(
        (0..100 * 100)
            .map(|task_id| Box::new(SimpleTask::new(task_id, 8, 128000, 1000.0, 2.0)) as Box<dyn Task>)
            .collect(),
        false,
    );
    dag.add_stage(
        (0..100 * 100)
            .map(|task_id| Box::new(SimpleTask::new(task_id, 8, 128000, 2000.0, 0.1)) as Box<dyn Task>)
            .collect(),
        true,
    );
    dag.add_connection(0, 1, Some(Box::new(UniformShuffle {})));
    dag.add_initial_data(0, 100 * 100 * 128);

    let plan = SimulationPlan {
        dags: vec![DagPlan {
            start_time: 0.0,
            dag: Rc::new(RefCell::new(dag)),
            input: [(
                0,
                InputPlan::RegisterInitially {
                    host: "host_0_0".to_string(),
                },
            )]
            .into_iter()
            .collect(),
        }],
        global_inputs: Vec::new(),
    };

    let start_time = Instant::now();
    let sim = ClusterSimulation::new(
        123,
        plan,
        SystemConfig::from_yaml("system.yaml"),
        Box::new(RandomReplicationStrategy::new(3, ChunkDistribution::ProhibitSameRack)),
        Box::new(PackingScheduler::new(0.01, 0.01)),
        Some("trace.json".into()),
    );

    let run_stats = sim.run();
    println!("\nRun stats:\n{}", serde_yaml::to_string(&run_stats).unwrap());
    println!("Finished in {:.2?}", start_time.elapsed());
}
