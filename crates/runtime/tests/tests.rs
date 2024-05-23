use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use dslab_core::Id;
use dslab_dfs::{
    host_info::{ChunkId, HostInfo},
    replication_strategy::ReplicationStrategy,
};
use dslab_mr::{
    cluster_simulation::{ClusterSimulation, DagPlan, InputPlan, SimulationPlan},
    dag::{Dag, SimpleTask, Task, UniformShuffle},
    placement_strategies::packing_scheduler::PackingScheduler,
    system::{HostConfig, NetworkConfig, SystemConfig},
};
use dslab_network::Network;

struct SimpleReplicationStrategy {}

impl ReplicationStrategy for SimpleReplicationStrategy {
    fn register_chunks(
        &mut self,
        _chunk_size: u64,
        _host: Id,
        chunks: &[ChunkId],
        _need_to_replicate: bool,
        host_info: &BTreeMap<Id, HostInfo>,
        _network: &Network,
    ) -> BTreeMap<ChunkId, Vec<Id>> {
        let hosts = host_info.keys().copied().collect::<Vec<_>>();
        chunks
            .iter()
            .map(|chunk_id| (*chunk_id, vec![hosts[*chunk_id as usize % 2]]))
            .collect()
    }
}

#[test]
fn map_reduce() {
    let mut dag = Dag::new();
    dag.add_stage(
        (0..2)
            .map(|task_id| Box::new(SimpleTask::new(task_id, 1, 128, 1.0, 4.0)) as Box<dyn Task>)
            .collect(),
        false,
    );
    dag.add_stage(
        (0..2)
            .map(|task_id| Box::new(SimpleTask::new(task_id, 1, 128, 2.0, 0.1)) as Box<dyn Task>)
            .collect(),
        true,
    );
    dag.add_connection(0, 1, Some(Box::new(UniformShuffle {})));
    dag.add_initial_data(0, 200);

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

    let sim = ClusterSimulation::new(
        123,
        plan,
        SystemConfig {
            network: NetworkConfig::Constant {
                racks: 1,
                hosts_per_rack: 2,
                bandwidth: 100.0,
                internal_bw: 1000.0,
            },
            chunk_size: 10,
            hosts: vec![HostConfig {
                name: "default".to_string(),
                speed: 1.0,
                available_space: 100000,
                available_cores: 1,
                available_memory: 1024,
            }],
        },
        Box::new(SimpleReplicationStrategy {}),
        Box::new(PackingScheduler::new(0.1, 0.1)),
        None,
    );

    let run_stats = sim.run();
    let correct_makespan = 0.01 + 100.0 + 2.0 + 1e-4 + 800.0 + 0.1 + 1e-4;
    println!("{:.10}", run_stats.total_makespan);
    println!("{:.10}", correct_makespan);
    assert!((run_stats.total_makespan - correct_makespan) < 1e-9);
}
