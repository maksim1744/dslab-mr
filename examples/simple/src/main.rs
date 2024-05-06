use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
};

use env_logger::Builder;

use dslab_core::Id;
use dslab_dfs::{
    dfs::DistributedFileSystem,
    host_info::{ChunkId, HostInfo},
    replication_strategy::ReplicationStrategy,
};
use dslab_mr::{
    cluster_simulation::{ClusterSimulation, NetworkConfig, SimulationPlan},
    compute_host_info::ComputeHostInfo,
    dag::{Dag, Stage},
    data_item::DataItem,
    placement_strategy::{PlacementStrategy, TaskPlacement},
};
use dslab_network::Network;

struct SimpleReplicationStrategy {}

impl SimpleReplicationStrategy {
    fn new() -> Self {
        SimpleReplicationStrategy {}
    }
}

impl ReplicationStrategy for SimpleReplicationStrategy {
    fn register_chunks(
        &mut self,
        _chunk_size: u64,
        _host: Id,
        chunks: &[ChunkId],
        _need_to_replicate: bool,
        host_info: &BTreeMap<Id, HostInfo>,
    ) -> BTreeMap<ChunkId, Vec<Id>> {
        let mut result = BTreeMap::new();
        for &chunk_id in chunks.iter() {
            let ids = host_info.keys().collect::<Vec<_>>();
            let hosts = vec![
                *ids[chunk_id as usize % ids.len()],
                *ids[(chunk_id + 1) as usize % ids.len()],
            ];
            result.insert(chunk_id, hosts);
        }
        result
    }
}

struct SimplePlacementStrategy {}

impl PlacementStrategy for SimplePlacementStrategy {
    fn place_stage(
        &mut self,
        stage: &Stage,
        _graph: &Dag,
        input_data: &[DataItem],
        _input_data_shuffled: &[Vec<DataItem>],
        dfs: &DistributedFileSystem,
        _compute_host_info: &BTreeMap<Id, ComputeHostInfo>,
        _network: &Network,
    ) -> Vec<TaskPlacement> {
        let mut my_data_items = Vec::new();
        for &data_item in input_data.iter() {
            match data_item {
                DataItem::Chunk { .. } | DataItem::Local { .. } => my_data_items.push(data_item),
                DataItem::Replicated { data_id, .. } => {
                    for &chunk_id in dfs.data_chunks(data_id).unwrap() {
                        my_data_items.push(DataItem::Chunk {
                            size: dfs.chunk_size(),
                            chunk_id,
                        });
                    }
                }
            }
        }
        let hosts = dfs.hosts_info().keys().copied().collect::<Vec<_>>();
        let mut result = (0..stage.tasks().len())
            .map(|task_id| TaskPlacement {
                host: hosts[task_id % hosts.len()],
                input: Vec::new(),
            })
            .collect::<Vec<_>>();
        for (i, &chunk_id) in my_data_items.iter().enumerate() {
            let target_task = i % result.len();
            result[target_task].input.push(chunk_id);
        }
        result
    }
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let sim = ClusterSimulation::new(
        123,
        SimulationPlan::from_yaml("plan.yaml", ".".into()),
        NetworkConfig::Tree {
            star_count: 3,
            hosts_per_star: 2,
        },
        (0..3)
            .flat_map(|star| {
                (0..2).map(move |host| {
                    (
                        format!("host_{star}_{host}"),
                        HostInfo {
                            free_space: 1024,
                            chunks: BTreeSet::new(),
                        },
                    )
                })
            })
            .collect(),
        Box::new(SimpleReplicationStrategy::new()),
        16,
        Box::new(SimplePlacementStrategy {}),
        (0..3)
            .flat_map(|star| {
                (0..2).map(move |host| (format!("host_{star}_{host}"), ComputeHostInfo { available_slots: 4 }))
            })
            .collect(),
    );

    sim.run();
}
