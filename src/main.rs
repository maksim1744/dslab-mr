use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Write,
    rc::Rc,
};

use env_logger::Builder;

use distributed_file_system::{
    host_info::{ChunkId, DataId, HostInfo},
    replication_strategy::ReplicationStrategy,
};
use dslab_core::{EventHandler, Id, Simulation};
use dslab_network::{
    models::{SharedBandwidthNetworkModel, TopologyAwareNetworkModel},
    Link, Network,
};
use map_reduce::{
    compute_host_info::ComputeHostInfo,
    data_item::DataItem,
    map_reduce_params::{MapOutput, MapReduceParams},
    placement_strategy::{MapTaskPlacement, PlacementStrategy},
    runner::{InitialDataLocation, MapReduceRunner, Start},
};

use crate::distributed_file_system::dfs::DistributedFileSystem;

mod distributed_file_system;
mod map_reduce;

fn make_star_topology(network: &mut Network, host_count: usize) {
    let switch_name = "switch".to_string();
    network.add_node(&switch_name, Box::new(SharedBandwidthNetworkModel::new(1e+5, 0.)));

    for i in 0..host_count {
        let host_name = format!("host_{}", i);
        network.add_node(&host_name, Box::new(SharedBandwidthNetworkModel::new(1e+5, 0.)));
        network.add_link(&host_name, &switch_name, Link::shared(1., 1e-4));
    }
}

struct DataOnHost {}

impl DataOnHost {
    fn new() -> Self {
        DataOnHost {}
    }
}

impl EventHandler for DataOnHost {
    fn on(&mut self, _event: dslab_core::Event) {
        panic!()
    }
}

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

struct SimpleMapReduceParams {}

impl MapReduceParams for SimpleMapReduceParams {
    fn map_tasks_count(&self) -> u64 {
        2
    }

    fn map_task_time(&self, task_id: u64, _input_size: u64) -> f64 {
        300. + task_id as f64 * 50.
    }

    fn map_task_output(&self, _task_id: u64, _input_size: u64) -> Vec<MapOutput> {
        (0..self.reduce_tasks_count())
            .map(|i| MapOutput {
                reducer_id: i,
                size: 128,
            })
            .collect()
    }

    fn reduce_tasks_count(&self) -> u64 {
        2
    }

    fn reduce_task_time(&self, task_id: u64, _input_size: u64) -> f64 {
        100. + task_id as f64 * 10.
    }

    fn reduce_task_output(&self, _task_id: u64, _input_size: u64) -> u64 {
        32
    }
}

struct SimplePlacementStrategy {}

impl PlacementStrategy for SimplePlacementStrategy {
    fn place_map_tasks(
        &mut self,
        task_count: u64,
        input_chunks: &[ChunkId],
        _chunks_location: &HashMap<ChunkId, BTreeSet<Id>>,
        host_info: &BTreeMap<Id, HostInfo>,
        _compute_host_info: &BTreeMap<Id, map_reduce::compute_host_info::ComputeHostInfo>,
        _network: &Network,
    ) -> Vec<map_reduce::placement_strategy::MapTaskPlacement> {
        let hosts = host_info.keys().copied().collect::<Vec<_>>();
        let mut result = (0..task_count)
            .map(|task_id| MapTaskPlacement {
                host: hosts[task_id as usize % hosts.len()],
                chunks: Vec::new(),
            })
            .collect::<Vec<_>>();
        for (i, &chunk_id) in input_chunks.iter().enumerate() {
            let target_task = i % result.len();
            result[target_task].chunks.push(chunk_id);
        }
        result
    }

    fn place_reduce_task(
        &mut self,
        task_id: u64,
        _input_data_items: &[DataItem],
        _data_chunks: &HashMap<DataId, Vec<ChunkId>>,
        _chunks_location: &HashMap<ChunkId, BTreeSet<Id>>,
        host_info: &BTreeMap<Id, HostInfo>,
        _compute_host_info: &BTreeMap<Id, map_reduce::compute_host_info::ComputeHostInfo>,
        _network: &Network,
    ) -> Id {
        let hosts = host_info.keys().copied().collect::<Vec<_>>();
        hosts[(task_id as usize + hosts.len() / 2) % hosts.len()]
    }
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(123);

    let mut network = Network::new(Box::new(TopologyAwareNetworkModel::new()), sim.create_context("net"));
    make_star_topology(&mut network, 3);
    network.init_topology();
    let network_rc = Rc::new(RefCell::new(network));
    sim.add_handler("net", network_rc.clone());

    let mut hosts: BTreeMap<Id, HostInfo> = BTreeMap::new();
    let nodes = network_rc.borrow_mut().get_nodes();
    let mut actor_ids = Vec::new();
    for node_name in nodes {
        if !node_name.starts_with("host_") {
            continue;
        }
        for actor in 0..2 {
            let data_on_host_name = format!("data_on_host_{}_{}", &node_name[5..], actor);
            sim.create_context(&data_on_host_name);
            let data_on_host = DataOnHost::new();
            let data_on_host_id = sim.add_handler(data_on_host_name, Rc::new(RefCell::new(data_on_host)));
            hosts.insert(
                data_on_host_id,
                HostInfo {
                    free_space: 1024,
                    chunks: BTreeSet::new(),
                },
            );
            network_rc.borrow_mut().set_location(data_on_host_id, &node_name);
            actor_ids.push(data_on_host_id);
        }
    }

    let dfs = DistributedFileSystem::new(
        hosts,
        HashMap::new(),
        network_rc.clone(),
        Box::new(SimpleReplicationStrategy::new()),
        16,
        sim.create_context("dfs"),
    );
    let dfs = Rc::new(RefCell::new(dfs));
    sim.add_handler("dfs", dfs.clone());
    let root = sim.create_context("root");

    let runner = Rc::new(RefCell::new(MapReduceRunner::new(
        Box::new(SimpleMapReduceParams {}),
        Box::new(SimplePlacementStrategy {}),
        actor_ids
            .iter()
            .map(|&actor_id| (actor_id, ComputeHostInfo { available_slots: 4 }))
            .collect(),
        InitialDataLocation::ExistsOnHost {
            host: actor_ids[0],
            size: 256,
        },
        dfs.clone(),
        network_rc.clone(),
        sim.create_context("runner"),
    )));
    let runner_id = sim.add_handler("runner", runner.clone());

    root.emit_now(Start {}, runner_id);
    sim.step_until_no_events();
}
