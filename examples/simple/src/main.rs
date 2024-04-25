use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Write,
    rc::Rc,
};

use env_logger::Builder;

use dslab_core::{log_info, EventHandler, Id, Simulation};
use dslab_dfs::{
    dfs::RegisterData,
    host_info::{ChunkId, HostInfo},
    replication_strategy::ReplicationStrategy,
};
use dslab_mr::{
    compute_host_info::ComputeHostInfo,
    dag::{Dag, Stage},
    data_item::DataItem,
    placement_strategy::{PlacementStrategy, TaskPlacement},
    runner::{Runner, Start},
};
use dslab_network::Network;

use dslab_dfs::{dfs::DistributedFileSystem, network::make_tree_topology};

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

struct SimplePlacementStrategy {}

impl PlacementStrategy for SimplePlacementStrategy {
    fn place_stage(
        &mut self,
        stage: &Stage,
        _graph: &Dag,
        input_data: &[DataItem],
        input_data_shuffled: &[Vec<DataItem>],
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
        for (i, shuffled_data) in input_data_shuffled.iter().enumerate() {
            result[i].input.extend(shuffled_data);
        }
        result
    }
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(123);

    let network_rc = make_tree_topology(&mut sim, 3, 2);

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

    let dag = Dag::from_yaml("map_reduce.yaml");

    let first_host = *hosts.keys().next().unwrap();
    let dfs = DistributedFileSystem::new(
        hosts,
        HashMap::new(),
        network_rc.clone(),
        Box::new(SimpleReplicationStrategy::new()),
        16,
        sim.create_context("dfs"),
    );
    let dfs = Rc::new(RefCell::new(dfs));
    let dfs_id = sim.add_handler("dfs", dfs.clone());
    let root = sim.create_context("root");

    root.emit_now(
        RegisterData {
            size: dag.initial_data(),
            host: first_host,
            data_id: 0,
            need_to_replicate: true,
        },
        dfs_id,
    );
    sim.step_until_no_events();
    log_info!(root, "data registered, starting execution");

    let runner = Rc::new(RefCell::new(Runner::new(
        dag,
        Box::new(SimplePlacementStrategy {}),
        actor_ids
            .iter()
            .map(|&actor_id| (actor_id, ComputeHostInfo { available_slots: 4 }))
            .collect(),
        [(0, vec![DataItem::Replicated { size: 256, data_id: 0 }])]
            .into_iter()
            .collect(),
        dfs.clone(),
        network_rc.clone(),
        sim.create_context("runner"),
    )));
    let runner_id = sim.add_handler("runner", runner.clone());

    root.emit_now(Start {}, runner_id);
    sim.step_until_no_events();
}
