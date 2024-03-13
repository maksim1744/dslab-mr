use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Write,
    rc::Rc,
};

use env_logger::Builder;

use distributed_file_system::{
    dfs::RegisterData,
    host_info::{ChunkId, HostInfo},
    replication_strategy::ReplicationStrategy,
};
use dslab_core::{EventHandler, Id, Simulation};
use dslab_network::{
    models::{SharedBandwidthNetworkModel, TopologyAwareNetworkModel},
    Link, Network,
};

use crate::distributed_file_system::dfs::DistributedFileSystem;

mod distributed_file_system;

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
    for node_name in nodes {
        if !node_name.starts_with("host_") {
            continue;
        }
        let data_on_host_name = format!("data_on_host_{}", &node_name[5..]);
        sim.create_context(&data_on_host_name);
        let data_on_host = DataOnHost::new();
        let data_on_host_id = sim.add_handler(data_on_host_name, Rc::new(RefCell::new(data_on_host)));
        hosts.insert(
            data_on_host_id,
            HostInfo {
                free_space: 1000,
                chunks: BTreeSet::new(),
            },
        );
        network_rc.borrow_mut().set_location(data_on_host_id, &node_name);
    }
    let host_ids = hosts.keys().copied().collect::<Vec<_>>();

    let dfs = DistributedFileSystem::new(
        hosts,
        HashMap::new(),
        network_rc,
        Box::new(SimpleReplicationStrategy::new()),
        16,
        sim.create_context("dfs"),
    );
    let dfs = Rc::new(RefCell::new(dfs));
    let dfs_id = sim.add_handler("dfs", dfs.clone());
    let root = sim.create_context("root");
    for i in 1..=10 {
        root.emit(
            RegisterData {
                size: i * 10,
                host: host_ids[i as usize % host_ids.len()],
                data_id: i,
                need_to_replicate: false,
            },
            dfs_id,
            i as f64,
        );
    }
    sim.step_until_no_events();
    for i in 1..=10 {
        let dfs = dfs.borrow();
        let chunks = dfs.data_chunks(i).unwrap();
        println!("Data {i} is split into chunks {chunks:?}");
        for &chunk_id in chunks.iter() {
            println!(
                "    Chunk {chunk_id} is located on hosts {:?}",
                dfs.chunks_location(chunk_id).unwrap()
            );
        }
    }
}
