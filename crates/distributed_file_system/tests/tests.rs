use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    rc::Rc,
};

use dslab_core::{EventHandler, Id, Simulation};
use dslab_dfs::{
    dfs::{DistributedFileSystem, RegisterData},
    host_info::{ChunkId, HostInfo},
    network::make_constant_network,
    replication_strategies::random::{ChunkDistribution, RandomReplicationStrategy},
    replication_strategy::ReplicationStrategy,
};
use dslab_network::Network;

struct DataOnHost {}

impl EventHandler for DataOnHost {
    fn on(&mut self, _event: dslab_core::Event) {
        panic!()
    }
}

struct SimpleStrategy {}

impl ReplicationStrategy for SimpleStrategy {
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
            .map(|chunk_id| {
                (
                    *chunk_id,
                    if chunk_id % 3 == 0 {
                        vec![hosts[0]]
                    } else if chunk_id % 3 == 1 {
                        vec![hosts[1]]
                    } else {
                        vec![hosts[0], hosts[1]]
                    },
                )
            })
            .collect()
    }
}

#[test]
fn simple() {
    let mut sim = Simulation::new(123);
    let network = make_constant_network(&mut sim, 2, 1, 100.0, 100.0);
    let nodes = network.borrow().get_nodes();
    let mut actor_by_host_name: BTreeMap<String, Id> = BTreeMap::new();
    for node_name in nodes {
        if !node_name.starts_with("host_") {
            continue;
        }

        let data_on_host_name = format!("data_on_host_{}", &node_name[5..]);
        sim.create_context(&data_on_host_name);
        let data_on_host = DataOnHost {};
        let data_on_host_id = sim.add_handler(data_on_host_name, Rc::new(RefCell::new(data_on_host)));
        actor_by_host_name.insert(node_name.clone(), data_on_host_id);
        network.borrow_mut().set_location(data_on_host_id, &node_name);
    }

    let dfs = DistributedFileSystem::new(
        [
            (
                actor_by_host_name["host_0_0"],
                HostInfo {
                    free_space: 1000,
                    chunks: BTreeSet::new(),
                },
            ),
            (
                actor_by_host_name["host_1_0"],
                HostInfo {
                    free_space: 1000,
                    chunks: BTreeSet::new(),
                },
            ),
        ]
        .into_iter()
        .collect(),
        HashMap::new(),
        network.clone(),
        Box::new(SimpleStrategy {}),
        100,
        sim.create_context("dfs"),
    );
    let dfs = Rc::new(RefCell::new(dfs));
    let dfs_id = sim.add_handler("dfs", dfs.clone());

    let root = sim.create_context("root");
    root.emit_now(
        RegisterData {
            size: 600,
            host: *actor_by_host_name.values().next().unwrap(),
            data_id: 0,
            need_to_replicate: true,
        },
        dfs_id,
    );
    sim.step_until_no_events();

    let correct_time = 1.0 + 1e-4;
    assert!((sim.time() - correct_time).abs() < 1e-9);

    assert_eq!(
        dfs.borrow()
            .hosts_info()
            .values()
            .map(|host| host.chunks.iter().copied().collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec![0, 2, 3, 5], vec![1, 2, 4, 5]],
    );
}

#[test]
fn different_racks() {
    let mut sim = Simulation::new(123);
    let network = make_constant_network(&mut sim, 3, 3, 100.0, 100.0);
    let nodes = network.borrow().get_nodes();
    let mut actor_by_host_name: BTreeMap<String, Id> = BTreeMap::new();
    for node_name in nodes {
        if !node_name.starts_with("host_") {
            continue;
        }

        let data_on_host_name = format!("data_on_host_{}", &node_name[5..]);
        sim.create_context(&data_on_host_name);
        let data_on_host = DataOnHost {};
        let data_on_host_id = sim.add_handler(data_on_host_name, Rc::new(RefCell::new(data_on_host)));
        actor_by_host_name.insert(node_name.clone(), data_on_host_id);
        network.borrow_mut().set_location(data_on_host_id, &node_name);
    }

    let dfs = DistributedFileSystem::new(
        (0..3)
            .flat_map(|rack| (0..3).map(move |host| format!("host_{rack}_{host}")))
            .map(|name| {
                (
                    actor_by_host_name[&name],
                    HostInfo {
                        free_space: 1000000,
                        chunks: BTreeSet::new(),
                    },
                )
            })
            .collect(),
        HashMap::new(),
        network.clone(),
        Box::new(RandomReplicationStrategy::new(2, ChunkDistribution::ProhibitSameRack)),
        100,
        sim.create_context("dfs"),
    );
    let dfs = Rc::new(RefCell::new(dfs));
    let dfs_id = sim.add_handler("dfs", dfs.clone());

    let root = sim.create_context("root");
    root.emit_now(
        RegisterData {
            size: 100 * 1000,
            host: *actor_by_host_name.values().next().unwrap(),
            data_id: 0,
            need_to_replicate: true,
        },
        dfs_id,
    );
    sim.step_until_no_events();

    let rack_by_id = actor_by_host_name
        .iter()
        .map(|(name, id)| (*id, name.split('_').nth(1).unwrap().parse::<usize>().unwrap()))
        .collect::<HashMap<_, _>>();

    let mut chunk_racks: BTreeMap<ChunkId, Vec<usize>> = BTreeMap::new();
    for (&host_id, host) in dfs.borrow().hosts_info().iter() {
        for &chunk_id in host.chunks.iter() {
            chunk_racks.entry(chunk_id).or_default().push(rack_by_id[&host_id]);
        }
    }

    for (_chunk_id, mut hosts) in chunk_racks.into_iter() {
        hosts.sort();
        assert!(hosts.windows(2).all(|a| a[0] != a[1]));
        assert_eq!(hosts.len(), 2);
    }
}
