//! Common network topologies and useful functions.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use dslab_core::{Id, Simulation};
use dslab_network::models::{ConstantBandwidthNetworkModel, SharedBandwidthNetworkModel, TopologyAwareNetworkModel};
use dslab_network::{Link, Network, NodeId};

/// Creates [constant bandwidth](ConstantBandwidthNetworkModel) network with given bandwidth.
pub fn make_constant_network(
    sim: &mut Simulation,
    racks: usize,
    hosts_per_rack: usize,
    bandwidth: f64,
    internal_bw: f64,
) -> Rc<RefCell<Network>> {
    let mut network = Network::new(
        Box::new(ConstantBandwidthNetworkModel::new(bandwidth, 1e-4)),
        sim.create_context("net"),
    );

    for i in 0..racks {
        for j in 0..hosts_per_rack {
            let host_name = format!("host_{}_{}", i, j);
            network.add_node(
                &host_name,
                Box::new(ConstantBandwidthNetworkModel::new(internal_bw, 0.)),
            );
        }
    }

    let network_rc = Rc::new(RefCell::new(network));
    sim.add_handler("net", network_rc.clone());
    network_rc
}

/// Creates [shared bandwidth](SharedBandwidthNetworkModel) network with given bandwidth.
pub fn make_shared_network(
    sim: &mut Simulation,
    racks: usize,
    hosts_per_rack: usize,
    bandwidth: f64,
    internal_bw: f64,
) -> Rc<RefCell<Network>> {
    let mut network = Network::new(
        Box::new(SharedBandwidthNetworkModel::new(bandwidth, 1e-4)),
        sim.create_context("net"),
    );

    for i in 0..racks {
        for j in 0..hosts_per_rack {
            let host_name = format!("host_{}_{}", i, j);
            network.add_node(
                &host_name,
                Box::new(ConstantBandwidthNetworkModel::new(internal_bw, 0.)),
            );
        }
    }

    let network_rc = Rc::new(RefCell::new(network));
    sim.add_handler("net", network_rc.clone());
    network_rc
}

/// Creates tree topology with `star_count` racks and `hosts_per_star` hosts on each one.
/// * `downlink_bw` corresponds to a link bandwidth between rack switch and each host.
/// * `uplink_bw` corresponds to a link bandwidth between rack switch and root switch.
/// * `internal_bw` corresponds to an internal bandwidth of each host.
pub fn make_tree_topology(
    sim: &mut Simulation,
    star_count: usize,
    hosts_per_star: usize,
    downlink_bw: f64,
    uplink_bw: f64,
    internal_bw: f64,
) -> Rc<RefCell<Network>> {
    let mut network = Network::new(Box::new(TopologyAwareNetworkModel::new()), sim.create_context("net"));

    let root_switch_name = "root_switch".to_string();
    network.add_node(
        &root_switch_name,
        Box::new(ConstantBandwidthNetworkModel::new(internal_bw, 0.)),
    );

    for i in 0..star_count {
        let switch_name = format!("switch_{}", i);
        network.add_node(
            &switch_name,
            Box::new(ConstantBandwidthNetworkModel::new(internal_bw, 0.)),
        );
        network.add_link(&root_switch_name, &switch_name, Link::shared(uplink_bw, 1e-4));

        for j in 0..hosts_per_star {
            let host_name = format!("host_{}_{}", i, j);
            network.add_node(
                &host_name,
                Box::new(ConstantBandwidthNetworkModel::new(internal_bw, 0.)),
            );
            network.add_link(&host_name, &switch_name, Link::shared(downlink_bw, 1e-4));
        }
    }

    network.init_topology();
    let network_rc = Rc::new(RefCell::new(network));
    sim.add_handler("net", network_rc.clone());
    network_rc
}

/// Creates fat tree topology.
/// * `l2_switch_count` --- number of top-level switches.
/// * `l1_switch_count` --- number of racks.
/// * `hosts_per_switch` --- number of hosts per rack.
/// * `downlink_bw` --- link bandwidth between rack switch and each host.
/// * `uplink_bw` --- link bandwidth between rack switch and one top-level switch.
/// * `internal_bw` --- internal bandwidth of each host.
pub fn make_fat_tree_topology(
    sim: &mut Simulation,
    l2_switch_count: usize,
    l1_switch_count: usize,
    hosts_per_switch: usize,
    downlink_bw: f64,
    uplink_bw: f64,
    internal_bw: f64,
) -> Rc<RefCell<Network>> {
    let mut network = Network::new(Box::new(TopologyAwareNetworkModel::new()), sim.create_context("net"));

    for i in 0..l2_switch_count {
        let switch_name = format!("l2_switch_{}", i);
        network.add_node(
            &switch_name,
            Box::new(ConstantBandwidthNetworkModel::new(internal_bw, 0.)),
        );
    }

    for i in 0..l1_switch_count {
        let switch_name = format!("l1_switch_{}", i);
        network.add_node(
            &switch_name,
            Box::new(ConstantBandwidthNetworkModel::new(internal_bw, 0.)),
        );

        for j in 0..hosts_per_switch {
            let host_name = format!("host_{}_{}", i, j);
            network.add_node(
                &host_name,
                Box::new(ConstantBandwidthNetworkModel::new(internal_bw, 0.)),
            );
            network.add_link(&switch_name, &host_name, Link::shared(downlink_bw, 1e-4));
        }

        for j in 0..l2_switch_count {
            network.add_link(&switch_name, &format!("l2_switch_{}", j), Link::shared(uplink_bw, 1e-4));
        }
    }

    network.init_topology();
    let network_rc = Rc::new(RefCell::new(network));
    sim.add_handler("net", network_rc.clone());
    network_rc
}

/// Returns rack id for a given simulation component.
///
/// Works in linear time, consider using `get_all_racks` instead of calling this function for a large number of components.
pub fn get_rack(network: &Network, id: Id) -> Option<u64> {
    let id = network.get_location(id);
    network
        .get_nodes()
        .into_iter()
        .find(|s| network.get_node_id(s) == id)
        .and_then(|s| s.split('_').nth(1).and_then(|s| s.parse().ok()))
}

/// Returns rack id for all nodes in a network.
///
/// To find location of a simulation component using this map first call [get_location](Network::get_location).
pub fn get_all_racks(network: &Network) -> BTreeMap<NodeId, u64> {
    network
        .get_nodes()
        .into_iter()
        .filter(|s| s.starts_with("host_"))
        .map(|s| {
            (
                network.get_node_id(&s),
                s.split('_').nth(1).and_then(|s| s.parse().ok()).unwrap(),
            )
        })
        .collect()
}
