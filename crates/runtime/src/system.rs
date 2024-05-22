//! Represents cluster configuration.

use serde::{Deserialize, Serialize};

/// Represents config for a network.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NetworkConfig {
    /// Represents tree network, see [make_tree_topology](dslab_dfs::network::make_tree_topology).
    Tree {
        star_count: usize,
        hosts_per_star: usize,
        downlink_bw: f64,
        internal_bw: f64,
    },
    /// Represents fat tree network, see [make_fat_tree_topology](dslab_dfs::network::make_fat_tree_topology).
    FatTree {
        l2_switch_count: usize,
        l1_switch_count: usize,
        hosts_per_switch: usize,
        downlink_bw: f64,
        internal_bw: f64,
    },
}

/// Represents one host.
#[derive(Clone, Serialize, Deserialize)]
pub struct HostConfig {
    /// Host name or "default", see [SystemConfig::hosts] for more.
    pub name: String,
    /// Speed of a single core.
    pub speed: f64,
    /// Total space on a host.
    pub available_space: u64,
    /// Total number of cores on a host.
    pub available_cores: u32,
    /// Total memory on a host.
    pub available_memory: u64,
}

/// Represents cluster.
#[derive(Clone, Serialize, Deserialize)]
pub struct SystemConfig {
    /// Network of a cluster.
    pub network: NetworkConfig,
    /// Chunk size of [DFS](dslab_dfs::dfs::DistributedFileSystem).
    pub chunk_size: u64,
    /// Information about hosts.
    ///
    /// Each host must be named in the format `host_{rack_id}_{host_id}` where both ids start from 0 and `host_id`
    /// is independend on each rack. If there is a config with name "default" it will be used as config for all
    /// unspecified hosts.
    pub hosts: Vec<HostConfig>,
}
