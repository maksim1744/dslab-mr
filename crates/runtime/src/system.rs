use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NetworkConfig {
    Tree {
        star_count: usize,
        hosts_per_star: usize,
    },
    FatTree {
        l2_switch_count: usize,
        l1_switch_count: usize,
        hosts_per_switch: usize,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub struct HostConfig {
    pub name: String,
    pub speed: f64,
    pub available_space: u64,
    pub available_cores: u32,
    pub available_memory: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SystemConfig {
    pub network: NetworkConfig,
    pub chunk_size: u64,
    pub hosts: Vec<HostConfig>,
}
