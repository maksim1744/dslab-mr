use std::collections::BTreeMap;

use dslab_core::Id;
use dslab_network::Network;

use crate::distributed_file_system::dfs::DistributedFileSystem;

use super::{
    compute_host_info::ComputeHostInfo,
    data_item::DataItem,
    graph::{Graph, Stage},
};

#[derive(Debug)]
pub struct TaskPlacement {
    pub host: Id,
    pub input: Vec<DataItem>,
}

pub trait PlacementStrategy {
    fn place_stage(
        &mut self,
        stage: &Stage,
        graph: &Graph,
        input_data: &[DataItem],
        input_data_shuffled: &[Vec<DataItem>],
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHostInfo>,
        network: &Network,
    ) -> Vec<TaskPlacement>;
}
