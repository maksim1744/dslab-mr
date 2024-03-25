use std::collections::{BTreeMap, BTreeSet, HashMap};

use dslab_core::Id;

use crate::distributed_file_system::host_info::{ChunkId, DataId, HostInfo};

use super::{compute_host_info::ComputeHostInfo, data_item::DataItem};

#[derive(Debug)]
pub struct MapTaskPlacement {
    pub host: Id,
    pub chunks: Vec<ChunkId>,
}

pub trait PlacementStrategy {
    fn place_map_tasks(
        &mut self,
        task_count: u64,
        input_chunks: &[ChunkId],
        chunks_location: &HashMap<ChunkId, BTreeSet<Id>>,
        host_info: &BTreeMap<Id, HostInfo>,
        compute_host_info: &BTreeMap<Id, ComputeHostInfo>,
    ) -> Vec<MapTaskPlacement>;

    fn place_reduce_task(
        &mut self,
        task_id: u64,
        input_data_items: &[DataItem],
        data_chunks: &HashMap<DataId, Vec<ChunkId>>,
        chunks_location: &HashMap<ChunkId, BTreeSet<Id>>,
        host_info: &BTreeMap<Id, HostInfo>,
        compute_host_info: &BTreeMap<Id, ComputeHostInfo>,
    ) -> Id;
}
