use std::collections::{BTreeMap, BTreeSet, HashMap};

use dslab_core::Id;

use crate::distributed_file_system::host_info::{ChunkId, HostInfo};

#[derive(Copy, Clone, Debug)]
pub enum TaskType {
    Map,
    Reduce,
}

pub trait PlacementStrategy {
    fn assign_chunks(&mut self, chunks: &[ChunkId], map_tasks_count: u64) -> BTreeMap<ChunkId, u64>;

    fn place_task(
        &mut self,
        task_type: TaskType,
        task_id: u64,
        input_chunks: &[ChunkId],
        chunks_location: &HashMap<ChunkId, BTreeSet<Id>>,
        host_info: &BTreeMap<Id, HostInfo>,
    ) -> Id;
}
