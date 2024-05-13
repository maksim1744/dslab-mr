use std::collections::BTreeMap;

use dslab_core::Id;
use dslab_network::Network;

use super::host_info::{ChunkId, HostInfo};

pub trait ReplicationStrategy {
    fn register_chunks(
        &mut self,
        chunk_size: u64,
        host: Id,
        chunks: &[ChunkId],
        need_to_replicate: bool,
        host_info: &BTreeMap<Id, HostInfo>,
        network: &Network,
    ) -> BTreeMap<ChunkId, Vec<Id>>;
}
