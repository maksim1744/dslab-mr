//! Trait for a replication strategy.

use std::collections::BTreeMap;

use dslab_core::Id;
use dslab_network::Network;

use super::host_info::{ChunkId, HostInfo};

/// Trait for a replication strategy.
pub trait ReplicationStrategy {
    /// Function which is called once per every [RegisterData](super::dfs::RegisterData) event.
    /// * `chunk_size` --- chunk size of the DFS.
    /// * `host` --- host with the data.
    /// * `chunks` --- chunk ids which the data was split into.
    /// * `need_to_replicate` --- parameter passed from the [RegisterData](super::dfs::RegisterData) event.
    /// * `host_info` --- information about hosts in the system, including remaining free space on each one.
    /// * `network` --- network of a system.
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
