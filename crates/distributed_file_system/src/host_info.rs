//! Model of a host in DFS.
use std::collections::BTreeSet;

pub type DataId = u64;
pub type ChunkId = u64;

/// Model of a host in DFS.
#[derive(Clone)]
pub struct HostInfo {
    /// Amount of free space left on a host.
    pub free_space: u64,
    /// Set of all chunks on a host.
    pub chunks: BTreeSet<ChunkId>,
}
