use std::collections::BTreeSet;

pub type DataId = u64;
pub type ChunkId = u64;

#[derive(Clone)]
pub struct HostInfo {
    pub free_space: u64,
    pub chunks: BTreeSet<ChunkId>,
}
