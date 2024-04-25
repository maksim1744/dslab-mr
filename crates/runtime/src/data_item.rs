use dslab_core::Id;

use dslab_dfs::host_info::{ChunkId, DataId};

#[derive(Debug, Clone, Copy)]
pub enum DataItem {
    Local { size: u64, host: Id },
    Replicated { size: u64, data_id: DataId },
    Chunk { size: u64, chunk_id: ChunkId },
}

impl DataItem {
    pub fn size(&self) -> u64 {
        match self {
            DataItem::Local { size, .. } | DataItem::Replicated { size, .. } | DataItem::Chunk { size, .. } => *size,
        }
    }
}
