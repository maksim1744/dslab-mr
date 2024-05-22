//! Represents a data item.

use dslab_core::Id;

use dslab_dfs::host_info::{ChunkId, DataId};

/// Represents a data item.
#[derive(Debug, Clone, Copy)]
pub enum DataItem {
    /// Piece of local data on a host.
    Local { size: u64, host: Id },
    /// Data which was uploaded to DFS.
    Replicated { size: u64, data_id: DataId },
    /// One chunk from DFS.
    Chunk { size: u64, chunk_id: ChunkId },
}

impl DataItem {
    /// Returns size of a data item.
    pub fn size(&self) -> u64 {
        match self {
            DataItem::Local { size, .. } | DataItem::Replicated { size, .. } | DataItem::Chunk { size, .. } => *size,
        }
    }
}
