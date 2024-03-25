use dslab_core::Id;

use crate::distributed_file_system::host_info::DataId;

#[derive(Debug, Clone, Copy)]
pub enum DataItem {
    Local {
        size: u64,
        host: Id,
    },
    #[allow(unused)]
    Replicated {
        size: u64,
        data_id: DataId,
    },
}

impl DataItem {
    pub fn size(&self) -> u64 {
        match self {
            DataItem::Local { size, .. } | DataItem::Replicated { size, .. } => *size,
        }
    }
}
