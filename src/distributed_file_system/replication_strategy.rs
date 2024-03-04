use std::collections::BTreeMap;

use dslab_core::Id;

use super::host_info::{DataId, HostInfo};

pub trait ReplicationStrategy {
    fn register_data(
        &mut self,
        size: u64,
        host: Id,
        data_id: DataId,
        need_to_replicate: bool,
        host_info: &BTreeMap<Id, HostInfo>,
    ) -> Vec<Id>;
}
