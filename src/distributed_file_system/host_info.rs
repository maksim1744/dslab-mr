use std::collections::BTreeSet;

pub type DataId = u64;

pub struct HostInfo {
    pub free_memory: u64,
    pub data: BTreeSet<DataId>,
}
