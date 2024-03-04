use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    rc::Rc,
};

use serde::Serialize;

use dslab_core::{cast, event::EventId, log_debug, Event, EventHandler, Id, SimulationContext};
use dslab_network::{DataTransferCompleted, Network};

use super::{
    host_info::{DataId, HostInfo},
    replication_strategy::ReplicationStrategy,
};

struct ReplicationTask {
    origin: Id,
    targets: BTreeSet<Id>,
    remove_from_origin: bool,
    requester_id: Id,
}

struct CopyDataTask {
    data_id: DataId,
    requester_id: Id,
}

pub struct DistributedFileSystem {
    replication_strategy: Box<dyn ReplicationStrategy>,
    data_location: HashMap<DataId, BTreeSet<Id>>,
    data_size: HashMap<DataId, u64>,
    host_info: BTreeMap<Id, HostInfo>,
    replication_tasks: HashMap<u64, ReplicationTask>,
    next_task_id: u64,
    waiting_for_data_on_host: HashMap<EventId, u64>,
    waiting_for_remove_data: HashMap<EventId, u64>,
    waiting_for_data_transfer: HashMap<usize, CopyDataTask>,
    network: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

#[derive(Clone, Serialize)]
pub struct RegisterData {
    pub size: u64,
    pub host: Id,
    pub data_id: DataId,
    pub need_to_replicate: bool,
}

#[derive(Clone, Serialize)]
pub struct CopyData {
    pub src: Id,
    pub dst: Id,
    pub data_id: DataId,
}

#[derive(Clone, Serialize)]
pub struct CopiedData {
    pub src: Id,
    pub dst: Id,
    pub data_id: DataId,
}

#[derive(Clone, Serialize)]
pub struct RegisteredData {
    pub data_id: DataId,
}

#[derive(Clone, Serialize)]
pub struct EraseDataOnHost {
    pub host: Id,
    pub data_id: DataId,
}

#[derive(Clone, Serialize)]
pub struct UnknownHost {
    pub host: Id,
}

#[derive(Clone, Serialize)]
pub struct UnknownData {
    pub data_id: DataId,
}

#[derive(Clone, Serialize)]
pub struct NoSuchDataOnHost {
    pub host: Id,
    pub data_id: DataId,
}

#[derive(Clone, Serialize)]
pub struct NotEnoughMemory {
    pub host: Id,
    pub free_memory: u64,
    pub need_memory: u64,
}

impl EventHandler for DistributedFileSystem {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            RegisterData {
                size,
                host,
                data_id,
                need_to_replicate,
            } => {
                self.data_size.insert(data_id, size);
                if !self.can_add_data(host, data_id, event.src) {
                    return;
                }
                let mut target_hosts = self
                    .replication_strategy
                    .register_data(size, host, data_id, need_to_replicate, &self.host_info)
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                log_debug!(
                    self.ctx,
                    "registering data {} of size {} on host {} and replicating to {:?}",
                    data_id,
                    size,
                    host,
                    &target_hosts
                );
                self.data_location.entry(data_id).or_default().insert(host);
                let host_info = self.host_info.get_mut(&host);
                if host_info.is_none() {
                    self.ctx.emit_now(UnknownHost { host }, event.src);
                    return;
                }
                host_info.unwrap().data.insert(data_id);
                let remove_from_origin = !target_hosts.remove(&host);
                let task_id = self.next_task_id;
                self.next_task_id += 1;
                for &target_host in target_hosts.iter() {
                    let event_id = self.ctx.emit_now(
                        CopyData {
                            src: host,
                            dst: target_host,
                            data_id,
                        },
                        self.ctx.id(),
                    );
                    self.waiting_for_data_on_host.insert(event_id, task_id);
                }
                self.replication_tasks.insert(
                    task_id,
                    ReplicationTask {
                        origin: host,
                        targets: target_hosts,
                        remove_from_origin,
                        requester_id: event.src,
                    },
                );
            }
            CopyData { src, dst, data_id } => {
                if !self.data_exists(src, data_id, event.src) || !self.can_add_data(dst, data_id, event.src) {
                    return;
                }
                log_debug!(self.ctx, "copying data {} from {} to {}", data_id, src, dst);
                self.host_info.get_mut(&dst).unwrap().free_memory -= self.data_size[&data_id];
                let transfer_id =
                    self.network
                        .borrow_mut()
                        .transfer_data(src, dst, self.data_size[&data_id] as f64, self.ctx.id());
                self.waiting_for_data_transfer.insert(
                    transfer_id,
                    CopyDataTask {
                        data_id,
                        requester_id: event.src,
                    },
                );
            }
            CopiedData { src, dst, data_id } => {
                log_debug!(self.ctx, "copied data {} from {} to {}", data_id, src, dst);
                self.host_info.get_mut(&dst).unwrap().data.insert(data_id);
                self.data_location.entry(data_id).or_default().insert(dst);
                if let Some(task_id) = self.waiting_for_data_on_host.remove(&event.id) {
                    let task = self.replication_tasks.get_mut(&task_id).unwrap();
                    task.targets.remove(&dst);
                    if task.targets.is_empty() {
                        if task.remove_from_origin {
                            let event_id = self.ctx.emit_now(
                                EraseDataOnHost {
                                    data_id,
                                    host: task.origin,
                                },
                                self.ctx.id(),
                            );
                            self.waiting_for_remove_data.insert(event_id, task_id);
                        } else {
                            self.ctx.emit_now(RegisteredData { data_id }, task.requester_id);
                            self.replication_tasks.remove(&task_id);
                        }
                    }
                }
            }
            EraseDataOnHost { host, data_id } => {
                if !self.data_exists(host, data_id, event.src) {
                    return;
                }
                log_debug!(self.ctx, "erasing data {} from {}", data_id, host);
                self.host_info.get_mut(&host).unwrap().free_memory += self.data_size[&data_id];
                self.host_info.get_mut(&host).unwrap().data.remove(&data_id);
                if let Some(task_id) = self.waiting_for_remove_data.remove(&event.id) {
                    let task = self.replication_tasks.get_mut(&task_id).unwrap();
                    self.ctx.emit_now(RegisteredData { data_id }, task.requester_id);
                    self.replication_tasks.remove(&task_id);
                }
            }
            DataTransferCompleted { dt } => {
                if let Some(task) = self.waiting_for_data_transfer.remove(&dt.id) {
                    self.ctx.emit_now(
                        CopiedData {
                            src: dt.src,
                            dst: dt.dst,
                            data_id: task.data_id,
                        },
                        task.requester_id,
                    );
                }
            }
        })
    }
}

impl DistributedFileSystem {
    pub fn new(
        host_info: BTreeMap<Id, HostInfo>,
        data_size: HashMap<DataId, u64>,
        network: Rc<RefCell<Network>>,
        replication_strategy: Box<dyn ReplicationStrategy>,
        ctx: SimulationContext,
    ) -> Self {
        let mut data_location: HashMap<DataId, BTreeSet<Id>> = HashMap::new();
        for (&host, host_info) in host_info.iter() {
            for &data_id in host_info.data.iter() {
                data_location.entry(data_id).or_default().insert(host);
            }
        }
        Self {
            replication_strategy,
            data_location,
            data_size,
            host_info,
            replication_tasks: HashMap::new(),
            next_task_id: 0,
            waiting_for_data_on_host: HashMap::new(),
            waiting_for_remove_data: HashMap::new(),
            waiting_for_data_transfer: HashMap::new(),
            network,
            ctx,
        }
    }

    pub fn data_location(&self, data_id: DataId) -> Option<&BTreeSet<Id>> {
        self.data_location.get(&data_id)
    }

    fn data_exists(&self, host: Id, data_id: DataId, notify_id: Id) -> bool {
        if !self.host_info.contains_key(&host) {
            self.ctx.emit_now(UnknownHost { host }, notify_id);
            return false;
        }
        if !self.host_info[&host].data.contains(&data_id) {
            self.ctx.emit_now(NoSuchDataOnHost { host, data_id }, notify_id);
            return false;
        }
        true
    }

    fn can_add_data(&self, host: Id, data_id: DataId, notify_id: Id) -> bool {
        if !self.host_info.contains_key(&host) {
            self.ctx.emit_now(UnknownHost { host }, notify_id);
            return false;
        }
        if !self.data_size.contains_key(&data_id) {
            self.ctx.emit_now(UnknownData { data_id }, notify_id);
            return false;
        }
        if self.host_info[&host].free_memory < self.data_size[&data_id] {
            self.ctx.emit_now(
                NotEnoughMemory {
                    host,
                    free_memory: self.host_info[&host].free_memory,
                    need_memory: self.data_size[&data_id],
                },
                notify_id,
            );
            return false;
        }
        true
    }
}
