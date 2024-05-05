use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    rc::Rc,
};

use serde::Serialize;

use dslab_core::{cast, log_debug, Event, EventHandler, Id, SimulationContext};
use dslab_network::{DataTransferCompleted, Network};

use super::{
    host_info::{ChunkId, DataId, HostInfo},
    replication_strategy::ReplicationStrategy,
};

struct ReplicationTask {
    data_id: DataId,
    targets: BTreeSet<(ChunkId, Id)>,
    requester_id: Id,
}

struct CopyChunkTask {
    chunk_id: ChunkId,
    requester_id: Id,
}

pub struct DistributedFileSystem {
    replication_strategy: Box<dyn ReplicationStrategy>,
    chunk_size: u64,
    chunks_location: HashMap<ChunkId, BTreeSet<Id>>,
    data_chunks: HashMap<DataId, Vec<ChunkId>>,
    host_info: BTreeMap<Id, HostInfo>,
    replication_tasks: HashMap<u64, ReplicationTask>,
    next_task_id: u64,
    waiting_for_chunk_on_host: HashMap<(ChunkId, Id), Vec<u64>>,
    waiting_for_data_transfer: HashMap<usize, CopyChunkTask>,
    next_chunk_id: ChunkId,
    network: Rc<RefCell<Network>>,
    next_data_id: u64,
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
pub struct UploadChunk {
    pub src: Id,
    pub dst: Id,
    pub chunk_id: ChunkId,
}

#[derive(Clone, Serialize)]
pub struct CopyChunk {
    pub src: Id,
    pub dst: Id,
    pub chunk_id: ChunkId,
}

#[derive(Clone, Serialize)]
pub struct CopiedChunk {
    pub src: Id,
    pub dst: Id,
    pub chunk_id: ChunkId,
}

#[derive(Clone, Serialize)]
pub struct RegisteredData {
    pub data_id: DataId,
}

#[derive(Clone, Serialize)]
pub struct EraseChunkOnHost {
    pub host: Id,
    pub chunk_id: ChunkId,
}

#[derive(Clone, Serialize)]
pub struct UnknownHost {
    pub host: Id,
}

#[derive(Clone, Serialize)]
pub struct UnknownData {
    pub chunk_id: ChunkId,
}

#[derive(Clone, Serialize)]
pub struct NoSuchChunkOnHost {
    pub host: Id,
    pub chunk_id: ChunkId,
}

#[derive(Clone, Serialize)]
pub struct NotEnoughSpace {
    pub host: Id,
    pub free_space: u64,
    pub need_space: u64,
}

#[derive(Clone, Serialize)]
pub struct ChunkAlreadyExists {
    pub host: Id,
    pub chunk_id: ChunkId,
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
                let chunks_count = (size + self.chunk_size - 1) / self.chunk_size;
                let mut chunks = Vec::new();
                for _ in 0..chunks_count {
                    chunks.push(self.next_chunk_id);
                    self.next_chunk_id += 1;
                }
                let target_hosts = self.replication_strategy.register_chunks(
                    self.chunk_size,
                    host,
                    &chunks,
                    need_to_replicate,
                    &self.host_info,
                );
                log_debug!(
                    self.ctx,
                    "registering data {} of size {} on host {} by splitting into chunks {:?} and replicating to {:?}",
                    data_id,
                    size,
                    host,
                    chunks,
                    &target_hosts
                );
                let host_info = self.host_info.get_mut(&host);
                if host_info.is_none() {
                    self.ctx.emit_now(UnknownHost { host }, event.src);
                    return;
                }
                self.next_data_id = self.next_data_id.max(data_id + 1);
                let task_id = self.next_task_id;
                self.next_task_id += 1;
                for (&chunk_id, target_hosts) in target_hosts.iter() {
                    for &target_host in target_hosts.iter() {
                        self.ctx.emit_now(
                            UploadChunk {
                                src: host,
                                dst: target_host,
                                chunk_id,
                            },
                            self.ctx.id(),
                        );
                        self.waiting_for_chunk_on_host
                            .entry((chunk_id, target_host))
                            .or_default()
                            .push(task_id);
                    }
                }
                self.data_chunks.insert(data_id, chunks.clone());
                self.replication_tasks.insert(
                    task_id,
                    ReplicationTask {
                        data_id,
                        targets: target_hosts
                            .into_iter()
                            .flat_map(|(chunk_id, target_hosts)| {
                                target_hosts.into_iter().map(move |host| (chunk_id, host))
                            })
                            .collect(),
                        requester_id: event.src,
                    },
                );
            }
            CopyChunk { src, dst, chunk_id } => {
                if !self.chunk_exists(src, chunk_id, event.src) || !self.can_add_chunk(dst, chunk_id, event.src) {
                    return;
                }
                log_debug!(self.ctx, "copying chunk {} from {} to {}", chunk_id, src, dst);
                self.copy_chunk(src, dst, chunk_id, event.src);
            }
            UploadChunk { src, dst, chunk_id } => {
                if !self.can_add_chunk(dst, chunk_id, event.src) {
                    return;
                }
                log_debug!(self.ctx, "uploading chunk {} from {} to {}", chunk_id, src, dst);
                self.copy_chunk(src, dst, chunk_id, event.src);
            }
            CopiedChunk { src, dst, chunk_id } => {
                log_debug!(self.ctx, "copied chunk {} from {} to {}", chunk_id, src, dst);
                self.host_info.get_mut(&dst).unwrap().chunks.insert(chunk_id);
                self.chunks_location.entry(chunk_id).or_default().insert(dst);
                for &task_id in self
                    .waiting_for_chunk_on_host
                    .remove(&(chunk_id, dst))
                    .iter()
                    .flat_map(|x| x.iter())
                {
                    let task = self.replication_tasks.get_mut(&task_id).unwrap();
                    task.targets.remove(&(chunk_id, dst));
                    if task.targets.is_empty() {
                        self.ctx
                            .emit_now(RegisteredData { data_id: task.data_id }, task.requester_id);
                        self.replication_tasks.remove(&task_id);
                    }
                }
            }
            EraseChunkOnHost { host, chunk_id } => {
                if !self.chunk_exists(host, chunk_id, event.src) {
                    return;
                }
                log_debug!(self.ctx, "erasing chunk {} from {}", chunk_id, host);
                self.host_info.get_mut(&host).unwrap().free_space += self.chunk_size;
                self.host_info.get_mut(&host).unwrap().chunks.remove(&chunk_id);
                self.chunks_location.get_mut(&chunk_id).unwrap().remove(&host);
            }
            DataTransferCompleted { dt } => {
                if let Some(task) = self.waiting_for_data_transfer.remove(&dt.id) {
                    self.ctx.emit_now(
                        CopiedChunk {
                            src: dt.src,
                            dst: dt.dst,
                            chunk_id: task.chunk_id,
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
        data_chunks: HashMap<DataId, Vec<ChunkId>>,
        network: Rc<RefCell<Network>>,
        replication_strategy: Box<dyn ReplicationStrategy>,
        chunk_size: u64,
        ctx: SimulationContext,
    ) -> Self {
        let mut chunks_location: HashMap<DataId, BTreeSet<Id>> = HashMap::new();
        let mut next_chunk_id = 0;
        for (&host, host_info) in host_info.iter() {
            for &chunk_id in host_info.chunks.iter() {
                chunks_location.entry(chunk_id).or_default().insert(host);
                next_chunk_id = next_chunk_id.max(chunk_id + 1);
            }
        }
        Self {
            replication_strategy,
            chunk_size,
            chunks_location,
            data_chunks,
            host_info,
            replication_tasks: HashMap::new(),
            next_task_id: 0,
            waiting_for_chunk_on_host: HashMap::new(),
            waiting_for_data_transfer: HashMap::new(),
            next_chunk_id,
            network,
            next_data_id: 0,
            ctx,
        }
    }

    pub fn id(&self) -> Id {
        self.ctx.id()
    }

    pub fn data_chunks(&self, data_id: DataId) -> Option<&Vec<ChunkId>> {
        self.data_chunks.get(&data_id)
    }

    pub fn datas_chunks(&self) -> &HashMap<DataId, Vec<ChunkId>> {
        &self.data_chunks
    }

    pub fn chunks_location(&self) -> &HashMap<ChunkId, BTreeSet<Id>> {
        &self.chunks_location
    }

    pub fn chunk_location(&self, chunk_id: ChunkId) -> Option<&BTreeSet<Id>> {
        self.chunks_location.get(&chunk_id)
    }

    pub fn hosts_info(&self) -> &BTreeMap<Id, HostInfo> {
        &self.host_info
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub fn next_data_id(&mut self) -> u64 {
        self.next_data_id += 1;
        self.next_data_id - 1
    }

    fn chunk_exists(&self, host: Id, chunk_id: ChunkId, notify_id: Id) -> bool {
        if !self.host_info.contains_key(&host) {
            self.ctx.emit_now(UnknownHost { host }, notify_id);
            return false;
        }
        if !self.host_info[&host].chunks.contains(&chunk_id) {
            self.ctx.emit_now(NoSuchChunkOnHost { host, chunk_id }, notify_id);
            return false;
        }
        true
    }

    fn can_add_chunk(&self, host: Id, chunk_id: ChunkId, notify_id: Id) -> bool {
        if !self.host_info.contains_key(&host) {
            self.ctx.emit_now(UnknownHost { host }, notify_id);
            return false;
        }
        if self.host_info[&host].chunks.contains(&chunk_id) {
            self.ctx.emit_now(ChunkAlreadyExists { host, chunk_id }, notify_id);
            return false;
        }
        if self.host_info[&host].free_space < self.chunk_size {
            self.ctx.emit_now(
                NotEnoughSpace {
                    host,
                    free_space: self.host_info[&host].free_space,
                    need_space: self.chunk_size,
                },
                notify_id,
            );
            return false;
        }
        true
    }

    fn copy_chunk(&mut self, src: Id, dst: Id, chunk_id: ChunkId, requester_id: Id) {
        self.host_info.get_mut(&dst).unwrap().free_space -= self.chunk_size;
        let transfer_id = self
            .network
            .borrow_mut()
            .transfer_data(src, dst, self.chunk_size as f64, self.ctx.id());
        self.waiting_for_data_transfer
            .insert(transfer_id, CopyChunkTask { chunk_id, requester_id });
    }
}
