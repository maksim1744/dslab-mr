use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    rc::Rc,
};

use dslab_core::{cast, log_debug, log_error, log_info, Event, EventHandler, Id, SimulationContext};
use dslab_network::{DataTransferCompleted, Network};
use serde::Serialize;

use crate::distributed_file_system::{
    dfs::{DistributedFileSystem, RegisterData, RegisteredData},
    host_info::{ChunkId, DataId},
};

use super::{
    map_reduce_params::{InitialDataLocation, MapReduceParams},
    placement_strategy::{PlacementStrategy, TaskType},
};

pub struct ComputeHostInfo {
    pub available_slots: usize,
}

pub struct InputWaitingMapTask {
    waiting_for_chunks: HashSet<ChunkId>,
    task_id: u64,
    input_size: u64,
}

pub struct InputWaitingReduceTask {
    registering_datas: HashSet<DataId>,
    registered_chunks: BTreeSet<ChunkId>,
    received_chunks: HashSet<ChunkId>,
}

#[derive(Clone, Serialize)]
pub struct Start {}

#[derive(Clone, Serialize)]
pub struct InitialDataPlacementCompleted {
    data_id: DataId,
}

#[derive(Clone, Serialize)]
pub struct MapTaskCompleted {
    task_id: u64,
    host: Id,
}

#[derive(Clone, Serialize)]
pub struct ReduceTaskCompleted {
    task_id: u64,
    host: Id,
}

pub struct MapReduceRunner {
    params: Box<dyn MapReduceParams>,
    placement_strategy: Box<dyn PlacementStrategy>,
    compute_host_info: BTreeMap<Id, ComputeHostInfo>,
    dfs: Rc<RefCell<DistributedFileSystem>>,
    network: Rc<RefCell<Network>>,
    next_data_id: DataId,
    initial_data_id: DataId,
    chunk_by_transfer_id: HashMap<usize, ChunkId>,
    map_queues: BTreeMap<Id, VecDeque<(u64, Vec<ChunkId>)>>,
    map_tasks_waiting_for_input: HashMap<usize, Rc<RefCell<InputWaitingMapTask>>>,
    map_tasks_left: u64,
    reduce_queues: BTreeMap<Id, VecDeque<u64>>,
    reduce_tasks: HashMap<u64, InputWaitingReduceTask>,
    reduce_tasks_waiting_for_data_register: HashMap<DataId, u64>,
    reduce_tasks_waiting_for_input: HashMap<usize, u64>,
    reduce_tasks_left: u64,
    reduce_uploads_left: u64,
    reduce_outputs: HashSet<DataId>,
    ctx: SimulationContext,
}

impl MapReduceRunner {
    pub fn new(
        params: Box<dyn MapReduceParams>,
        placement_strategy: Box<dyn PlacementStrategy>,
        compute_host_info: BTreeMap<Id, ComputeHostInfo>,
        dfs: Rc<RefCell<DistributedFileSystem>>,
        network: Rc<RefCell<Network>>,
        ctx: SimulationContext,
    ) -> Self {
        let next_data_id = dfs
            .borrow()
            .datas_chunks()
            .iter()
            .map(|(&k, _v)| k + 1)
            .max()
            .unwrap_or(0);
        let reduce_tasks_left = params.reduce_tasks_count();
        let reduce_uploads_left = reduce_tasks_left;
        MapReduceRunner {
            params,
            placement_strategy,
            compute_host_info,
            dfs,
            network,
            next_data_id,
            initial_data_id: DataId::MAX,
            chunk_by_transfer_id: HashMap::new(),
            map_queues: BTreeMap::new(),
            map_tasks_waiting_for_input: HashMap::new(),
            map_tasks_left: u64::MAX,
            reduce_queues: BTreeMap::new(),
            reduce_tasks: HashMap::new(),
            reduce_tasks_waiting_for_data_register: HashMap::new(),
            reduce_tasks_waiting_for_input: HashMap::new(),
            reduce_tasks_left,
            reduce_uploads_left,
            reduce_outputs: HashSet::new(),
            ctx,
        }
    }

    fn start(&mut self) {
        self.place_initial_data();
    }

    fn start_map(&mut self) {
        let dfs = self.dfs.borrow();
        let chunks = dfs.data_chunks(self.initial_data_id).unwrap();
        let assigned_chunks = self
            .placement_strategy
            .assign_chunks(chunks, self.params.map_tasks_count());
        if assigned_chunks.len() != chunks.len()
            || !chunks.iter().all(|chunk_id| assigned_chunks.contains_key(chunk_id))
        {
            log_error!(
                self.ctx,
                "placement strategy must assign all chunks: {:?}, assigned: {:?}",
                chunks,
                assigned_chunks
            );
            return;
        }
        let mut chunks_by_task: BTreeMap<u64, Vec<ChunkId>> = BTreeMap::new();
        for (chunk_id, task_id) in assigned_chunks {
            chunks_by_task.entry(task_id).or_default().push(chunk_id);
        }
        self.map_tasks_left = 0;
        for map_task in 0..self.params.map_tasks_count() {
            self.map_tasks_left += 1;
            let host = self.placement_strategy.place_task(
                TaskType::Map,
                map_task,
                chunks_by_task.get(&map_task).unwrap_or(&Vec::new()),
                dfs.chunks_location(),
                dfs.hosts_info(),
            );
            if !self.compute_host_info.contains_key(&host) {
                log_error!(self.ctx, "can't place task on host {} since it doesn't exist", host);
            }
            self.map_queues
                .entry(host)
                .or_default()
                .push_back((map_task, chunks_by_task.get(&map_task).cloned().unwrap_or_default()));
        }
        drop(dfs);
        let hosts = self.map_queues.keys().copied().collect::<Vec<_>>();
        for &host in hosts.iter() {
            self.process_map_queue(host);
        }
    }

    fn process_map_queue(&mut self, host: Id) {
        while self.compute_host_info.get(&host).unwrap().available_slots > 0
            && self
                .map_queues
                .get(&host)
                .map(|queue| !queue.is_empty())
                .unwrap_or(false)
        {
            self.compute_host_info.get_mut(&host).unwrap().available_slots -= 1;
            let (task_id, chunks) = self.map_queues.get_mut(&host).unwrap().pop_front().unwrap();
            let input_size = self.dfs.borrow().chunk_size() * chunks.len() as u64;
            let transfers = self.start_download_inputs(host, &chunks);
            let task = Rc::new(RefCell::new(InputWaitingMapTask {
                waiting_for_chunks: transfers
                    .iter()
                    .map(|transfer_id| self.chunk_by_transfer_id[transfer_id])
                    .collect(),
                task_id,
                input_size,
            }));
            for transfer in transfers {
                self.map_tasks_waiting_for_input.insert(transfer, task.clone());
            }
        }
    }

    fn start_reduce(&mut self) {
        let hosts = self.map_queues.keys().copied().collect::<Vec<_>>();
        for &host in hosts.iter() {
            self.process_reduce_queue(host);
        }
    }

    fn process_reduce_queue(&mut self, host: Id) {
        while self.compute_host_info.get(&host).unwrap().available_slots > 0
            && self
                .reduce_queues
                .get(&host)
                .map(|queue| !queue.is_empty())
                .unwrap_or(false)
        {
            self.compute_host_info.get_mut(&host).unwrap().available_slots -= 1;
            let task_id = self.reduce_queues.get_mut(&host).unwrap().pop_front().unwrap();
            let chunks = self.reduce_tasks[&task_id]
                .registered_chunks
                .iter()
                .copied()
                .collect::<Vec<_>>();
            let transfers = self.start_download_inputs(host, &chunks);
            for transfer in transfers {
                self.reduce_tasks_waiting_for_input.insert(transfer, task_id);
            }
        }
    }

    fn start_download_inputs(&mut self, host: Id, chunks: &[ChunkId]) -> HashSet<usize> {
        let mut transfers = HashSet::new();
        for &chunk_id in chunks.iter() {
            let source_host = self.closest_host_with_chunk(host, chunk_id).unwrap();
            let transfer_id = self.network.borrow_mut().transfer_data(
                source_host,
                host,
                self.dfs.borrow().chunk_size() as f64,
                self.ctx.id(),
            );
            self.chunk_by_transfer_id.insert(transfer_id, chunk_id);
            transfers.insert(transfer_id);
        }
        transfers
    }

    fn closest_host_with_chunk(&self, host: Id, chunk: ChunkId) -> Option<Id> {
        self.dfs
            .borrow()
            .chunks_location()
            .get(&chunk)?
            .iter()
            .copied()
            .map(|id| (self.network.borrow().latency(id, host), id))
            .min_by(|a, b| a.0.total_cmp(&b.0))
            .map(|(_latency, id)| id)
    }

    fn place_initial_data(&mut self) {
        match self.params.initial_data_location() {
            InitialDataLocation::UploadedToDFS { data_id } => {
                self.initial_data_id = data_id;
                self.ctx
                    .emit_now(InitialDataPlacementCompleted { data_id }, self.ctx.id());
            }
            InitialDataLocation::ExistsOnHost { host, size } => {
                self.initial_data_id = self.next_data_id;
                self.next_data_id += 1;
                self.ctx.emit_now(
                    RegisterData {
                        size,
                        host,
                        data_id: self.initial_data_id,
                        need_to_replicate: true,
                    },
                    self.dfs.borrow().id(),
                );
            }
        }
    }

    fn is_reduce_phase(&self) -> bool {
        self.map_tasks_left == 0
    }
}

impl EventHandler for MapReduceRunner {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.start();
            }
            RegisteredData { data_id } => {
                if data_id == self.initial_data_id {
                    log_debug!(self.ctx, "registered initial data: {}", data_id);
                    self.ctx
                        .emit_now(InitialDataPlacementCompleted { data_id }, self.ctx.id());
                } else if let Some(reduce_task_id) = self.reduce_tasks_waiting_for_data_register.remove(&data_id) {
                    log_debug!(
                        self.ctx,
                        "registered data {} for reduce task {}",
                        data_id,
                        reduce_task_id
                    );
                    let reduce_task = self.reduce_tasks.get_mut(&reduce_task_id).unwrap();
                    reduce_task.registering_datas.remove(&data_id);
                    for &chunk_id in self.dfs.borrow().data_chunks(data_id).unwrap() {
                        reduce_task.registered_chunks.insert(chunk_id);
                    }
                    let reduce_task = self.reduce_tasks.get(&reduce_task_id).unwrap();
                    if self.is_reduce_phase() && reduce_task.registering_datas.is_empty() {
                        let host = self.placement_strategy.place_task(
                            TaskType::Reduce,
                            reduce_task_id,
                            &reduce_task.registered_chunks.iter().copied().collect::<Vec<_>>(),
                            self.dfs.borrow().chunks_location(),
                            self.dfs.borrow().hosts_info(),
                        );
                        self.reduce_queues.entry(host).or_default().push_back(reduce_task_id);
                        self.process_reduce_queue(host);
                    }
                } else if self.reduce_outputs.contains(&data_id) {
                    log_debug!(self.ctx, "registered reduce output {}", data_id);
                    self.reduce_uploads_left -= 1;
                    if self.reduce_uploads_left == 0 {
                        log_info!(self.ctx, "map reduce completed");
                    }
                } else {
                    log_debug!(self.ctx, "registered unknown data {}", data_id);
                }
            }
            InitialDataPlacementCompleted { .. } => {
                self.start_map();
            }
            DataTransferCompleted { dt } => {
                if let Some(task) = self.map_tasks_waiting_for_input.remove(&dt.id) {
                    let mut task = task.borrow_mut();
                    let chunk_id = self.chunk_by_transfer_id.remove(&dt.id).unwrap();
                    log_debug!(
                        self.ctx,
                        "received input chunk {} for map task {}",
                        chunk_id,
                        task.task_id
                    );
                    task.waiting_for_chunks.remove(&chunk_id);
                    if task.waiting_for_chunks.is_empty() {
                        self.ctx.emit(
                            MapTaskCompleted {
                                task_id: task.task_id,
                                host: dt.dst,
                            },
                            self.ctx.id(),
                            self.params.map_task_time(task.task_id, task.input_size),
                        );
                    }
                } else if let Some(task_id) = self.reduce_tasks_waiting_for_input.remove(&dt.id) {
                    let task = self.reduce_tasks.get_mut(&task_id).unwrap();
                    let chunk_id = self.chunk_by_transfer_id.remove(&dt.id).unwrap();
                    log_debug!(
                        self.ctx,
                        "received input chunk {} for reduce task {}",
                        chunk_id,
                        task_id
                    );
                    task.received_chunks.insert(chunk_id);
                    if task.received_chunks.len() == task.registered_chunks.len() {
                        self.ctx.emit(
                            ReduceTaskCompleted { task_id, host: dt.dst },
                            self.ctx.id(),
                            self.params.reduce_task_time(
                                task_id,
                                task.received_chunks.len() as u64 * self.dfs.borrow().chunk_size(),
                            ),
                        );
                    }
                }
            }
            MapTaskCompleted { task_id, host } => {
                log_debug!(self.ctx, "map task {} completed on host {}", task_id, host);
                for output in self.params.map_task_output(task_id) {
                    let data_id = self.next_data_id;
                    self.next_data_id += 1;
                    self.ctx.emit_now(
                        RegisterData {
                            size: output.size,
                            host,
                            data_id,
                            need_to_replicate: true,
                        },
                        self.dfs.borrow().id(),
                    );
                    let reduce_task =
                        self.reduce_tasks
                            .entry(output.reducer_id)
                            .or_insert_with(|| InputWaitingReduceTask {
                                registering_datas: HashSet::new(),
                                registered_chunks: BTreeSet::new(),
                                received_chunks: HashSet::new(),
                            });
                    reduce_task.registering_datas.insert(data_id);
                    self.reduce_tasks_waiting_for_data_register
                        .insert(data_id, output.reducer_id);
                }

                self.compute_host_info.get_mut(&host).unwrap().available_slots += 1;
                self.process_map_queue(host);

                self.map_tasks_left -= 1;
                if self.map_tasks_left == 0 {
                    self.start_reduce();
                }
            }
            ReduceTaskCompleted { task_id, host } => {
                log_debug!(self.ctx, "reduce task {} completed on host {}", task_id, host);
                let data_id = self.next_data_id;
                self.next_data_id += 1;
                self.ctx.emit_now(
                    RegisterData {
                        size: self.params.reduce_task_output(task_id),
                        host,
                        data_id,
                        need_to_replicate: true,
                    },
                    self.dfs.borrow().id(),
                );
                self.reduce_outputs.insert(data_id);
                self.reduce_tasks_left -= 1;
            }
        })
    }
}
