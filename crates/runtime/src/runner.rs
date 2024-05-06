use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    rc::Rc,
};

use dslab_core::{cast, log_debug, log_error, log_warn, Event, EventHandler, Id, SimulationContext};
use dslab_network::{DataTransferCompleted, Network};
use serde::Serialize;

use dslab_dfs::{
    dfs::{DistributedFileSystem, RegisterData, RegisteredData},
    host_info::{ChunkId, DataId},
};

use crate::placement_strategy::{DynamicPlacementStrategy, StageActions};

use super::{compute_host_info::ComputeHostInfo, dag::Dag, data_item::DataItem};

#[derive(Clone, Serialize)]
pub struct NewDag {
    #[serde(skip)]
    pub dag: Rc<RefCell<Dag>>,
    #[serde(skip)]
    pub initial_data: HashMap<usize, Vec<DataItem>>,
}

#[derive(Clone, Serialize)]
pub struct TaskCompleted {
    task_id: u64,
    host: Id,
}

#[derive(Debug)]
pub struct RunningTask {
    dag_id: usize,
    stage_id: usize,
    task_id: usize,
    input: Vec<DataItem>,
    waiting_for_transfers: HashSet<usize>,
}

impl RunningTask {
    fn input_size(&self) -> u64 {
        self.input.iter().map(|data_item| data_item.size()).sum()
    }
}

pub struct RunningStage {
    running_tasks: usize,
    outputs: Vec<DataItem>,
    waiting_for_replication: HashSet<DataId>,
}

pub struct Runner {
    placement_strategy: Box<dyn DynamicPlacementStrategy>,
    dags: Vec<Rc<RefCell<Dag>>>,
    compute_host_info: BTreeMap<Id, ComputeHostInfo>,
    dfs: Rc<RefCell<DistributedFileSystem>>,
    network: Rc<RefCell<Network>>,
    stage_input: HashMap<usize, BTreeMap<usize, Vec<DataItem>>>,
    stage_input_shuffled: HashMap<usize, BTreeMap<usize, Vec<Vec<DataItem>>>>,
    running_tasks: HashMap<u64, RunningTask>,
    next_running_task_id: u64,
    task_queues: BTreeMap<Id, VecDeque<u64>>,
    task_waiting_for_transfer: HashMap<usize, u64>,
    running_stages: HashMap<(usize, usize), RunningStage>,
    waiting_for_replication: HashMap<u64, (usize, usize)>,
    ctx: SimulationContext,
}

impl Runner {
    pub fn new(
        placement_strategy: Box<dyn DynamicPlacementStrategy>,
        compute_host_info: BTreeMap<Id, ComputeHostInfo>,
        dfs: Rc<RefCell<DistributedFileSystem>>,
        network: Rc<RefCell<Network>>,
        ctx: SimulationContext,
    ) -> Self {
        Runner {
            placement_strategy,
            dags: Vec::new(),
            compute_host_info,
            dfs,
            network,
            stage_input: HashMap::new(),
            stage_input_shuffled: HashMap::new(),
            running_tasks: HashMap::new(),
            next_running_task_id: 0,
            task_queues: BTreeMap::new(),
            task_waiting_for_transfer: HashMap::new(),
            running_stages: HashMap::new(),
            waiting_for_replication: HashMap::new(),
            ctx,
        }
    }

    fn process_scheduler_stage_actions(&mut self, dag_id: usize, actions: StageActions) -> BTreeSet<Id> {
        log_debug!(self.ctx, "got actions from scheduler: {:?}", actions);
        let dag = self.dags[dag_id].borrow();
        let mut affected_hosts = BTreeSet::new();
        let stage_id = actions.stage_id;
        if !dag.running_stages().contains(&stage_id) {
            log_error!(self.ctx, "stage {}.{} is not ready yet", dag_id, stage_id);
            return affected_hosts;
        }
        for placement in actions.task_placements.into_iter() {
            let task_id = placement.task_id;
            if !self.compute_host_info.contains_key(&placement.host) {
                log_error!(
                    self.ctx,
                    "can't place task {} on host {} since it doesn't exist",
                    task_id,
                    placement.host
                );
            }
            log_debug!(
                self.ctx,
                "placing task {}.{}.{} on host {}",
                dag_id,
                stage_id,
                task_id,
                placement.host
            );
            let mut input = placement.input;
            if let Some(input_shuffled) = self
                .stage_input_shuffled
                .get(&dag_id)
                .and_then(|input| input.get(&stage_id))
                .map(|stage_input| &stage_input[task_id])
            {
                input.extend(input_shuffled);
            }
            self.running_tasks.insert(
                self.next_running_task_id,
                RunningTask {
                    dag_id,
                    stage_id,
                    task_id,
                    input,
                    waiting_for_transfers: HashSet::new(),
                },
            );
            self.task_queues
                .entry(placement.host)
                .or_default()
                .push_back(self.next_running_task_id);
            self.next_running_task_id += 1;
            affected_hosts.insert(placement.host);
        }
        affected_hosts
    }

    fn process_scheduler_actions(&mut self, dag_id: usize, actions: Vec<StageActions>) {
        let mut affected_hosts = BTreeSet::new();
        for stage_action in actions.into_iter() {
            affected_hosts.extend(self.process_scheduler_stage_actions(dag_id, stage_action));
        }
        for host in affected_hosts.into_iter() {
            self.process_tasks_queue(host);
        }
    }

    fn start_stage(&mut self, dag_id: usize, stage_id: usize) {
        log_debug!(self.ctx, "starting stage {}.{}", dag_id, stage_id);
        if self
            .stage_input
            .get(&dag_id)
            .and_then(|dag_input| dag_input.get(&stage_id))
            .is_none()
            && self
                .stage_input_shuffled
                .get(&dag_id)
                .and_then(|dag_input| dag_input.get(&stage_id))
                .is_none()
        {
            log_error!(self.ctx, "no input found for stage {}.{}", dag_id, stage_id);
            return;
        }
        self.dags[dag_id].borrow_mut().mark_started(stage_id);
        let dag = self.dags[dag_id].borrow();
        self.running_stages.insert(
            (dag_id, stage_id),
            RunningStage {
                running_tasks: dag.stage(stage_id).tasks().len(),
                outputs: Vec::new(),
                waiting_for_replication: HashSet::new(),
            },
        );

        let actions = self.placement_strategy.on_stage_ready(
            stage_id,
            &dag,
            self.stage_input.get(&dag_id).unwrap_or(&BTreeMap::new()),
            self.stage_input_shuffled.get(&dag_id).unwrap_or(&BTreeMap::new()),
            &self.dfs.borrow(),
            &self.compute_host_info,
            &self.network.borrow(),
        );
        drop(dag);
        self.process_scheduler_actions(dag_id, actions);
    }

    fn new_dag(&mut self, dag: Rc<RefCell<Dag>>) {
        self.dags.push(dag);
        self.process_ready_stages(self.dags.len() - 1);
    }

    fn process_tasks_queue(&mut self, host: Id) {
        while self.compute_host_info.get(&host).unwrap().available_slots > 0
            && self
                .task_queues
                .get(&host)
                .map(|queue| !queue.is_empty())
                .unwrap_or(false)
        {
            self.compute_host_info.get_mut(&host).unwrap().available_slots -= 1;
            let task_id = self.task_queues.get_mut(&host).unwrap().pop_front().unwrap();
            let running_task = &self.running_tasks[&task_id];
            let mut transfers = HashSet::new();
            log_debug!(
                self.ctx,
                "starting task {}.{}.{} on host {}",
                running_task.dag_id,
                running_task.stage_id,
                running_task.task_id,
                host
            );

            for data_item in running_task.input.iter() {
                match *data_item {
                    DataItem::Local {
                        size,
                        host: source_host,
                    } => {
                        let transfer_id =
                            self.network
                                .borrow_mut()
                                .transfer_data(source_host, host, size as f64, self.ctx.id());
                        transfers.insert(transfer_id);
                    }
                    DataItem::Replicated { data_id, .. } => {
                        for &chunk_id in self.dfs.borrow().data_chunks(data_id).unwrap() {
                            let source_host = self.closest_host_with_chunk(host, chunk_id).unwrap();
                            let transfer_id = self.network.borrow_mut().transfer_data(
                                source_host,
                                host,
                                self.dfs.borrow().chunk_size() as f64,
                                self.ctx.id(),
                            );
                            transfers.insert(transfer_id);
                        }
                    }
                    DataItem::Chunk { chunk_id, .. } => {
                        let source_host = self.closest_host_with_chunk(host, chunk_id).unwrap();
                        let transfer_id = self.network.borrow_mut().transfer_data(
                            source_host,
                            host,
                            self.dfs.borrow().chunk_size() as f64,
                            self.ctx.id(),
                        );
                        transfers.insert(transfer_id);
                    }
                }
            }

            for &transfer_id in transfers.iter() {
                self.task_waiting_for_transfer.insert(transfer_id, task_id);
            }
            self.running_tasks.get_mut(&task_id).unwrap().waiting_for_transfers = transfers;
        }
    }

    fn stage_completed(&mut self, dag_id: usize, stage_id: usize) {
        log_debug!(self.ctx, "dag {}: stage {} completed", dag_id, stage_id);
        self.dags[dag_id].borrow_mut().mark_completed(stage_id);
        let dag = self.dags[dag_id].borrow();
        if dag.completed_stages().len() == dag.stages().len() {
            log_debug!(self.ctx, "dag {} finished", dag_id);
        }
        let running_stage = self.running_stages.remove(&(dag_id, stage_id)).unwrap();
        for &connection_id in dag.outgoing_connections(stage_id).iter() {
            let outputs = &running_stage.outputs;
            if let Some(shuffle) = &dag.connection(connection_id).shuffle {
                let shuffled_outputs = shuffle.shuffle(
                    outputs,
                    &self.dfs.borrow(),
                    dag.stage(dag.connection(connection_id).to).tasks().len(),
                );
                let stage_input = self
                    .stage_input_shuffled
                    .entry(dag_id)
                    .or_default()
                    .entry(dag.connection(connection_id).to)
                    .or_insert(vec![vec![]; shuffled_outputs.len()]);
                for (i, data_items) in shuffled_outputs.into_iter().enumerate() {
                    stage_input[i].extend(data_items);
                }
            } else {
                self.stage_input
                    .entry(dag_id)
                    .or_default()
                    .entry(dag.connection(connection_id).to)
                    .or_default()
                    .extend(outputs);
            }
        }

        let actions = self.placement_strategy.on_stage_completed(
            stage_id,
            &dag,
            self.stage_input.get(&dag_id).unwrap_or(&BTreeMap::new()),
            self.stage_input_shuffled.get(&dag_id).unwrap_or(&BTreeMap::new()),
            &self.dfs.borrow(),
            &self.compute_host_info,
            &self.network.borrow(),
        );
        drop(dag);
        self.process_scheduler_actions(dag_id, actions);

        self.process_ready_stages(dag_id);
    }

    fn process_ready_stages(&mut self, dag_id: usize) {
        let ready_stages = self.dags[dag_id].borrow().ready_stages().clone();
        for stage in ready_stages.into_iter() {
            self.start_stage(dag_id, stage);
        }
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
}

impl EventHandler for Runner {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            NewDag { dag, initial_data } => {
                log_debug!(self.ctx, "got new dag with {} stages", dag.borrow().stages().len());
                let dag_id = self.dags.len();
                self.stage_input
                    .entry(dag_id)
                    .or_default()
                    .extend(initial_data.into_iter());
                self.new_dag(dag);
            }
            RegisteredData { data_id } => {
                if let Some((dag_id, stage_id)) = self.waiting_for_replication.remove(&data_id) {
                    let running_stage = self.running_stages.get_mut(&(dag_id, stage_id)).unwrap();
                    running_stage.waiting_for_replication.remove(&data_id);
                    if running_stage.running_tasks == 0 && running_stage.waiting_for_replication.is_empty() {
                        self.stage_completed(dag_id, stage_id);
                    }
                } else {
                    log_warn!(self.ctx, "registered unknown data {}", data_id);
                }
            }
            DataTransferCompleted { dt } => {
                if let Some(task_id) = self.task_waiting_for_transfer.remove(&dt.id) {
                    let task = self.running_tasks.get_mut(&task_id).unwrap();
                    log_debug!(
                        self.ctx,
                        "received input data_item for task {}.{}.{}",
                        task.dag_id,
                        task.stage_id,
                        task.task_id
                    );
                    task.waiting_for_transfers.remove(&dt.id);
                    if task.waiting_for_transfers.is_empty() {
                        log_debug!(
                            self.ctx,
                            "started execution of task {}.{}.{} with input size {}",
                            task.dag_id,
                            task.stage_id,
                            task.task_id,
                            task.input_size()
                        );
                        self.ctx.emit(
                            TaskCompleted { task_id, host: dt.dst },
                            self.ctx.id(),
                            self.dags[task.dag_id]
                                .borrow()
                                .stage(task.stage_id)
                                .task(task.task_id)
                                .time(task.input_size()),
                        );
                    }
                }
            }
            TaskCompleted { task_id, host } => {
                let task = &self.running_tasks[&task_id];
                log_debug!(
                    self.ctx,
                    "task {}.{}.{} completed on host {}",
                    task.dag_id,
                    task.stage_id,
                    task.task_id,
                    host
                );
                let running_stage = self.running_stages.get_mut(&(task.dag_id, task.stage_id)).unwrap();
                running_stage.running_tasks -= 1;
                let dag = self.dags[task.dag_id].borrow();
                let output_size = dag
                    .stage(task.stage_id)
                    .task(task.task_id)
                    .output_size(task.input_size());
                if dag.stage(task.stage_id).upload_result_to_dfs() {
                    let data_id = self.dfs.borrow_mut().next_data_id();
                    self.ctx.emit_now(
                        RegisterData {
                            size: output_size,
                            host,
                            data_id,
                            need_to_replicate: true,
                        },
                        self.dfs.borrow().id(),
                    );
                    running_stage.waiting_for_replication.insert(data_id);
                    self.waiting_for_replication
                        .insert(data_id, (task.dag_id, task.stage_id));
                } else {
                    running_stage.outputs.push(DataItem::Local {
                        size: output_size,
                        host,
                    });
                }
                drop(dag);
                self.compute_host_info.get_mut(&host).unwrap().available_slots += 1;

                let actions = self.placement_strategy.on_task_completed(
                    task.stage_id,
                    task.task_id,
                    &self.dags[task.dag_id].borrow(),
                    self.stage_input.get(&task.dag_id).unwrap_or(&BTreeMap::new()),
                    self.stage_input_shuffled.get(&task.dag_id).unwrap_or(&BTreeMap::new()),
                    &self.dfs.borrow(),
                    &self.compute_host_info,
                    &self.network.borrow(),
                );
                self.process_scheduler_actions(task.dag_id, actions);

                let task = &self.running_tasks[&task_id];
                let running_stage = self.running_stages.get(&(task.dag_id, task.stage_id)).unwrap();
                if running_stage.running_tasks == 0 && running_stage.waiting_for_replication.is_empty() {
                    self.stage_completed(task.dag_id, task.stage_id);
                }
            }
        })
    }
}
