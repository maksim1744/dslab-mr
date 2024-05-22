//! Model of a DAG.

use std::collections::{BTreeMap, BTreeSet};

use dslab_dfs::dfs::DistributedFileSystem;

use super::data_item::DataItem;

/// Trait for a task.
pub trait Task {
    /// Task id.
    fn id(&self) -> u64;

    /// Number of cores needed for a task.
    fn cores(&self) -> u32;

    /// Amount of memory needed for a task.
    fn memory(&self) -> u64;

    /// Computational complexity of a task given size of input data.
    fn flops(&self, input_size: u64) -> f64;

    /// Size of the output data of a task given size of input data.
    fn output_size(&self, input_size: u64) -> u64;
}

/// Simple task which implements [Task] trait.
pub struct SimpleTask {
    /// Id of the task.
    pub id: u64,
    /// Number of cores needed for a task.
    pub cores: u32,
    /// Amount of memory needed for a task.
    pub memory: u64,
    /// Needed flops ber each byte of input data, used to implement [Task::flops].
    pub flops_per_byte: f64,
    /// Ratio of output size to input size, used to implement [Task::output_size].
    pub output_size_ratio: f64,
}

impl SimpleTask {
    /// Creates new task.
    pub fn new(id: u64, cores: u32, memory: u64, flops_per_byte: f64, output_size_ratio: f64) -> Self {
        SimpleTask {
            id,
            cores,
            memory,
            flops_per_byte,
            output_size_ratio,
        }
    }
}

impl Task for SimpleTask {
    fn id(&self) -> u64 {
        self.id
    }

    fn cores(&self) -> u32 {
        self.cores
    }

    fn memory(&self) -> u64 {
        self.memory
    }

    fn flops(&self, input_size: u64) -> f64 {
        self.flops_per_byte * input_size as f64
    }

    fn output_size(&self, input_size: u64) -> u64 {
        (self.output_size_ratio * input_size as f64).round() as u64
    }
}

/// Represents one stage of a DAG which corresponds to a single vertex.
pub struct Stage {
    id: usize,
    tasks: Vec<Box<dyn Task>>,
    upload_result_to_dfs: bool,
}

impl Stage {
    /// Id of the stage.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Information about tasks on a stage.
    pub fn tasks(&self) -> &Vec<Box<dyn Task>> {
        &self.tasks
    }

    /// Returns task by id.
    pub fn task(&self, task_id: usize) -> &dyn Task {
        &*self.tasks[task_id]
    }

    /// Whether to upload all task outputs to DFS or not and save the locally.
    pub fn upload_result_to_dfs(&self) -> bool {
        self.upload_result_to_dfs
    }
}

/// Trait which represents shuffle edge.
pub trait Shuffle {
    /// Gets input data for a stage and returns vector of size `output_tasks` with all input split into `output_tasks` parts.
    fn shuffle(&self, input: &[DataItem], dfs: &DistributedFileSystem, output_tasks: usize) -> Vec<Vec<DataItem>>;
}

/// Default implementation of [Shuffle] trait.
///
/// Shuffles data uniformly, that is, all input data is split into parts of size [chunk_size][DistributedFileSystem::chunk_size] and
/// then assigned one by one to tasks using round-robin algorithm.
pub struct UniformShuffle {}

impl Shuffle for UniformShuffle {
    fn shuffle(&self, input: &[DataItem], dfs: &DistributedFileSystem, output_tasks: usize) -> Vec<Vec<DataItem>> {
        let mut result = vec![vec![]; output_tasks];
        let mut next_task_id = 0;
        for &data_item in input.iter() {
            match data_item {
                DataItem::Chunk { .. } => unreachable!(),
                DataItem::Local { host, size } => {
                    for i in 0..output_tasks {
                        let current_size = (size + i as u64) / output_tasks as u64;
                        result[next_task_id].push(DataItem::Local {
                            size: current_size,
                            host,
                        });
                        next_task_id = (next_task_id + 1) % output_tasks;
                    }
                }
                DataItem::Replicated { data_id, .. } => {
                    for &chunk_id in dfs.data_chunks(data_id).unwrap() {
                        result[next_task_id].push(DataItem::Chunk {
                            size: dfs.chunk_size(),
                            chunk_id,
                        });
                        next_task_id = (next_task_id + 1) % output_tasks;
                    }
                }
            }
        }
        result
    }
}

/// Represent an edge between stages.
pub struct Connection {
    /// Id of the edge.
    pub id: usize,
    /// Start of an edge.
    pub from: usize,
    /// End of an edge.
    pub to: usize,
    /// Optional shuffle algorithm for an edge in case it is of shuffle type.
    pub shuffle: Option<Box<dyn Shuffle>>,
}

/// Represents DAG of stages.
pub struct Dag {
    initial_data: BTreeMap<usize, u64>,
    stages: Vec<Stage>,
    connections: Vec<Connection>,
    stage_dependencies: Vec<Vec<Connection>>,
    ready_stages: BTreeSet<usize>,
    running_stages: BTreeSet<usize>,
    completed_stages: BTreeSet<usize>,
    need_dependencies: Vec<usize>,
    outgoing_connections: Vec<Vec<usize>>,
}

impl Dag {
    /// Creates new empty DAG.
    pub fn new() -> Self {
        Dag {
            initial_data: BTreeMap::new(),
            stages: Vec::new(),
            connections: Vec::new(),
            stage_dependencies: Vec::new(),
            ready_stages: BTreeSet::new(),
            running_stages: BTreeSet::new(),
            completed_stages: BTreeSet::new(),
            need_dependencies: Vec::new(),
            outgoing_connections: Vec::new(),
        }
    }

    /// Adds initial data for a stage.
    pub fn add_initial_data(&mut self, stage_id: usize, initial_data: u64) {
        self.initial_data.insert(stage_id, initial_data);
    }

    /// Returns map of initial data sizes for each stage.
    pub fn initial_data(&self) -> &BTreeMap<usize, u64> {
        &self.initial_data
    }

    /// Add new [stage](Stage).
    pub fn add_stage(&mut self, tasks: Vec<Box<dyn Task>>, upload_result_to_dfs: bool) -> usize {
        self.stages.push(Stage {
            id: self.stages.len(),
            tasks,
            upload_result_to_dfs,
        });
        self.stage_dependencies.push(vec![]);
        self.ready_stages.insert(self.stages.len() - 1);
        self.need_dependencies.push(0);
        self.outgoing_connections.push(Vec::new());
        self.stages.len() - 1
    }

    /// Add new [connection](Connection) between stages.
    pub fn add_connection(&mut self, from: usize, to: usize, shuffle: Option<Box<dyn Shuffle>>) -> usize {
        self.connections.push(Connection {
            id: self.connections.len(),
            from,
            to,
            shuffle,
        });
        self.ready_stages.remove(&to);
        self.need_dependencies[to] += 1;
        self.outgoing_connections[from].push(self.connections.len() - 1);
        self.connections.len() - 1
    }

    /// Mark stage as started.
    pub fn mark_started(&mut self, stage_id: usize) {
        self.ready_stages.remove(&stage_id);
        self.running_stages.insert(stage_id);
    }

    /// Mark stage as completed.
    pub fn mark_completed(&mut self, stage_id: usize) {
        self.ready_stages.remove(&stage_id);
        self.running_stages.remove(&stage_id);
        self.completed_stages.insert(stage_id);
        for &connection in self.outgoing_connections[stage_id].iter() {
            let to = self.connections[connection].to;
            self.need_dependencies[to] -= 1;
            if self.need_dependencies[to] == 0 {
                self.ready_stages.insert(to);
            }
        }
    }

    /// Returns set of all stage ready to be started.
    pub fn ready_stages(&self) -> &BTreeSet<usize> {
        &self.ready_stages
    }

    /// Returns set of all running stage.
    pub fn running_stages(&self) -> &BTreeSet<usize> {
        &self.running_stages
    }

    /// Returns set of all completed stage.
    pub fn completed_stages(&self) -> &BTreeSet<usize> {
        &self.completed_stages
    }

    /// Returns vector of all stages.
    pub fn stages(&self) -> &Vec<Stage> {
        &self.stages
    }

    /// Returns stage by id.
    pub fn stage(&self, stage_id: usize) -> &Stage {
        &self.stages[stage_id]
    }

    /// Returns connection by id.
    pub fn connection(&self, connection_id: usize) -> &Connection {
        &self.connections[connection_id]
    }

    /// Returns stage dependencies.
    pub fn stage_dependencies(&self, stage_id: usize) -> &Vec<Connection> {
        &self.stage_dependencies[stage_id]
    }

    /// Returns stage outdoing connections.
    pub fn outgoing_connections(&self, stage_id: usize) -> &Vec<usize> {
        &self.outgoing_connections[stage_id]
    }
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
}
