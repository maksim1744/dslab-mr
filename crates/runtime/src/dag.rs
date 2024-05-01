use std::collections::BTreeSet;

use dslab_dfs::dfs::DistributedFileSystem;

use super::data_item::DataItem;

pub trait Task {
    fn id(&self) -> u64;
    fn time(&self, input_size: u64) -> f64;
    fn output_size(&self, input_size: u64) -> u64;
}

pub struct SimpleTask {
    id: u64,
    time_per_byte: f64,
    output_size_ratio: f64,
}

impl SimpleTask {
    pub fn new(id: u64, time_per_byte: f64, output_size_ratio: f64) -> Self {
        SimpleTask {
            id,
            time_per_byte,
            output_size_ratio,
        }
    }
}

impl Task for SimpleTask {
    fn id(&self) -> u64 {
        self.id
    }

    fn time(&self, input_size: u64) -> f64 {
        self.time_per_byte * input_size as f64
    }

    fn output_size(&self, input_size: u64) -> u64 {
        (self.output_size_ratio * input_size as f64).round() as u64
    }
}

pub struct Stage {
    id: usize,
    tasks: Vec<Box<dyn Task>>,
    upload_result_to_dfs: bool,
}

impl Stage {
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn tasks(&self) -> &Vec<Box<dyn Task>> {
        &self.tasks
    }

    pub fn task(&self, task_id: usize) -> &dyn Task {
        &*self.tasks[task_id]
    }

    pub fn upload_result_to_dfs(&self) -> bool {
        self.upload_result_to_dfs
    }
}

pub trait Shuffle {
    fn shuffle(&self, input: &[DataItem], dfs: &DistributedFileSystem, output_tasks: usize) -> Vec<Vec<DataItem>>;
}

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

pub struct Connection {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    pub shuffle: Option<Box<dyn Shuffle>>,
}

pub struct Dag {
    initial_data: u64,
    stages: Vec<Stage>,
    connections: Vec<Connection>,
    stage_dependencies: Vec<Vec<Connection>>,
    ready_stages: BTreeSet<usize>,
    completed_stages: BTreeSet<usize>,
    need_dependencies: Vec<usize>,
    outgoing_connections: Vec<Vec<usize>>,
}

impl Dag {
    pub fn new() -> Self {
        Dag {
            initial_data: 0,
            stages: Vec::new(),
            connections: Vec::new(),
            stage_dependencies: Vec::new(),
            ready_stages: BTreeSet::new(),
            completed_stages: BTreeSet::new(),
            need_dependencies: Vec::new(),
            outgoing_connections: Vec::new(),
        }
    }

    pub fn with_initial_data(mut self, initial_data: u64) -> Self {
        self.initial_data = initial_data;
        self
    }

    pub fn initial_data(&self) -> u64 {
        self.initial_data
    }

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

    pub fn mark_started(&mut self, stage_id: usize) {
        self.ready_stages.remove(&stage_id);
    }

    pub fn mark_completed(&mut self, stage_id: usize) {
        self.ready_stages.remove(&stage_id);
        self.completed_stages.insert(stage_id);
        for &connection in self.outgoing_connections[stage_id].iter() {
            let to = self.connections[connection].to;
            self.need_dependencies[to] -= 1;
            if self.need_dependencies[to] == 0 {
                self.ready_stages.insert(to);
            }
        }
    }

    pub fn ready_stages(&self) -> &BTreeSet<usize> {
        &self.ready_stages
    }

    pub fn completed_stages(&self) -> &BTreeSet<usize> {
        &self.completed_stages
    }

    pub fn stages(&self) -> &Vec<Stage> {
        &self.stages
    }

    pub fn stage(&self, stage_id: usize) -> &Stage {
        &self.stages[stage_id]
    }

    pub fn connection(&self, connection_id: usize) -> &Connection {
        &self.connections[connection_id]
    }

    pub fn stage_dependencies(&self, stage_id: usize) -> &Vec<Connection> {
        &self.stage_dependencies[stage_id]
    }

    pub fn outgoing_connections(&self, stage_id: usize) -> &Vec<usize> {
        &self.outgoing_connections[stage_id]
    }
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
}
