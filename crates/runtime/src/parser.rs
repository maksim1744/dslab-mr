//! Tools for loading plans and dags from YAML files.

use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
};

use serde::{Deserialize, Serialize};

use crate::{
    cluster_simulation::{DagPlan, GlobalInputPlan, InputPlan, SimulationPlan},
    system::SystemConfig,
};

use super::dag::{Dag, Shuffle, SimpleTask, Task, UniformShuffle};

/// Struct representing [SimpleTask], see parameter description there.
#[derive(Serialize, Deserialize)]
pub struct YamlTask {
    pub cores: u32,
    pub memory: u64,
    pub flops_per_byte: f64,
    pub output_size_ratio: f64,
}

/// Struct representing [Connection](super::dag::Connection).
#[derive(Serialize, Deserialize)]
pub struct YamlConnection {
    /// Start of an edge.
    pub from: usize,
    /// Shuffle type for an edge of a shuffle type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shuffle: Option<ShuffleType>,
}

fn is_false(b: &bool) -> bool {
    !b
}

/// Struct representing a [Stage](super::dag::Stage).
#[derive(Serialize, Deserialize)]
pub struct YamlStage {
    /// Information about stage tasks.
    pub tasks: Vec<YamlTask>,
    /// Whether to upload task outputs to DFS.
    #[serde(default, skip_serializing_if = "is_false")]
    pub upload_result_to_dfs: bool,
    /// Incoming connections.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<YamlConnection>,
}

/// Possible shuffle types.
#[derive(Serialize, Deserialize)]
pub enum ShuffleType {
    /// [UniformShuffle].
    Simple,
}

/// Information about input data for a stage.
#[derive(Serialize, Deserialize)]
pub struct StageInitialData {
    /// Stage id.
    pub stage: usize,
    /// Size of the data.
    pub size: u64,
}

/// YAML representation of a DAG.
#[derive(Serialize, Deserialize)]
pub struct YamlDag {
    /// Information about initial data for input stages.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initial_data: Vec<StageInitialData>,
    /// Information about stages.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stages: Vec<YamlStage>,
}

impl Dag {
    /// Read [Dag] from YAML file. Panic on error.
    pub fn from_yaml<P: AsRef<Path>>(file: P) -> Self {
        let yaml: YamlDag = serde_yaml::from_str(
            &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display())),
        )
        .unwrap_or_else(|e| panic!("Can't parse YAML from file {}: {e:?}", file.as_ref().display()));

        let mut dag = Dag::new();
        for data in yaml.initial_data.into_iter() {
            dag.add_initial_data(data.stage, data.size);
        }
        for (stage_id, stage) in yaml.stages.into_iter().enumerate() {
            let mut tasks = Vec::new();
            for (task_id, task) in stage.tasks.into_iter().enumerate() {
                tasks.push(Box::new(SimpleTask::new(
                    task_id as u64,
                    task.cores,
                    task.memory,
                    task.flops_per_byte,
                    task.output_size_ratio,
                )) as Box<dyn Task>);
            }
            dag.add_stage(tasks, stage.upload_result_to_dfs);
            for connection in stage.connections {
                dag.add_connection(
                    connection.from,
                    stage_id,
                    connection.shuffle.map(|shuffle_type| match shuffle_type {
                        ShuffleType::Simple => Box::new(UniformShuffle {}),
                    } as Box<dyn Shuffle>),
                );
            }
        }
        dag
    }
}

/// Struct representing [InputPlan].
#[derive(Serialize, Deserialize)]
pub struct YamlStageInputPlan {
    /// Stage id.
    pub stage: usize,
    /// Input plan for a stage.
    pub input: InputPlan,
}

/// Struct representing [DagPlan].
#[derive(Serialize, Deserialize)]
pub struct YamlDagPlan {
    /// Start time of a DAG.
    pub start_time: f64,
    /// Dag name which corresponds to a file name in a provided folder, see [SimulationPlan::from_yaml].
    pub dag: String,
    /// Information about input plans for stages.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input: Vec<YamlStageInputPlan>,
}

/// Struct representing [SimulationPlan].
#[derive(Serialize, Deserialize)]
pub struct YamlSimulationPlan {
    /// Information about DAGs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dags: Vec<YamlDagPlan>,
    /// Information about global inputs which can be used by all DAGs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub global_inputs: Vec<GlobalInputPlan>,
}

impl SimulationPlan {
    /// Read [SimulationPlan] from YAML file and dags from YAML files in a corresponding folder. Panic on error.
    pub fn from_yaml<P: AsRef<Path>>(file: P, dag_folder: PathBuf) -> Self {
        let yaml: YamlSimulationPlan = serde_yaml::from_str(
            &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display())),
        )
        .unwrap_or_else(|e| panic!("Can't parse YAML from file {}: {e:?}", file.as_ref().display()));

        let mut plan = SimulationPlan {
            dags: Vec::new(),
            global_inputs: yaml.global_inputs,
        };

        for dag in yaml.dags.into_iter() {
            plan.dags.push(DagPlan {
                start_time: dag.start_time,
                dag: Rc::new(RefCell::new(Dag::from_yaml(
                    dag_folder.join(&dag.dag).with_extension("yaml"),
                ))),
                input: dag.input.into_iter().map(|input| (input.stage, input.input)).collect(),
            })
        }

        plan
    }
}

impl SystemConfig {
    /// Read [SystemConfig] from YAML file. Panic on error.
    pub fn from_yaml<P: AsRef<Path>>(file: P) -> Self {
        serde_yaml::from_str(
            &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display())),
        )
        .unwrap_or_else(|e| panic!("Can't parse YAML from file {}: {e:?}", file.as_ref().display()))
    }
}
