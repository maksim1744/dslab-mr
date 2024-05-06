use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
};

use serde::{Deserialize, Serialize};

use crate::cluster_simulation::{DagPlan, GlobalInputPlan, InputPlan, SimulationPlan};

use super::dag::{Dag, Shuffle, SimpleTask, Task, UniformShuffle};

#[derive(Serialize, Deserialize)]
pub struct YamlTask {
    pub time_per_byte: f64,
    pub output_size_ratio: f64,
}

#[derive(Serialize, Deserialize)]
pub struct YamlConnection {
    pub from: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shuffle: Option<ShuffleType>,
}

fn is_false(b: &bool) -> bool {
    !b
}

#[derive(Serialize, Deserialize)]
pub struct YamlStage {
    pub tasks: Vec<YamlTask>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub upload_result_to_dfs: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<YamlConnection>,
}

#[derive(Serialize, Deserialize)]
pub enum ShuffleType {
    Simple,
}

#[derive(Serialize, Deserialize)]
pub struct StageInitialData {
    pub stage: usize,
    pub size: u64,
}

#[derive(Serialize, Deserialize)]
pub struct YamlDag {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub initial_data: Vec<StageInitialData>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stages: Vec<YamlStage>,
}

impl Dag {
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
                    task.time_per_byte,
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

#[derive(Serialize, Deserialize)]
pub struct YamlStageInputPlan {
    pub stage: usize,
    pub input: InputPlan,
}

#[derive(Serialize, Deserialize)]
pub struct YamlDagPlan {
    pub start_time: f64,
    pub dag: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input: Vec<YamlStageInputPlan>,
}

#[derive(Serialize, Deserialize)]
pub struct YamlSimulationPlan {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dags: Vec<YamlDagPlan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub global_inputs: Vec<GlobalInputPlan>,
}

impl SimulationPlan {
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
