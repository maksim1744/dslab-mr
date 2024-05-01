use std::path::Path;

use serde::{Deserialize, Serialize};

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
pub struct YamlDag {
    pub initial_data: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stages: Vec<YamlStage>,
}

impl Dag {
    pub fn from_yaml<P: AsRef<Path>>(file: P) -> Self {
        let yaml: YamlDag = serde_yaml::from_str(
            &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display())),
        )
        .unwrap_or_else(|e| panic!("Can't parse YAML from file {}: {e:?}", file.as_ref().display()));

        let mut dag = Dag::new().with_initial_data(yaml.initial_data);
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
