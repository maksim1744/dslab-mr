use std::path::Path;

use serde::Deserialize;

use super::dag::{Dag, Shuffle, SimpleTask, Task, UniformShuffle};

#[derive(Deserialize)]
struct YamlTask {
    time_per_byte: f64,
    output_size_ratio: f64,
}

#[derive(Deserialize)]
struct YamlConnection {
    from: usize,
    #[serde(default)]
    shuffle: Option<ShuffleType>,
}

#[derive(Deserialize)]
struct YamlStage {
    tasks: Vec<YamlTask>,
    #[serde(default)]
    upload_result_to_dfs: bool,
    #[serde(default)]
    connections: Vec<YamlConnection>,
}

#[derive(Deserialize)]
enum ShuffleType {
    Simple,
}

#[derive(Deserialize)]
struct YamlDag {
    initial_data: u64,
    stages: Vec<YamlStage>,
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
