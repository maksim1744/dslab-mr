use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::Write,
    path::PathBuf,
};

use clap::Parser;
use csv::ReaderBuilder;
use dslab_mr::parser::{ShuffleType, StageInitialData, YamlConnection, YamlDag, YamlStage, YamlTask};
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;
use serde::Deserialize;

#[derive(Parser, Debug)]
struct Args {
    /// Path to a folder with extracted csv traces.
    #[arg(short, long)]
    traces_path: PathBuf,

    /// Path to a folder with results.
    #[arg(short, long)]
    output_path: PathBuf,

    /// Consider only jobs which start not earlier than this.
    #[arg(short, long, default_value_t = 0)]
    start_time: i64,

    /// Consider only jobs which end not later than this.
    #[arg(short, long, default_value_t = i64::MAX)]
    end_time: i64,

    /// Skip jobs having less tasks than this.
    #[arg(long, default_value_t = 1)]
    min_tasks_in_job: usize,

    /// Allow multiple outgoing edges (unlike for original RDD).
    #[arg(long, default_value_t = false)]
    allow_multiple_outgoing_edges: bool,

    /// Bounds of uniform distribute for time_per_byte.
    #[arg(long, default_value_t = 0.001)]
    time_per_byte_from: f64,

    /// Bounds of uniform distribute for time_per_byte.
    #[arg(long, default_value_t = 0.002)]
    time_per_byte_to: f64,

    /// Bounds of uniform distribute for output_size_ratio.
    #[arg(long, default_value_t = 0.5)]
    output_size_ratio_from: f64,

    /// Bounds of uniform distribute for output_size_ratio.
    #[arg(long, default_value_t = 2.0)]
    output_size_ratio_to: f64,

    /// Probability of adding shuffle along an edge.
    #[arg(long, default_value_t = 0.1)]
    shuffle_probability: f64,

    /// Probability of uploading intermediate results to dfs.
    #[arg(long, default_value_t = 0.0)]
    upload_result_to_dfs_probability: f64,

    /// Bounds of uniform distribute for initial data size. Will be multiplied by total number of instances of all tasks in a job.
    #[arg(long, default_value_t = 1000.0)]
    initial_data_size_from: f64,

    /// Bounds of uniform distribute for initial data size. Will be multiplied by total number of instances of all tasks in a job.
    #[arg(long, default_value_t = 2000.0)]
    initial_data_size_to: f64,
}

// https://github.com/alibaba/clusterdata/blob/master/cluster-trace-v2018/schema.txt
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct BatchTaskRaw {
    task_name: String,
    instance_num: String,
    job_name: String,
    task_type: String,
    status: String,
    start_time: i64,
    end_time: i64,
    plan_cpu: String,
    plan_mem: String,
}

struct BatchTask {
    task_id: i64,
    dependencies: Vec<i64>,
    instances: i64,
    job_id: i64,
    start_time: i64,
    end_time: i64,
}

impl TryFrom<BatchTaskRaw> for BatchTask {
    type Error = &'static str;

    fn try_from(raw_task: BatchTaskRaw) -> Result<Self, Self::Error> {
        if raw_task.status != "Terminated" {
            return Err("Task state must be \"Terminated\"");
        }
        let mut name_parts = raw_task.task_name.split('_');
        let task_id = name_parts
            .next()
            .unwrap()
            .strip_prefix(|c: char| c.is_ascii_uppercase())
            .and_then(|s| s.parse().ok())
            .ok_or("Can't parse task name")?;
        let dependencies = name_parts
            .map(|s| s.parse())
            .collect::<Result<Vec<i64>, _>>()
            .map_err(|_| "Can't parse task name")?;
        Ok(Self {
            task_id,
            dependencies,
            instances: raw_task.instance_num.parse().map_err(|_| "Can't parse instance_num")?,
            job_id: raw_task
                .job_name
                .strip_prefix("j_")
                .ok_or("Can't parse job_id")?
                .parse()
                .map_err(|_| "Can't parse job_id")?,
            start_time: raw_task.start_time,
            end_time: raw_task.end_time,
        })
    }
}

fn read_batch_task(args: &Args) -> BTreeMap<i64, Vec<BatchTask>> {
    let mut successes = 0;
    let mut failures = 0;
    let mut job_tasks: BTreeMap<i64, Vec<BatchTask>> = BTreeMap::new();
    let mut failed_jobs: HashSet<String> = HashSet::new();
    for (i, result) in ReaderBuilder::new()
        .has_headers(false)
        .from_reader(File::open(args.traces_path.join("batch_task.csv")).expect("Can't open batch_task.csv"))
        .deserialize::<BatchTaskRaw>()
        .enumerate()
    {
        let raw_task = result.unwrap();
        let job_name = raw_task.job_name.clone();
        if let Ok(task) = BatchTask::try_from(raw_task) {
            job_tasks.entry(task.job_id).or_default().push(task);
            successes += 1;
        } else {
            failed_jobs.insert(job_name);
            failures += 1;
        }
        if i % 10000 == 0 {
            print!("\rReading batch_task.csv: {successes} successes, {failures} failures");
        }
    }
    println!("\rReading batch_task.csv: {successes} successes, {failures} failures");
    job_tasks.retain(|&job, _| !failed_jobs.contains(&format!("j_{job}")));
    job_tasks.retain(|_, tasks| tasks.len() >= args.min_tasks_in_job);
    job_tasks.retain(|_, tasks| {
        tasks
            .iter()
            .all(|task| args.start_time <= task.start_time && task.end_time <= args.end_time)
    });
    if !args.allow_multiple_outgoing_edges {
        job_tasks.retain(|_, tasks| {
            let mut outgoing_edges: HashMap<i64, usize> = HashMap::new();
            for task in tasks.iter() {
                for &dependency in task.dependencies.iter() {
                    *outgoing_edges.entry(dependency).or_default() += 1;
                }
            }
            outgoing_edges.values().all(|&cnt| cnt <= 1)
        });
    }
    job_tasks.retain(|_, tasks| tasks.iter().all(|task| task.instances > 0));
    job_tasks.retain(|_, tasks| {
        let task_ids = tasks.iter().map(|t| t.task_id).collect::<HashSet<_>>();
        if task_ids.len() != tasks.len() {
            return false;
        }
        tasks
            .iter()
            .flat_map(|t| t.dependencies.iter().copied())
            .all(|t| task_ids.contains(&t))
    });
    println!("batch_task.csv: found {} jobs", job_tasks.len());
    job_tasks
}

fn main() {
    let args = Args::parse();
    std::fs::create_dir_all(&args.output_path).expect("Can't create output directory");
    let dags_directory = args.output_path.join("dags");
    let _ = std::fs::remove_dir_all(&dags_directory);
    std::fs::create_dir_all(&dags_directory).expect("Can't create output directory for dags");

    let mut rng = Pcg64::seed_from_u64(123);

    let jobs = read_batch_task(&args);
    for (job_id, tasks) in jobs.into_iter() {
        let mut dag = YamlDag {
            initial_data: tasks
                .iter()
                .enumerate()
                .filter(|(_id, task)| task.dependencies.is_empty())
                .map(|(id, task)| {
                    (
                        id,
                        (rng.gen_range(args.initial_data_size_from..args.initial_data_size_to) * task.instances as f64)
                            as u64,
                    )
                })
                .map(|(stage, size)| StageInitialData { stage, size })
                .collect(),
            stages: Vec::new(),
        };
        let task_index = tasks
            .iter()
            .enumerate()
            .map(|(i, task)| (task.task_id, i))
            .collect::<HashMap<_, _>>();
        let has_outgoing_edges = tasks
            .iter()
            .flat_map(|t| t.dependencies.iter().copied())
            .collect::<HashSet<_>>();
        for task in tasks.iter() {
            dag.stages.push(YamlStage {
                tasks: (0..task.instances)
                    .map(|_| YamlTask {
                        time_per_byte: rng.gen_range(args.time_per_byte_from..args.time_per_byte_to),
                        output_size_ratio: rng.gen_range(args.output_size_ratio_from..args.output_size_ratio_to),
                    })
                    .collect(),
                upload_result_to_dfs: !has_outgoing_edges.contains(&task.task_id)
                    || rng.gen_bool(args.upload_result_to_dfs_probability),
                connections: task
                    .dependencies
                    .iter()
                    .map(|&t| YamlConnection {
                        from: task_index[&t],
                        shuffle: if rng.gen_bool(args.shuffle_probability) {
                            Some(ShuffleType::Simple)
                        } else {
                            None
                        },
                    })
                    .collect(),
            });
        }
        File::create(dags_directory.join(format!("j_{job_id}.yaml")))
            .unwrap_or_else(|_| panic!("Can't create file for dag {}", job_id))
            .write_all(serde_yaml::to_string(&dag).unwrap().as_bytes())
            .unwrap_or_else(|_| panic!("Can't write dag {}", job_id));
    }
}
