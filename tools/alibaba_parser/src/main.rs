use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    env,
    fs::File,
    io::Write,
    path::PathBuf,
};

use clap::Parser;
use csv::ReaderBuilder;
use dslab_mr::{
    cluster_simulation::InputPlan,
    parser::{
        ShuffleType, StageInitialData, YamlConnection, YamlDag, YamlDagPlan, YamlSimulationPlan, YamlStage,
        YamlStageInputPlan, YamlTask,
    },
};
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;
use serde::Deserialize;

/// Parses 2018 alibaba traces <https://github.com/alibaba/clusterdata/tree/master/cluster-trace-v2018> into simulation plan.
/// Only batch_task.csv is needed.
#[derive(Parser, Debug)]
struct Args {
    /// Path to a folder with extracted csv alibaba traces.
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

    /// Maximum number of jobs.
    #[arg(long, default_value_t = usize::MAX)]
    limit: usize,

    /// Skip jobs having less tasks than this.
    #[arg(long, default_value_t = 1)]
    min_tasks_in_job: usize,

    /// Allow multiple outgoing edges (unlike for original RDD).
    #[arg(long, default_value_t = false)]
    allow_multiple_outgoing_edges: bool,

    /// Upload inputs for all jobs to dfs in advance before starting execution.
    #[arg(long, default_value_t = false)]
    upload_job_inputs_in_advance: bool,

    /// Number of cores corresponding to 100% CPU usage.
    #[arg(long, default_value_t = 1.0)]
    cpu_multiplier: f64,

    /// Needed memory for a task using 100% memory.
    #[arg(long, default_value_t = 1.0)]
    mem_multiplier: f64,

    /// Bounds of uniform distribute for flops_per_byte.
    #[arg(long, default_value_t = 0.001)]
    flops_per_byte_from: f64,

    /// Bounds of uniform distribute for flops_per_byte.
    #[arg(long, default_value_t = 0.002)]
    flops_per_byte_to: f64,

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

    /// Multiply number of instances by a random number on an interval.
    #[arg(long, default_value_t = 1.0)]
    instances_multiplier_from: f64,

    /// Multiply number of instances by a random number on an interval.
    #[arg(long, default_value_t = 1.0)]
    instances_multiplier_to: f64,
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

#[derive(Clone)]
struct BatchTask {
    task_id: i64,
    dependencies: Vec<i64>,
    instances: i64,
    job_id: i64,
    start_time: i64,
    end_time: i64,
    plan_cpu: f64,
    plan_mem: f64,
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
            plan_cpu: raw_task.plan_cpu.parse().unwrap_or(0.0) / 100.0,
            plan_mem: raw_task.plan_mem.parse().unwrap_or(0.0) / 100.0,
        })
    }
}

fn read_batch_task(args: &Args, rng: &mut Pcg64) -> BTreeMap<i64, Vec<BatchTask>> {
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
        if let Ok(mut task) = BatchTask::try_from(raw_task) {
            task.instances = (task.instances as f64
                * rng.gen_range(args.instances_multiplier_from..=args.instances_multiplier_to))
            .round() as i64;
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
    println!(
        "batch_task.csv: found {} jobs, {} instances",
        job_tasks.len(),
        job_tasks
            .values()
            .flat_map(|t| t.iter())
            .map(|t| t.instances)
            .sum::<i64>()
    );
    if !job_tasks.is_empty() {
        let mut jobs_by_task_count: BTreeMap<usize, usize> = BTreeMap::new();
        for job in job_tasks.values() {
            *jobs_by_task_count.entry(job.len()).or_default() += 1;
        }
        println!("distribution by number of tasks");
        for (k, v) in jobs_by_task_count.into_iter() {
            println!("    {: <4}: {}", k, v);
        }
    }
    job_tasks
}

fn main() {
    let args = Args::parse();
    std::fs::create_dir_all(&args.output_path).expect("Can't create output directory");
    let dags_directory = args.output_path.join("dags");
    let _ = std::fs::remove_dir_all(&dags_directory);
    std::fs::create_dir_all(&dags_directory).expect("Can't create output directory for dags");

    let mut rng = Pcg64::seed_from_u64(123);

    let jobs = read_batch_task(&args, &mut rng);
    let mut plan = YamlSimulationPlan {
        dags: Vec::new(),
        global_inputs: Vec::new(),
    };
    for (job_id, mut tasks) in jobs.into_iter().take(args.limit) {
        let mut tasks_order = Vec::new();
        {
            let task_index = tasks
                .iter()
                .enumerate()
                .map(|(i, task)| (task.task_id, i))
                .collect::<HashMap<_, _>>();
            let mut q = VecDeque::new();
            let mut inputs = vec![0usize; tasks.len()];
            let mut outgoing_deps = vec![vec![]; tasks.len()];
            for (task_id, task) in tasks.iter().enumerate() {
                for dep in task.dependencies.iter() {
                    outgoing_deps[task_index[dep]].push(task_id);
                    inputs[task_id] += 1;
                }
            }
            for (task_id, &input) in inputs.iter().enumerate() {
                if input == 0 {
                    q.push_back(task_id);
                }
            }
            while let Some(task) = q.pop_front() {
                tasks_order.push(task);
                for &out in outgoing_deps[task].iter() {
                    inputs[out] -= 1;
                    if inputs[out] == 0 {
                        q.push_back(out);
                    }
                }
            }
            assert_eq!(tasks_order.len(), tasks.len());
            tasks = tasks_order.into_iter().map(|task_id| tasks[task_id].clone()).collect();
        }
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
                        cores: ((task.plan_cpu.min(1.0) * args.cpu_multiplier).round() as u32).max(1),
                        memory: (task.plan_mem * args.mem_multiplier).round() as u64,
                        flops_per_byte: rng.gen_range(args.flops_per_byte_from..args.flops_per_byte_to)
                            * (task.end_time - task.start_time).max(1) as f64,
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
        let dag_name = format!("j_{job_id}");
        plan.dags.push(YamlDagPlan {
            start_time: tasks
                .iter()
                .map(|task| task.start_time - args.start_time)
                .min()
                .unwrap() as f64,
            dag: dag_name.clone(),
            input: dag
                .initial_data
                .iter()
                .map(|input| YamlStageInputPlan {
                    stage: input.stage,
                    input: match args.upload_job_inputs_in_advance {
                        false => InputPlan::RegisterOnStart {
                            host: "random".to_string(),
                        },
                        true => InputPlan::RegisterInitially {
                            host: "random".to_string(),
                        },
                    },
                })
                .collect(),
        });
        File::create(dags_directory.join(format!("{dag_name}.yaml")))
            .unwrap_or_else(|_| panic!("Can't create file for dag {}", job_id))
            .write_all(serde_yaml::to_string(&dag).unwrap().as_bytes())
            .unwrap_or_else(|_| panic!("Can't write dag {}", job_id));
    }
    plan.dags.sort_by(|a, b| a.start_time.total_cmp(&b.start_time));
    File::create(args.output_path.join("plan.yaml"))
        .expect("Can't create file for plan.yaml")
        .write_all(serde_yaml::to_string(&plan).unwrap().as_bytes())
        .expect("Can't write plan to file");
    File::create(args.output_path.join("cmd.txt"))
        .expect("Can't create file for cmd.txt")
        .write_all(env::args().skip(1).collect::<Vec<_>>().join(" ").as_bytes())
        .expect("Can't write cmd to file");
}
