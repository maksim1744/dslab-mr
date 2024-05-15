use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use dslab_core::Id;
use dslab_dfs::{dfs::DistributedFileSystem, network::get_all_racks};
use dslab_network::Network;
use rand::SeedableRng;
use rand_pcg::Pcg64;

use crate::{
    compute_host::ComputeHost,
    dag::Dag,
    data_item::DataItem,
    placement_strategy::{DynamicPlacementStrategy, DynamicTaskPlacement, StageActions},
};

use super::common::{collect_all_input, shuffle};

struct PreparedTask {
    dag_id: usize,
    stage_id: usize,
    task_id: usize,
    input: Vec<DataItem>,
    resource_vec: [f64; 2],
    good_hosts: BTreeSet<Id>,
    good_racks: BTreeSet<u64>,
}

struct ComputHostInfo {
    cores: u32,
    memory: u64,
}

pub struct PackingScheduler {
    other_rack_input_penalty: f64,
    other_host_input_penalty: f64,
    prepared_tasks: VecDeque<PreparedTask>,
    host_racks: BTreeMap<Id, u64>,
    rng: Pcg64,
}

impl PackingScheduler {
    pub fn new(other_rack_input_penalty: f64, other_host_input_penalty: f64) -> Self {
        Self {
            other_rack_input_penalty,
            other_host_input_penalty,
            prepared_tasks: VecDeque::new(),
            host_racks: BTreeMap::new(),
            rng: Pcg64::seed_from_u64(123),
        }
    }

    fn place_tasks(&mut self, compute_host_info: &BTreeMap<Id, ComputeHost>) -> Vec<StageActions> {
        let mut compute_host_info = compute_host_info
            .iter()
            .map(|(id, host)| {
                (
                    *id,
                    ComputHostInfo {
                        cores: host.available_cores,
                        memory: host.available_memory,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut result: BTreeMap<(usize, usize), Vec<DynamicTaskPlacement>> = BTreeMap::new();
        while !self.prepared_tasks.is_empty() {
            let task = self.prepared_tasks.front().unwrap();
            let best_host = compute_host_info
                .iter()
                .map(|(id, compute_host)| (*id, [compute_host.cores as f64, compute_host.memory as f64]))
                .filter(|(_id, resources)| task.resource_vec.iter().zip(resources).all(|(need, have)| need <= have))
                .map(|(id, resources)| {
                    (
                        id,
                        task.resource_vec.iter().zip(resources).map(|(x, y)| x * y).sum::<f64>(),
                    )
                })
                .map(|(id, score)| {
                    (
                        id,
                        score
                            * if task.good_hosts.contains(&id) {
                                1.0
                            } else {
                                self.other_host_input_penalty
                            },
                    )
                })
                .map(|(id, score)| {
                    (
                        id,
                        score
                            * if task.good_racks.contains(&self.host_racks[&id]) {
                                1.0
                            } else {
                                self.other_rack_input_penalty
                            },
                    )
                })
                .max_by(|a, b| a.1.total_cmp(&b.1));
            if best_host.is_none() {
                break;
            }
            let task = self.prepared_tasks.pop_front().unwrap();
            let best_host = best_host.unwrap().0;
            compute_host_info.get_mut(&best_host).unwrap().cores -= task.resource_vec[0].round() as u32;
            compute_host_info.get_mut(&best_host).unwrap().memory -= task.resource_vec[1].round() as u64;
            result
                .entry((task.dag_id, task.stage_id))
                .or_default()
                .push(DynamicTaskPlacement {
                    task_id: task.task_id,
                    host: best_host,
                    input: task.input,
                })
        }
        result
            .into_iter()
            .map(|((dag_id, stage_id), task_placements)| StageActions {
                dag_id,
                stage_id,
                task_placements,
                remaining_input: Vec::new(),
            })
            .collect()
    }
}

impl DynamicPlacementStrategy for PackingScheduler {
    fn on_stage_ready(
        &mut self,
        dag_id: usize,
        stage_id: usize,
        graph: &Dag,
        input_data: &BTreeMap<usize, Vec<DataItem>>,
        _input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<StageActions> {
        if self.host_racks.is_empty() {
            let node_racks = get_all_racks(network);
            self.host_racks = compute_host_info
                .keys()
                .copied()
                .map(|host| (host, node_racks[&network.get_location(host)]))
                .collect::<BTreeMap<_, _>>();
        }
        let all_racks = self.host_racks.values().copied().collect::<BTreeSet<_>>();
        let mut all_inputs = collect_all_input(input_data.get(&stage_id).unwrap_or(&Vec::new()), dfs);
        for (_data_item, hosts) in all_inputs.iter_mut() {
            shuffle(&mut self.rng, hosts);
        }
        all_inputs.sort_by_key(|(_data_item, hosts)| hosts[0]);
        let mut remaining_inputs = all_inputs.len();
        let tasks_count = graph.stage(stage_id).tasks().len();
        let mut next_input = 0;
        for task_id in 0..tasks_count {
            let current_inputs = remaining_inputs / (tasks_count - task_id);
            remaining_inputs -= current_inputs;
            let inputs = all_inputs[next_input..next_input + current_inputs].to_vec();
            next_input += current_inputs;
            let mut inputs_on_host: HashMap<Id, HashSet<usize>> = HashMap::new();
            let mut inputs_on_rack: HashMap<u64, HashSet<usize>> = HashMap::new();
            for (i, (_data_item, hosts)) in inputs.iter().enumerate() {
                for &host in hosts.iter() {
                    inputs_on_host.entry(host).or_default().insert(i);
                    inputs_on_rack.entry(self.host_racks[&host]).or_default().insert(i);
                }
            }

            self.prepared_tasks.push_back(PreparedTask {
                dag_id,
                stage_id,
                task_id,
                input: inputs.into_iter().map(|x| x.0).collect(),
                resource_vec: [
                    graph.stage(stage_id).task(task_id).cores() as f64,
                    graph.stage(stage_id).task(task_id).memory() as f64,
                ],
                good_hosts: self
                    .host_racks
                    .keys()
                    .copied()
                    .filter(|host| inputs_on_host.get(host).map(|inputs| inputs.len()).unwrap_or(0) == current_inputs)
                    .collect(),
                good_racks: all_racks
                    .iter()
                    .copied()
                    .filter(|rack| inputs_on_rack.get(rack).map(|inputs| inputs.len()).unwrap_or(0) == current_inputs)
                    .collect(),
            })
        }
        self.place_tasks(compute_host_info)
    }

    fn on_task_completed(
        &mut self,
        _dag_id: usize,
        _stage_id: usize,
        _task: usize,
        _graph: &Dag,
        _input_data: &BTreeMap<usize, Vec<DataItem>>,
        _input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        _dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        _network: &Network,
    ) -> Vec<StageActions> {
        self.place_tasks(compute_host_info)
    }

    fn on_stage_completed(
        &mut self,
        _dag_id: usize,
        _stage_id: usize,
        _graph: &Dag,
        _input_data: &BTreeMap<usize, Vec<DataItem>>,
        _input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        _dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        _network: &Network,
    ) -> Vec<StageActions> {
        self.place_tasks(compute_host_info)
    }
}
