use std::collections::{BTreeMap, BTreeSet};

use dslab_core::Id;
use dslab_dfs::{dfs::DistributedFileSystem, network::get_all_racks};
use dslab_network::Network;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;

use crate::{
    compute_host::ComputeHost,
    dag::{Dag, Stage},
    data_item::DataItem,
    placement_strategy::{PlacementStrategy, TaskPlacement},
};

pub struct LocalityAwareStrategy {
    rng: Pcg64,
}

impl LocalityAwareStrategy {
    pub fn new() -> Self {
        Self {
            rng: Pcg64::seed_from_u64(123),
        }
    }

    fn pick_random<'a, T>(&mut self, data: &'a [T]) -> &'a T {
        &data[self.rng.gen_range(0..data.len())]
    }
}

impl PlacementStrategy for LocalityAwareStrategy {
    fn place_stage(
        &mut self,
        stage: &Stage,
        _graph: &Dag,
        input_data: &[DataItem],
        input_data_shuffled: &[Vec<DataItem>],
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<TaskPlacement> {
        let mut all_inputs: Vec<(DataItem, Vec<Id>)> = Vec::new();
        let node_racks = get_all_racks(network);
        let host_racks = compute_host_info
            .keys()
            .copied()
            .map(|host| (host, node_racks[&network.get_location(host)]))
            .collect::<BTreeMap<_, _>>();
        for data_item in input_data.iter() {
            match data_item {
                DataItem::Chunk { chunk_id, .. } => {
                    all_inputs.push((
                        *data_item,
                        dfs.chunk_location(*chunk_id)
                            .map(|locations| locations.iter().copied().collect::<Vec<Id>>())
                            .unwrap_or_default(),
                    ));
                }
                DataItem::Local { mut size, host } => {
                    while size > 0 {
                        let size_here = if size >= dfs.chunk_size() * 2 {
                            dfs.chunk_size()
                        } else {
                            size
                        };
                        all_inputs.push((
                            DataItem::Local {
                                size: size_here,
                                host: *host,
                            },
                            vec![*host],
                        ));
                        size -= size_here;
                    }
                }
                DataItem::Replicated { data_id, .. } => {
                    for &chunk_id in dfs.data_chunks(*data_id).unwrap().iter() {
                        all_inputs.push((
                            *data_item,
                            dfs.chunk_location(chunk_id)
                                .map(|locations| locations.iter().copied().collect::<Vec<Id>>())
                                .unwrap_or_default(),
                        ));
                    }
                }
            }
        }

        for i in 1..all_inputs.len() {
            all_inputs.swap(i, self.rng.gen_range(0..i + 1));
        }

        let mut unassigned = (0..all_inputs.len()).collect::<BTreeSet<_>>();
        let mut result: Vec<TaskPlacement> = (0..stage.tasks().len())
            .map(|_task_id| TaskPlacement {
                host: compute_host_info.keys().next().copied().unwrap(),
                input: vec![],
            })
            .collect();

        for task_id in 0..stage.tasks().len() {
            if unassigned.is_empty() {
                break;
            }
            let mut assign_here =
                (unassigned.len() + stage.tasks().len() - task_id - 1) / (stage.tasks().len() - task_id);
            let host = if input_data_shuffled[task_id].is_empty() {
                *self.pick_random(&all_inputs[*unassigned.iter().next().unwrap()].1)
            } else {
                match self.pick_random(&input_data_shuffled[task_id]) {
                    DataItem::Chunk { chunk_id, .. } => {
                        **self.pick_random(&dfs.chunk_location(*chunk_id).unwrap().iter().collect::<Vec<_>>())
                    }
                    DataItem::Local { host, .. } => *host,
                    DataItem::Replicated { data_id, .. } => *self.pick_random(
                        &dfs.data_chunks(*data_id)
                            .unwrap()
                            .iter()
                            .flat_map(|chunk_id| dfs.chunk_location(*chunk_id).unwrap().iter().copied())
                            .collect::<Vec<_>>(),
                    ),
                }
            };
            result[task_id].host = host;

            let mut inputs = BTreeSet::new();

            // same host
            for input_id in unassigned
                .iter()
                .copied()
                .filter(|&input_id| all_inputs[input_id].1.contains(&host))
            {
                if assign_here == 0 {
                    break;
                }
                assign_here -= 1;
                inputs.insert(input_id);
            }

            // same rack
            for input_id in unassigned.iter().copied().filter(|&input_id| {
                all_inputs[input_id]
                    .1
                    .iter()
                    .any(|input_host| host_racks[input_host] == host_racks[&host])
            }) {
                if assign_here == 0 {
                    break;
                }
                if inputs.contains(&input_id) {
                    continue;
                }
                assign_here -= 1;
                inputs.insert(input_id);
            }

            // everything
            for input_id in unassigned.iter().copied() {
                if assign_here == 0 {
                    break;
                }
                if inputs.contains(&input_id) {
                    continue;
                }
                assign_here -= 1;
                inputs.insert(input_id);
            }

            for input_id in inputs.iter() {
                unassigned.remove(input_id);
            }

            result[task_id].input = inputs.into_iter().map(|input_id| all_inputs[input_id].0).collect();
        }

        result
    }
}

impl Default for LocalityAwareStrategy {
    fn default() -> Self {
        Self::new()
    }
}
