use std::collections::BTreeMap;

use dslab_core::Id;
use dslab_dfs::dfs::DistributedFileSystem;
use dslab_network::Network;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;

use crate::{
    compute_host::ComputeHost,
    dag::{Dag, Stage},
    data_item::DataItem,
    placement_strategy::{PlacementStrategy, TaskPlacement},
};

pub struct RandomPlacementStrategy {
    rng: Pcg64,
}

impl RandomPlacementStrategy {
    pub fn new() -> Self {
        Self {
            rng: Pcg64::seed_from_u64(123),
        }
    }
}

impl PlacementStrategy for RandomPlacementStrategy {
    fn place_stage(
        &mut self,
        stage: &Stage,
        _graph: &Dag,
        input_data: &[DataItem],
        _input_data_shuffled: &[Vec<DataItem>],
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        _network: &Network,
    ) -> Vec<TaskPlacement> {
        let mut all_inputs: Vec<DataItem> = Vec::new();
        for data_item in input_data.iter() {
            match data_item {
                DataItem::Chunk { .. } => {
                    all_inputs.push(*data_item);
                }
                DataItem::Local { mut size, host } => {
                    while size > 0 {
                        let size_here = if size >= dfs.chunk_size() * 2 {
                            dfs.chunk_size()
                        } else {
                            size
                        };
                        all_inputs.push(DataItem::Local {
                            size: size_here,
                            host: *host,
                        });
                        size -= size_here;
                    }
                }
                DataItem::Replicated { data_id, .. } => {
                    for _chunk_id in dfs.data_chunks(*data_id).unwrap().iter() {
                        all_inputs.push(*data_item);
                    }
                }
            }
        }

        for i in 1..all_inputs.len() {
            all_inputs.swap(i, self.rng.gen_range(0..i + 1));
        }

        let hosts = compute_host_info.keys().copied().collect::<Vec<_>>();

        (0..stage.tasks().len())
            .map(|task_id| TaskPlacement {
                host: hosts[self.rng.gen_range(0..hosts.len())],
                input: (task_id..all_inputs.len())
                    .step_by(stage.tasks().len())
                    .map(|i| all_inputs[i])
                    .collect(),
            })
            .collect()
    }
}

impl Default for RandomPlacementStrategy {
    fn default() -> Self {
        Self::new()
    }
}
