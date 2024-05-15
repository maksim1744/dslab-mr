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

use super::common::{collect_all_input, shuffle};

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
        _dag_id: usize,
        stage: &Stage,
        _graph: &Dag,
        input_data: &[DataItem],
        _input_data_shuffled: &[Vec<DataItem>],
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        _network: &Network,
    ) -> Vec<TaskPlacement> {
        let mut all_inputs = collect_all_input(input_data, dfs)
            .into_iter()
            .map(|x| x.0)
            .collect::<Vec<_>>();

        shuffle(&mut self.rng, &mut all_inputs);

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
