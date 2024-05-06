use std::collections::BTreeMap;

use dslab_core::Id;
use dslab_network::Network;

use dslab_dfs::dfs::DistributedFileSystem;

use crate::compute_host::ComputeHost;

use super::{
    dag::{Dag, Stage},
    data_item::DataItem,
};

#[derive(Debug)]
pub struct TaskPlacement {
    pub host: Id,
    pub input: Vec<DataItem>,
}

pub trait PlacementStrategy {
    #[allow(clippy::too_many_arguments)]
    fn place_stage(
        &mut self,
        stage: &Stage,
        graph: &Dag,
        input_data: &[DataItem],
        input_data_shuffled: &[Vec<DataItem>],
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<TaskPlacement>;
}

#[derive(Debug)]
pub struct DynamicTaskPlacement {
    pub task_id: usize,
    pub host: Id,
    pub input: Vec<DataItem>,
}

#[derive(Debug)]
pub struct StageActions {
    pub stage_id: usize,
    pub task_placements: Vec<DynamicTaskPlacement>,
    pub remaining_input: Vec<DataItem>,
}

pub trait DynamicPlacementStrategy {
    #[allow(clippy::too_many_arguments)]
    fn on_stage_ready(
        &mut self,
        stage_id: usize,
        graph: &Dag,
        input_data: &BTreeMap<usize, Vec<DataItem>>,
        input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<StageActions>;

    #[allow(clippy::too_many_arguments)]
    fn on_task_completed(
        &mut self,
        stage_id: usize,
        task: usize,
        graph: &Dag,
        input_data: &BTreeMap<usize, Vec<DataItem>>,
        input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<StageActions>;

    #[allow(clippy::too_many_arguments)]
    fn on_stage_completed(
        &mut self,
        stage_id: usize,
        graph: &Dag,
        input_data: &BTreeMap<usize, Vec<DataItem>>,
        input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<StageActions>;
}

impl<T> DynamicPlacementStrategy for T
where
    T: PlacementStrategy,
{
    fn on_stage_ready(
        &mut self,
        stage_id: usize,
        graph: &Dag,
        input_data: &BTreeMap<usize, Vec<DataItem>>,
        input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<StageActions> {
        vec![StageActions {
            stage_id,
            task_placements: self
                .place_stage(
                    graph.stage(stage_id),
                    graph,
                    input_data.get(&stage_id).unwrap_or(&Vec::new()),
                    input_data_shuffled.get(&stage_id).unwrap_or(&Vec::new()),
                    dfs,
                    compute_host_info,
                    network,
                )
                .into_iter()
                .enumerate()
                .map(|(task_id, task_placement)| DynamicTaskPlacement {
                    task_id,
                    host: task_placement.host,
                    input: task_placement.input,
                })
                .collect(),
            remaining_input: Vec::new(),
        }]
    }

    fn on_task_completed(
        &mut self,
        _stage_id: usize,
        _task: usize,
        _graph: &Dag,
        _input_data: &BTreeMap<usize, Vec<DataItem>>,
        _input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        _dfs: &DistributedFileSystem,
        _compute_host_info: &BTreeMap<Id, ComputeHost>,
        _network: &Network,
    ) -> Vec<StageActions> {
        Vec::new()
    }

    fn on_stage_completed(
        &mut self,
        _stage_id: usize,
        _graph: &Dag,
        _input_data: &BTreeMap<usize, Vec<DataItem>>,
        _input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        _dfs: &DistributedFileSystem,
        _compute_host_info: &BTreeMap<Id, ComputeHost>,
        _network: &Network,
    ) -> Vec<StageActions> {
        Vec::new()
    }
}
