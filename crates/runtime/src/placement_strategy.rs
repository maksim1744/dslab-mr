//! Traits for implementing placement strategies.

use std::collections::BTreeMap;

use dslab_core::Id;
use dslab_network::Network;

use dslab_dfs::dfs::DistributedFileSystem;

use crate::compute_host::ComputeHost;

use super::{
    dag::{Dag, Stage},
    data_item::DataItem,
};

/// Information about one task placement for [PlacementStrategy].
#[derive(Debug)]
pub struct TaskPlacement {
    /// Host to place task to.
    pub host: Id,
    /// Inputs which task should process.
    pub input: Vec<DataItem>,
}

/// Simplified placement strategy.
///
/// Contains only one [place_stage](PlacementStrategy::place_stage) functions which corresponds to
/// [DynamicPlacementStrategy::on_stage_ready] and places all tasks right away.
pub trait PlacementStrategy {
    /// Callback when new [NewDag](crate::runner::NewDag) is received by [Runner](crate::runner::Runner).
    fn register_dag(&mut self, _dag_id: usize, _graph: &Dag) {}

    /// Place all tasks of a given stage.
    #[allow(clippy::too_many_arguments)]
    fn place_stage(
        &mut self,
        dag_id: usize,
        stage: &Stage,
        graph: &Dag,
        input_data: &[DataItem],
        input_data_shuffled: &[Vec<DataItem>],
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<TaskPlacement>;
}

/// Information about one task placement for [DynamicTaskPlacement].
#[derive(Debug)]
pub struct DynamicTaskPlacement {
    /// Id of a task.
    pub task_id: usize,
    /// Host to place task to.
    pub host: Id,
    /// Inputs which task should process.
    pub input: Vec<DataItem>,
}

/// Information about task placement from the same stage.
#[derive(Debug)]
pub struct StageActions {
    /// DAG id.
    pub dag_id: usize,
    /// Stage id.
    pub stage_id: usize,
    /// Vector of task placement.
    pub task_placements: Vec<DynamicTaskPlacement>,
    /// Remaining input to be processed in the future by yet unplaced tasks.
    pub remaining_input: Vec<DataItem>,
}

/// Dynamic placement strategy.
///
/// It receives callbacks for some simulation events and can place some unplaced tasks from ready or running stages in reponse.
pub trait DynamicPlacementStrategy {
    /// Callback when new [NewDag](crate::runner::NewDag) is received by [Runner](crate::runner::Runner).
    fn register_dag(&mut self, _dag_id: usize, _graph: &Dag) {}

    /// Callback when a stage of some DAG is ready.
    #[allow(clippy::too_many_arguments)]
    fn on_stage_ready(
        &mut self,
        dag_id: usize,
        stage_id: usize,
        graph: &Dag,
        input_data: &BTreeMap<usize, Vec<DataItem>>,
        input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<StageActions>;

    /// Callback when some task is completed.
    #[allow(clippy::too_many_arguments)]
    fn on_task_completed(
        &mut self,
        dag_id: usize,
        stage_id: usize,
        task: usize,
        graph: &Dag,
        input_data: &BTreeMap<usize, Vec<DataItem>>,
        input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<StageActions>;

    /// Callback when some stage is completed.
    #[allow(clippy::too_many_arguments)]
    fn on_stage_completed(
        &mut self,
        dag_id: usize,
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
    fn register_dag(&mut self, dag_id: usize, graph: &Dag) {
        T::register_dag(self, dag_id, graph);
    }

    fn on_stage_ready(
        &mut self,
        dag_id: usize,
        stage_id: usize,
        graph: &Dag,
        input_data: &BTreeMap<usize, Vec<DataItem>>,
        input_data_shuffled: &BTreeMap<usize, Vec<Vec<DataItem>>>,
        dfs: &DistributedFileSystem,
        compute_host_info: &BTreeMap<Id, ComputeHost>,
        network: &Network,
    ) -> Vec<StageActions> {
        vec![StageActions {
            dag_id,
            stage_id,
            task_placements: self
                .place_stage(
                    dag_id,
                    graph.stage(stage_id),
                    graph,
                    input_data.get(&stage_id).unwrap_or(&Vec::new()),
                    input_data_shuffled
                        .get(&stage_id)
                        .unwrap_or(&vec![vec![]; graph.stage(stage_id).tasks().len()]),
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
        _dag_id: usize,
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
        _dag_id: usize,
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
