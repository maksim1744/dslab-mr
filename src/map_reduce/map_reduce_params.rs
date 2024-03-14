use dslab_core::Id;

use crate::distributed_file_system::host_info::DataId;

#[derive(Copy, Clone, Debug)]
pub struct MapOutput {
    pub reducer_id: u64,
    pub size: u64,
}

#[derive(Debug)]
pub enum InitialDataLocation {
    #[allow(unused)]
    UploadedToDFS {
        data_id: DataId,
    },
    ExistsOnHost {
        host: Id,
        size: u64,
    },
}

pub trait MapReduceParams {
    fn initial_data_location(&self) -> InitialDataLocation;

    fn map_tasks_count(&self) -> u64;
    fn map_task_time(&self, task_id: u64, input_size: u64) -> f64;
    fn map_task_output(&self, task_id: u64) -> Vec<MapOutput>;

    fn reduce_tasks_count(&self) -> u64;
    fn reduce_task_time(&self, task_id: u64, input_size: u64) -> f64;
    fn reduce_task_output(&self, task_id: u64) -> u64;
}
