#[derive(Copy, Clone, Debug)]
pub struct MapOutput {
    pub reducer_id: u64,
    pub size: u64,
}

pub trait MapReduceParams {
    fn map_tasks_count(&self) -> u64;
    fn map_task_time(&self, task_id: u64, input_size: u64) -> f64;
    fn map_task_output(&self, task_id: u64, input_size: u64) -> Vec<MapOutput>;

    fn reduce_tasks_count(&self) -> u64;
    fn reduce_task_time(&self, task_id: u64, input_size: u64) -> f64;
    fn reduce_task_output(&self, task_id: u64, input_size: u64) -> u64;
}
