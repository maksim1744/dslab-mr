use dslab_core::Id;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunStats {
    pub total_makespan: f64,
    pub total_dag_count: usize,
    pub completed_dag_count: usize,
    pub average_dag_makespan: f64,
    pub min_dag_makespan: f64,
    pub max_dag_makespan: f64,
    pub total_cpu_flops: f64,
    pub cpu_utilization: f64,
    pub memory_utilization: f64,
    pub total_network_traffic: f64,
    pub network_traffic_between_racks: f64,
    pub network_traffic_between_hosts: f64,
    pub total_chunks_in_dfs: u64,
    pub total_space_used: f64,

    #[serde(skip)]
    total_cores: u32,
    #[serde(skip)]
    total_memory: u64,
}

impl RunStats {
    pub fn new(total_cores: u32, total_memory: u64) -> Self {
        RunStats {
            total_makespan: 0.0,
            total_dag_count: 0,
            completed_dag_count: 0,
            average_dag_makespan: 0.0,
            min_dag_makespan: f64::MAX,
            max_dag_makespan: 0.0,
            total_cpu_flops: 0.0,
            cpu_utilization: 0.0,
            memory_utilization: 0.0,
            total_network_traffic: 0.0,
            network_traffic_between_racks: 0.0,
            network_traffic_between_hosts: 0.0,
            total_cores,
            total_memory,
            total_chunks_in_dfs: 0,
            total_space_used: 0.0,
        }
    }

    pub fn register_dag(&mut self, makespan: f64) {
        self.average_dag_makespan = (self.average_dag_makespan * self.completed_dag_count as f64 + makespan)
            / (self.completed_dag_count + 1) as f64;
        self.completed_dag_count += 1;
        self.max_dag_makespan = self.max_dag_makespan.max(makespan);
        self.min_dag_makespan = self.min_dag_makespan.min(makespan);
    }

    pub fn register_transfer(&mut self, size: f64, src: (Id, usize), dst: (Id, usize)) {
        let (src, src_rack) = src;
        let (dst, dst_rack) = dst;
        self.total_network_traffic += size;
        if src != dst {
            self.network_traffic_between_hosts += size;
        }
        if src_rack != dst_rack {
            self.network_traffic_between_racks += size;
        }
    }

    pub fn register_task_execution(&mut self, flops: f64) {
        self.total_cpu_flops += flops;
    }

    pub fn register_cpu_utilization(&mut self, cpu_utilization: f64) {
        self.cpu_utilization += cpu_utilization / self.total_cores as f64;
    }

    pub fn register_memory_utilization(&mut self, memory_utilization: f64) {
        self.memory_utilization += memory_utilization / self.total_memory as f64;
    }

    pub fn finalize(&mut self, makespan: f64) {
        self.total_makespan = makespan;
        self.cpu_utilization /= makespan;
        self.memory_utilization /= makespan;
    }
}
