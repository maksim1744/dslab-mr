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
    pub total_cpu_time: f64,
    pub total_network_traffic: f64,
    pub network_traffic_between_racks: f64,
    pub network_traffic_between_hosts: f64,
}

impl RunStats {
    pub fn new() -> Self {
        RunStats {
            total_makespan: 0.0,
            total_dag_count: 0,
            completed_dag_count: 0,
            average_dag_makespan: 0.0,
            min_dag_makespan: f64::MAX,
            max_dag_makespan: 0.0,
            total_cpu_time: 0.0,
            total_network_traffic: 0.0,
            network_traffic_between_racks: 0.0,
            network_traffic_between_hosts: 0.0,
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

    pub fn register_task_execution(&mut self, time: f64) {
        self.total_cpu_time += time;
    }
}

impl Default for RunStats {
    fn default() -> Self {
        Self::new()
    }
}
