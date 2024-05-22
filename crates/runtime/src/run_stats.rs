//! Some stats from a completed simulation.

use dslab_core::Id;
use serde::{Deserialize, Serialize};

/// Some stats from a completed simulation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunStats {
    /// Total time between first [NewDag](crate::runner::NewDag) event and last simulation event.
    pub total_makespan: f64,
    /// Total number of received DAGs.
    pub total_dag_count: usize,
    /// Total number of completed DAGs.
    pub completed_dag_count: usize,
    /// Average makespan of a DAG among completed DAGs.
    pub average_dag_makespan: f64,
    /// Minimum makespan of a DAG among completed DAGs.
    pub min_dag_makespan: f64,
    /// Maximum makespan of a DAG among completed DAGs.
    pub max_dag_makespan: f64,
    /// Total CPU flops used by all completed tasks.
    pub total_cpu_flops: f64,
    /// Average CPU utilization during simulation.
    pub cpu_utilization: f64,
    /// Average memory utilization during simulation.
    pub memory_utilization: f64,
    /// Total network traffic. Includes transfers with `src == dst`.
    pub total_network_traffic: f64,
    /// Total network traffic between different racks.
    pub network_traffic_between_racks: f64,
    /// Total network traffic between different hosts.
    pub network_traffic_between_hosts: f64,
    /// Total number chunks in DFS in the end of the simulation.
    pub total_chunks_in_dfs: u64,
    /// Fraction of the used space in the end of the simulation.
    pub total_space_used: f64,

    #[serde(skip)]
    total_cores: u32,
    #[serde(skip)]
    total_memory: u64,
}

impl RunStats {
    /// Initialize new stats for a system with given number of cores and amount of memory.
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

    /// Register completed DAG with given makespan.
    pub fn register_dag(&mut self, makespan: f64) {
        self.average_dag_makespan = (self.average_dag_makespan * self.completed_dag_count as f64 + makespan)
            / (self.completed_dag_count + 1) as f64;
        self.completed_dag_count += 1;
        self.max_dag_makespan = self.max_dag_makespan.max(makespan);
        self.min_dag_makespan = self.min_dag_makespan.min(makespan);
    }

    /// Register completed transfer with given parameters.
    /// * `size` --- size of the data which was transfered.
    /// * `src`, `dst` --- pairs (simulation component, rack) for transfer source and destination.
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

    /// Register task with a given computation size.
    pub fn register_task_execution(&mut self, flops: f64) {
        self.total_cpu_flops += flops;
    }

    /// Register CPU utilization measure in `(number of cores used) * (time of the usage)`.
    pub fn register_cpu_utilization(&mut self, cpu_utilization: f64) {
        self.cpu_utilization += cpu_utilization / self.total_cores as f64;
    }

    /// Register memory utilization measure in `(amount of memory used) * (time of the usage)`.
    pub fn register_memory_utilization(&mut self, memory_utilization: f64) {
        self.memory_utilization += memory_utilization / self.total_memory as f64;
    }

    /// Finalize result given [total_makespan](RunStats::total_makespan);
    pub fn finalize(&mut self, makespan: f64) {
        self.total_makespan = makespan;
        self.cpu_utilization /= makespan;
        self.memory_utilization /= makespan;
    }
}
