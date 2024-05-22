//! Trace of a simulation.

use serde::{Deserialize, Serialize};

use crate::system::HostConfig;

/// One trace event in a simulation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TraceEvent {
    /// Start of a task.
    TaskStarted {
        /// Time of an event.
        time: f64,
        /// DAG id.
        dag_id: usize,
        /// Stage id.
        stage_id: usize,
        /// Task id.
        task_id: usize,
        /// Number of cores used.
        cores: u32,
        /// Amount of memory used.
        memory: u64,
        /// Host where the task was run.
        host: String,
    },
    /// Completion of a task.
    TaskCompleted {
        /// Time of an event.
        time: f64,
        /// DAG id.
        dag_id: usize,
        /// Stage id.
        stage_id: usize,
        /// Task id.
        task_id: usize,
    },
}

impl TraceEvent {
    /// Time of an event.
    pub fn time(&self) -> f64 {
        match self {
            TraceEvent::TaskStarted { time, .. } | TraceEvent::TaskCompleted { time, .. } => *time,
        }
    }
}

/// Trace of a simulation.
#[derive(Clone, Serialize, Deserialize)]
pub struct Trace {
    /// Information about hosts.
    pub hosts: Vec<HostConfig>,
    /// Simulation events.
    pub events: Vec<TraceEvent>,
}

impl Trace {
    /// Creates new trace.
    pub fn new(hosts: Vec<HostConfig>) -> Self {
        Self {
            hosts,
            events: Vec::new(),
        }
    }

    /// Adds event to a trace.
    pub fn log(&mut self, event: TraceEvent) {
        self.events.push(event);
    }
}
