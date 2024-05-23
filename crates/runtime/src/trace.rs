//! Trace of a simulation.

use serde::{Deserialize, Serialize};

use crate::system::HostConfig;

/// One trace event in a simulation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TraceEvent {
    /// Task stage is ready.
    TaskReady {
        /// Time of the event.
        time: f64,
        /// DAG id.
        dag_id: usize,
        /// Stage id.
        stage_id: usize,
        /// Task id.
        task_id: usize,
    },
    /// Task is waiting in a queue of a specific host for enough resources to start.
    TaskInQueue {
        /// Time of the event.
        time: f64,
        /// DAG id.
        dag_id: usize,
        /// Stage id.
        stage_id: usize,
        /// Task id.
        task_id: usize,
        /// Host.
        host: String,
    },
    /// Task is assigned to a host and waits for input data to be transfered.
    TaskAssigned {
        /// Time of the event.
        time: f64,
        /// DAG id.
        dag_id: usize,
        /// Stage id.
        stage_id: usize,
        /// Task id.
        task_id: usize,
        /// Host.
        host: String,
    },
    /// Task started processing input.
    TaskStarted {
        /// Time of the event.
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
    /// Task finished processing input.
    TaskCompleted {
        /// Time of the event.
        time: f64,
        /// DAG id.
        dag_id: usize,
        /// Stage id.
        stage_id: usize,
        /// Task id.
        task_id: usize,
    },
    /// Dag received by [Runner](crate::runner::Runner).
    DagStarted {
        /// Time of the event.
        time: f64,
        /// DAG id.
        dag_id: usize,
    },
    /// Dag completed.
    DagCompleted {
        /// Time of the event.
        time: f64,
        /// DAG id.
        dag_id: usize,
    },
}

impl TraceEvent {
    /// Time of an event.
    pub fn time(&self) -> f64 {
        match self {
            TraceEvent::TaskReady { time, .. }
            | TraceEvent::TaskInQueue { time, .. }
            | TraceEvent::TaskAssigned { time, .. }
            | TraceEvent::TaskStarted { time, .. }
            | TraceEvent::TaskCompleted { time, .. }
            | TraceEvent::DagStarted { time, .. }
            | TraceEvent::DagCompleted { time, .. } => *time,
        }
    }

    /// (dag id, stage id, task id) if the event corresponds to a task.
    pub fn task_path(&self) -> Option<(usize, usize, usize)> {
        match self {
            TraceEvent::TaskReady {
                dag_id,
                stage_id,
                task_id,
                ..
            }
            | TraceEvent::TaskInQueue {
                dag_id,
                stage_id,
                task_id,
                ..
            }
            | TraceEvent::TaskAssigned {
                dag_id,
                stage_id,
                task_id,
                ..
            }
            | TraceEvent::TaskStarted {
                dag_id,
                stage_id,
                task_id,
                ..
            }
            | TraceEvent::TaskCompleted {
                dag_id,
                stage_id,
                task_id,
                ..
            } => Some((*dag_id, *stage_id, *task_id)),
            _ => None,
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
