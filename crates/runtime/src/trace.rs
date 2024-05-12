use serde::{Deserialize, Serialize};

use crate::system::HostConfig;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TraceEvent {
    TaskStarted {
        time: f64,
        dag_id: usize,
        stage_id: usize,
        task_id: usize,
        cores: u32,
        memory: u64,
        host: String,
    },
    TaskCompleted {
        time: f64,
        dag_id: usize,
        stage_id: usize,
        task_id: usize,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Trace {
    pub hosts: Vec<HostConfig>,
    pub events: Vec<TraceEvent>,
}

impl Trace {
    pub fn new(hosts: Vec<HostConfig>) -> Self {
        Self {
            hosts,
            events: Vec::new(),
        }
    }

    pub fn log(&mut self, event: TraceEvent) {
        self.events.push(event);
    }
}
