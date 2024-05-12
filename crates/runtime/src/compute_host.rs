use std::{cell::RefCell, rc::Rc};

use dslab_compute::multicore::Compute;
use dslab_core::Id;

#[derive(Clone)]
pub struct ComputeHost {
    pub host: Id,
    pub name: String,
    pub speed: f64,
    pub cores: u32,
    pub available_cores: u32,
    pub memory: u64,
    pub available_memory: u64,
    pub compute: Rc<RefCell<Compute>>,
}
