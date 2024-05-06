use std::{cell::RefCell, rc::Rc};

use dslab_compute::multicore::Compute;
use dslab_core::Id;

#[derive(Clone)]
pub struct ComputeHost {
    pub host: Id,
    pub speed: f64,
    pub cores: u32,
    pub available_cores: u32,
    pub compute: Rc<RefCell<Compute>>,
}
