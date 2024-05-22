//! Model of a host.

use std::{cell::RefCell, rc::Rc};

use dslab_compute::multicore::Compute;
use dslab_core::Id;

/// Model of a host.
#[derive(Clone)]
pub struct ComputeHost {
    /// Host id.
    pub host: Id,
    /// Name of the host.
    pub name: String,
    /// Speed of a single core on a host.
    pub speed: f64,
    /// Number of cores.
    pub cores: u32,
    /// Number of available cores.
    pub available_cores: u32,
    /// Amount of memory.
    pub memory: u64,
    /// Amount of available memory.
    pub available_memory: u64,
    /// Corresponding [Compute] resource.
    pub compute: Rc<RefCell<Compute>>,
}
