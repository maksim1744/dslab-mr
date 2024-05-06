use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    rc::Rc,
};

use dslab_compute::multicore::Compute;
use dslab_core::{cast, log_info, Event, EventHandler, Id, Simulation, SimulationContext};
use dslab_dfs::{
    dfs::{DistributedFileSystem, RegisterData, RegisteredData},
    host_info::HostInfo,
    network::{make_fat_tree_topology, make_tree_topology},
    replication_strategy::ReplicationStrategy,
};
use serde::{Deserialize, Serialize};

use crate::{
    compute_host::ComputeHost,
    dag::Dag,
    data_item::DataItem,
    placement_strategy::DynamicPlacementStrategy,
    run_stats::RunStats,
    runner::{NewDag, Runner},
    system::{NetworkConfig, SystemConfig},
};

#[derive(Serialize, Deserialize)]
pub struct GlobalInputPlan {
    pub host: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum InputPlan {
    RegisterOnStart { host: String },
    RegisterInitially { host: String },
    Local { host: String },
    GlobalInput { id: usize },
}

pub struct DagPlan {
    pub start_time: f64,
    pub dag: Rc<RefCell<Dag>>,
    pub input: HashMap<usize, InputPlan>,
}

pub struct SimulationPlan {
    pub dags: Vec<DagPlan>,
    pub global_inputs: Vec<GlobalInputPlan>,
}

#[derive(Clone, Serialize)]
struct Start {}

struct DataOnHost {}

impl EventHandler for DataOnHost {
    fn on(&mut self, _event: dslab_core::Event) {
        panic!()
    }
}

struct DagManager {
    plan: SimulationPlan,
    dfs: Rc<RefCell<DistributedFileSystem>>,
    actor_by_host_name: HashMap<String, Id>,
    all_hosts: Vec<Id>,
    runner_id: Id,
    left_registrations: usize,
    start_time: f64,
    dag_inputs: Vec<HashMap<usize, Vec<DataItem>>>,
    waiting_inputs: Vec<usize>,
    registering_on_start: HashMap<u64, usize>,
    ctx: SimulationContext,
}

impl DagManager {
    fn new(
        plan: SimulationPlan,
        dfs: Rc<RefCell<DistributedFileSystem>>,
        actor_by_host_name: HashMap<String, Id>,
        runner_id: Id,
        ctx: SimulationContext,
    ) -> Self {
        let dags_count = plan.dags.len();
        let all_hosts = actor_by_host_name.values().copied().collect();
        Self {
            plan,
            dfs,
            actor_by_host_name,
            all_hosts,
            runner_id,
            left_registrations: 0,
            start_time: 0.0,
            dag_inputs: vec![HashMap::new(); dags_count],
            waiting_inputs: vec![0; dags_count],
            registering_on_start: HashMap::new(),
            ctx,
        }
    }

    fn on_start(&mut self) {
        for (id, global_input) in self.plan.global_inputs.iter().enumerate() {
            self.ctx.emit_now(
                RegisterData {
                    size: global_input.size,
                    host: self.get_host_by_name(&global_input.host),
                    data_id: id as u64,
                    need_to_replicate: true,
                },
                self.dfs.borrow().id(),
            );
            self.left_registrations += 1;
        }
        for (dag_id, dag) in self.plan.dags.iter().enumerate() {
            for (stage_id, input) in dag.input.iter() {
                match input {
                    InputPlan::RegisterInitially { host } => {
                        let size = dag.dag.borrow().initial_data()[stage_id];
                        let data_id = (dag_id + self.plan.global_inputs.len()) as u64;
                        self.ctx.emit_now(
                            RegisterData {
                                size,
                                host: self.get_host_by_name(host),
                                data_id,
                                need_to_replicate: true,
                            },
                            self.dfs.borrow().id(),
                        );
                        self.dag_inputs[dag_id].insert(*stage_id, vec![DataItem::Replicated { size, data_id }]);
                        self.left_registrations += 1;
                    }
                    InputPlan::GlobalInput { id } => {
                        self.dag_inputs[dag_id].insert(
                            *stage_id,
                            vec![DataItem::Replicated {
                                size: self.plan.global_inputs[*id].size,
                                data_id: *id as u64,
                            }],
                        );
                    }
                    InputPlan::Local { host } => {
                        let host = self.get_host_by_name(host);
                        self.dag_inputs[dag_id].insert(
                            *stage_id,
                            vec![DataItem::Local {
                                size: dag.dag.borrow().initial_data()[stage_id],
                                host,
                            }],
                        );
                    }
                    InputPlan::RegisterOnStart { .. } => {}
                }
            }
        }
    }

    fn start_execution(&mut self) {
        log_info!(self.ctx, "all data initialized, starting dags execution");
        self.start_time = self.ctx.time();
        for (dag_id, dag) in self.plan.dags.iter().enumerate() {
            for (stage_id, input) in dag.input.iter() {
                if let InputPlan::RegisterOnStart { host } = input {
                    let size = dag.dag.borrow().initial_data()[stage_id];
                    let data_id = self.dfs.borrow_mut().next_data_id();
                    self.ctx.emit(
                        RegisterData {
                            size,
                            host: self.get_host_by_name(host),
                            data_id,
                            need_to_replicate: true,
                        },
                        self.dfs.borrow().id(),
                        dag.start_time,
                    );
                    self.dag_inputs[dag_id].insert(*stage_id, vec![DataItem::Replicated { size, data_id }]);
                    self.waiting_inputs[dag_id] += 1;
                    self.registering_on_start.insert(data_id, dag_id);
                }
            }
            if self.waiting_inputs[dag_id] == 0 {
                self.ctx.emit(
                    NewDag {
                        dag: dag.dag.clone(),
                        initial_data: self.dag_inputs[dag_id].clone(),
                    },
                    self.runner_id,
                    dag.start_time,
                );
            }
        }
    }

    fn get_host_by_name(&self, host_name: &str) -> Id {
        if host_name == "random" {
            self.all_hosts[self.ctx.gen_range(0..self.all_hosts.len())]
        } else {
            self.actor_by_host_name[host_name]
        }
    }
}

impl EventHandler for DagManager {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_start();
                if self.left_registrations == 0 {
                    self.start_execution();
                }
            }
            RegisteredData { data_id } => {
                if let Some(dag_id) = self.registering_on_start.remove(&data_id) {
                    self.waiting_inputs[dag_id] -= 1;
                    if self.waiting_inputs[dag_id] == 0 {
                        self.ctx.emit_now(
                            NewDag {
                                dag: self.plan.dags[dag_id].dag.clone(),
                                initial_data: self.dag_inputs[dag_id].clone(),
                            },
                            self.runner_id,
                        );
                    }
                } else {
                    self.left_registrations -= 1;
                    if self.left_registrations == 0 {
                        self.start_execution();
                    }
                }
            }
        })
    }
}

pub struct ClusterSimulation {
    pub sim: Simulation,
    plan: SimulationPlan,
    system_config: SystemConfig,
    replication_strategy: Box<dyn ReplicationStrategy>,
    placement_strategy: Box<dyn DynamicPlacementStrategy>,
}

impl ClusterSimulation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        seed: u64,
        plan: SimulationPlan,
        system_config: SystemConfig,
        replication_strategy: Box<dyn ReplicationStrategy>,
        placement_strategy: Box<dyn DynamicPlacementStrategy>,
    ) -> Self {
        ClusterSimulation {
            sim: Simulation::new(seed),
            plan,
            system_config,
            replication_strategy,
            placement_strategy,
        }
    }

    pub fn run(mut self) -> RunStats {
        let network = match self.system_config.network {
            NetworkConfig::Tree {
                star_count,
                hosts_per_star,
            } => make_tree_topology(&mut self.sim, star_count, hosts_per_star),
            NetworkConfig::FatTree {
                l2_switch_count,
                l1_switch_count,
                hosts_per_switch,
            } => make_fat_tree_topology(&mut self.sim, l2_switch_count, l1_switch_count, hosts_per_switch),
        };
        let nodes = network.borrow().get_nodes();
        let mut actor_by_host_name: HashMap<String, Id> = HashMap::new();
        for node_name in nodes {
            if !node_name.starts_with("host_") {
                continue;
            }

            let data_on_host_name = format!("data_on_host_{}", &node_name[5..]);
            self.sim.create_context(&data_on_host_name);
            let data_on_host = DataOnHost {};
            let data_on_host_id = self
                .sim
                .add_handler(data_on_host_name, Rc::new(RefCell::new(data_on_host)));
            actor_by_host_name.insert(node_name.clone(), data_on_host_id);
            network.borrow_mut().set_location(data_on_host_id, &node_name);
        }

        let dfs = DistributedFileSystem::new(
            self.system_config
                .hosts
                .iter()
                .map(|host| {
                    (
                        actor_by_host_name[&host.name],
                        HostInfo {
                            free_space: host.available_space,
                            chunks: BTreeSet::new(),
                        },
                    )
                })
                .collect(),
            HashMap::new(),
            network.clone(),
            self.replication_strategy,
            self.system_config.chunk_size,
            self.sim.create_context("dfs"),
        );
        let dfs = Rc::new(RefCell::new(dfs));
        let _dfs_id = self.sim.add_handler("dfs", dfs.clone());

        let compute_hosts = self
            .system_config
            .hosts
            .iter()
            .map(|host| {
                let compute = Rc::new(RefCell::new(Compute::new(
                    host.speed,
                    host.available_cores,
                    0,
                    self.sim.create_context(&host.name),
                )));
                self.sim.add_handler(&host.name, compute.clone());
                (
                    actor_by_host_name[&host.name],
                    ComputeHost {
                        host: actor_by_host_name[&host.name],
                        speed: host.speed,
                        cores: host.available_cores,
                        available_cores: host.available_cores,
                        compute,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        let runner = Rc::new(RefCell::new(Runner::new(
            self.placement_strategy,
            compute_hosts,
            dfs.clone(),
            network.clone(),
            self.sim.create_context("runner"),
        )));
        let runner_id = self.sim.add_handler("runner", runner.clone());

        let dag_manager = Rc::new(RefCell::new(DagManager::new(
            self.plan,
            dfs.clone(),
            actor_by_host_name.clone(),
            runner_id,
            self.sim.create_context("dag_manager"),
        )));
        let dag_manager_id = self.sim.add_handler("dag_manager", dag_manager.clone());

        self.sim.create_context("root").emit_now(Start {}, dag_manager_id);
        self.sim.step_until_no_events();

        runner.borrow_mut().finalize();
        let runner_borrow = runner.borrow();
        runner_borrow.run_stats().clone()
    }
}
