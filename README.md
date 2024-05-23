# Cluster simulation

A library for studying the scheduling of MapReduce and Spark application on a cluster. The cluster is modelled as a set of [hosts](https://github.com/maksim1744/dslab-mr/tree/main/crates/runtime/src/compute_host.rs) connected by [network](https://github.com/maksim1744/dslab-mr/tree/main/crates/distributed_file_system/src/network.rs) with an arbitrary topology. Each host has a fixed number of cores with some speed and some amount of memory and available space.

The network is modelled using [dslab-network crate](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-network) and computing resources are modelled [dslab-compute crate](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-compute) using `multicore` model.

All data is stored on a cluster using [dslab-dfs](https://github.com/maksim1744/dslab-mr/tree/main/crates/distributed_file_system) crate which splits every new data into chunks of a fixed sized and replicates each on a set of hosts decided by custom [ReplicationStrategy](https://github.com/maksim1744/dslab-mr/tree/main/crates/distributed_file_system/src/replication_strategy.rs).

The cluster accepts incoming [graphs](https://github.com/maksim1744/dslab-mr/tree/main/crates/runtime/src/dag.rs) and schedules them on a cluster using custom [PlacementStrategy](https://github.com/maksim1744/dslab-mr/tree/main/crates/runtime/src/placement_strategy.rs).

[ClusterSimulation](https://github.com/maksim1744/dslab-mr/tree/main/crates/runtime/src/cluster_simulation.rs) can be used to simplify running simulations like in the example below.

```rust
use dslab_mr::cluster_simulation::{ClusterSimulation, SimulationPlan};
use dslab_mr::placement_strategies::random::RandomPlacementStrategy;
use dslab_mr::system::SystemConfig;
use dslab_dfs::replication_strategies::random::{ChunkDistribution, RandomReplicationStrategy};

fn main() {
    let sim = ClusterSimulation::new(
        123,
        SimulationPlan::from_yaml("../../examples/simple/plan.yaml", "../../examples/simple/".into()),
        SystemConfig::from_yaml("../../examples/simple/system.yaml"),
        Box::new(RandomReplicationStrategy::new(2, ChunkDistribution::ProhibitSameRack)),
        Box::new(RandomPlacementStrategy::new()),
        Some("../../examples/simple/trace.json".into()),
    );

    let run_stats = sim.run();
    println!("\nRun stats:\n{}", serde_yaml::to_string(&run_stats).unwrap());
}
```

For more examples, see [examples](https://github.com/maksim1744/dslab-mr/tree/main/examples/) or [run_experiment](https://github.com/maksim1744/dslab-mr/tree/main/tools/run_experiment/). For tools useful in common scenarios see [tools](https://github.com/maksim1744/dslab-mr/tree/main/tools/).

## Architecture

![](https://raw.githubusercontent.com/maksim1744/dslab-mr/main/architecture.png)

## Documentation

[Docs](https://maksim1744.github.io/dslab-mr/docs/dslab_mr)
