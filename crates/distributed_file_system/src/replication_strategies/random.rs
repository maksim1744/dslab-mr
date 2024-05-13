use std::collections::BTreeMap;

use dslab_core::Id;
use dslab_network::Network;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;

use crate::{
    host_info::{ChunkId, HostInfo},
    network::get_all_racks,
    replication_strategy::ReplicationStrategy,
};

pub enum ChunkDistribution {
    AllowEverything,
    ProhibitSameRack,
}

pub struct RandomReplicationStrategy {
    replication_factor: usize,
    chunk_distribution: ChunkDistribution,
    rng: Pcg64,
}

impl RandomReplicationStrategy {
    pub fn new(replication_factor: usize, chunk_distribution: ChunkDistribution) -> Self {
        Self {
            replication_factor,
            chunk_distribution,
            rng: Pcg64::seed_from_u64(123),
        }
    }

    fn pick_random(&mut self, n: usize, k: usize) -> Vec<usize> {
        if k >= n {
            return (0..n).collect();
        }
        let mut result = Vec::new();
        for i in 0..k {
            let mut x = self.rng.gen_range(0..n - i);
            for &y in result.iter() {
                if y <= x {
                    x += 1;
                }
            }
            result.insert(result.partition_point(|&y| y < x), x);
        }
        assert!(result.windows(2).all(|a| a[0] < a[1]));
        result
    }
}

impl ReplicationStrategy for RandomReplicationStrategy {
    fn register_chunks(
        &mut self,
        chunk_size: u64,
        host: Id,
        chunks: &[ChunkId],
        need_to_replicate: bool,
        host_info: &BTreeMap<Id, HostInfo>,
        network: &Network,
    ) -> BTreeMap<ChunkId, Vec<Id>> {
        if !need_to_replicate {
            return chunks.iter().copied().map(|chunk| (chunk, vec![host])).collect();
        }
        let mut result = BTreeMap::new();
        let mut free_space = host_info
            .iter()
            .map(|(id, host)| (*id, host.free_space))
            .collect::<BTreeMap<_, _>>();
        let host_racks = get_all_racks(network);
        for &chunk in chunks.iter() {
            let mut by_racks: BTreeMap<u64, Vec<Id>> = BTreeMap::new();
            for &host in free_space
                .iter()
                .filter(|&(_id, &free_space)| free_space >= chunk_size)
                .map(|(id, _free_space)| id)
            {
                let rack = host_racks[&network.get_location(host)];
                by_racks.entry(rack).or_default().push(host);
            }
            let all_hosts = free_space
                .iter()
                .filter(|&(_id, &free_space)| free_space >= chunk_size)
                .map(|(id, _free_space)| id)
                .copied()
                .collect::<Vec<_>>();
            let all_racks = by_racks.keys().copied().collect::<Vec<_>>();
            let hosts: Vec<Id> = match self.chunk_distribution {
                ChunkDistribution::AllowEverything => {
                    let inds = self.pick_random(all_hosts.len(), self.replication_factor);
                    inds.into_iter().map(|i| all_hosts[i]).collect()
                }
                ChunkDistribution::ProhibitSameRack => {
                    let inds = self.pick_random(all_racks.len(), self.replication_factor);
                    inds.into_iter()
                        .map(|i| all_racks[i])
                        .map(|rack| by_racks[&rack][self.rng.gen_range(0..by_racks[&rack].len())])
                        .collect()
                }
            };
            for host in hosts.iter() {
                *free_space.get_mut(host).unwrap() -= chunk_size;
            }
            result.insert(chunk, hosts);
        }
        result
    }
}
