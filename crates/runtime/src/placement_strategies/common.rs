use dslab_core::Id;
use dslab_dfs::dfs::DistributedFileSystem;
use rand::Rng;
use rand_pcg::Pcg64;

use crate::data_item::DataItem;

pub fn collect_all_input(input: &[DataItem], dfs: &DistributedFileSystem) -> Vec<(DataItem, Vec<Id>)> {
    let mut all_inputs = Vec::new();
    for data_item in input.iter() {
        match data_item {
            DataItem::Chunk { chunk_id, .. } => {
                all_inputs.push((
                    *data_item,
                    dfs.chunk_location(*chunk_id)
                        .map(|locations| locations.iter().copied().collect::<Vec<Id>>())
                        .unwrap_or_default(),
                ));
            }
            DataItem::Local { mut size, host } => {
                while size > 0 {
                    let size_here = if size >= dfs.chunk_size() * 2 {
                        dfs.chunk_size()
                    } else {
                        size
                    };
                    all_inputs.push((
                        DataItem::Local {
                            size: size_here,
                            host: *host,
                        },
                        vec![*host],
                    ));
                    size -= size_here;
                }
            }
            DataItem::Replicated { data_id, .. } => {
                for &chunk_id in dfs.data_chunks(*data_id).unwrap().iter() {
                    all_inputs.push((
                        DataItem::Chunk {
                            size: dfs.chunk_size(),
                            chunk_id,
                        },
                        dfs.chunk_location(chunk_id)
                            .map(|locations| locations.iter().copied().collect::<Vec<Id>>())
                            .unwrap_or_default(),
                    ));
                }
            }
        }
    }
    all_inputs
}

pub fn shuffle<T>(rng: &mut Pcg64, data: &mut [T]) {
    for i in 1..data.len() {
        data.swap(i, rng.gen_range(0..=i));
    }
}
