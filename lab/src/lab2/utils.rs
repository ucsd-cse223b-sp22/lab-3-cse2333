use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use serde::{Deserialize, Serialize};
use tribbler::rpc::{Key, KeyValue, Pattern};
use tribbler::{err::TribResult, rpc::trib_storage_client::TribStorageClient};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct StatusTableEntry {
    pub addr: String,
    pub status: bool,
}

// Migrate data from src node to dest node
// Initial order: start -> dest -> src
// Range: (start, dest] or (start, src]
// TODO: if dest > src?
// TODO: error catching?
#[allow(unused_variables)]
pub async fn data_migration(
    start: usize,
    dst: usize,
    src: usize,
    status_table: &Vec<StatusTableEntry>,
) -> TribResult<()> {
    let mut d = TribStorageClient::connect(status_table[dst].addr.to_string()).await?;
    let mut s = TribStorageClient::connect(status_table[src].addr.to_string()).await?;
    // Key-value pair
    let all_keys = s
        .keys(Pattern {
            prefix: "".to_string(),
            suffix: "".to_string(),
        })
        .await?
        .into_inner()
        .list;
    for each_key in all_keys {
        // check if should copy this one
        let mut hasher = DefaultHasher::new();
        hasher.write(each_key.as_bytes());
        let h = hasher.finish() as usize % status_table.len();
        // TODO: check different case
        if (h <= dst && h > start) || (start > src && (h > start || h <= dst)) {
            d.set(KeyValue {
                key: each_key.to_string(),
                value: s.get(Key { key: each_key }).await?.into_inner().value,
            })
            .await?;
        }
    }

    // Key-List
    let all_list_keys = s
        .list_keys(Pattern {
            prefix: "".to_string(),
            suffix: "".to_string(),
        })
        .await?
        .into_inner()
        .list;
    for each_key in all_list_keys {
        // TODO: if each_key exists in dest, remove all entries
        let mut hasher = DefaultHasher::new();
        hasher.write(each_key.as_bytes());
        let h = hasher.finish() as usize % status_table.len();
        if (h <= dst && h > start) || (start > src && (h > start || h <= dst)) {
            // remove old entries
            let old_records = d
                .list_get(Key {
                    key: each_key.to_string(),
                })
                .await?
                .into_inner()
                .list;
            for entry in old_records {
                d.list_remove(KeyValue {
                    key: each_key.to_string(),
                    value: entry,
                })
                .await?;
            }
            // append new records
            let records = s
                .list_get(Key {
                    key: each_key.to_string(),
                })
                .await?
                .into_inner()
                .list;
            for entry in records {
                d.list_append(KeyValue {
                    key: each_key.to_string(),
                    value: entry,
                })
                .await?;
            }
        }
    }
    Ok(())
}

#[allow(unused_variables)]
pub async fn node_leave(curr: usize, status_table: &Vec<StatusTableEntry>) -> TribResult<()> {
    // find successor and the second previous node
    let len = status_table.len();
    let mut prev = (curr + len - 1) % len;
    let mut next = (curr + 1) % len;
    while !status_table[next].status {
        next = (next + 1) % len;
    }
    while !status_table[prev].status {
        prev = (prev + len - 1) % len;
    }
    let mut prev_prev = (prev + len - 1) % len;
    let mut next_next = (next + 1) % len;
    while !status_table[prev_prev].status {
        prev_prev = (prev_prev + len - 1) % len;
    }
    while !status_table[next_next].status {
        next_next = (next_next + 1) % len;
    }

    let _ = data_migration(prev_prev, next, prev, status_table).await?;
    let _ = data_migration(prev, next_next, next, status_table).await?;
    Ok(())
}

#[allow(unused_variables)]
pub async fn node_join(curr: usize, status_table: &Vec<StatusTableEntry>) -> TribResult<()> {
    // find successor and prodecessor's predecessor
    let len = status_table.len();
    let mut prev = (curr + len - 1) % len;
    let mut next = (curr + 1) % len;
    while !status_table[next].status {
        next = (next + 1) % len;
    }
    while !status_table[prev].status {
        prev = (prev + len - 1) % len;
    }
    prev = (prev + len - 1) % len;
    while !status_table[prev].status {
        prev = (prev + len - 1) % len;
    }

    // data migration from succ to curr, copy data range (prev, curr]
    return data_migration(prev, curr, next, status_table).await;
}
