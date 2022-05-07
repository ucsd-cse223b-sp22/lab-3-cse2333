use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use serde::{Deserialize, Serialize};
use tribbler::colon::unescape;
use tribbler::rpc::{Key, KeyValue, Pattern};
use tribbler::{err::TribResult, rpc::trib_storage_client::TribStorageClient};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct StatusTableEntry {
    pub addr: String,
    pub status: bool,
}

#[allow(unused_variables)]
pub async fn write_twice(
    message: String,
    backend: usize,
    status_table: &Vec<StatusTableEntry>,
) -> TribResult<()> {
    let mut index = backend;

    // write  into promary
    while !status_table[index].status {
        index = (index + 1) % status_table.len();
    }
    let mut addr_http = "http://".to_string();
    addr_http.push_str(&status_table[index].addr.clone());
    let mut client = TribStorageClient::connect(addr_http.to_string()).await?;

    client
        .set(KeyValue {
            key: "BackendStatus".to_string(),
            value: message.to_string(),
        })
        .await?;

    // write into replica
    index = (index + 1) % status_table.len();
    while !status_table[index].status {
        index = (index + 1) % status_table.len();
    }
    let mut replica_addr_http = "http://".to_string();
    replica_addr_http.push_str(&status_table[index].addr.clone());
    let mut replica_client = TribStorageClient::connect(replica_addr_http.to_string()).await?;

    replica_client
        .set(KeyValue {
            key: "BackendStatus".to_string(),
            value: message.to_string(),
        })
        .await?;

    Ok(())
}

// Migrate data from src node to dest node
// Initial order: start -> dest -> src
// Range: (start, dest] or (start, src]
// TODO: error catching?
#[allow(unused_variables)]
pub async fn data_migration(
    start: usize,
    dst: usize,
    src: usize,
    leave: bool,
    status_table: &Vec<StatusTableEntry>,
) -> TribResult<()> {
    // println!("DATA MIGRATION INFORMATION");
    // println!("start {}, dst {}, src {}, leave {}", start, dst, src, leave);
    // connect to dest and src
    let mut addr_http = "http://".to_string();
    addr_http.push_str(&status_table[dst].addr);
    let mut d = TribStorageClient::connect(addr_http).await?;
    let mut addr_http0 = "http://".to_string();
    addr_http0.push_str(&status_table[src].addr);
    let mut s = TribStorageClient::connect(addr_http0).await?;

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
        let tmp: Vec<String> = each_key.split("::").map(|x| x.to_string()).collect();
        let unescape_key = unescape(tmp.get(0).unwrap()).to_string();
        // check if should copy this one
        let mut hasher = DefaultHasher::new();
        hasher.write(unescape_key.as_bytes());
        let h = hasher.finish() as usize % status_table.len();
        // println!("hash: {}, key: {}", h, each_key);
        if (!leave && ((h <= dst && h > start) || (start > dst && (h > start || h <= dst))))
            || (leave && ((h <= src && h > start) || (start > src && (h > start || h <= src))))
        {
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
        let tmp: Vec<String> = each_key.split("::").map(|x| x.to_string()).collect();
        let unescape_key = unescape(tmp.get(0).unwrap()).to_string();
        let mut hasher = DefaultHasher::new();
        hasher.write(unescape_key.as_bytes());
        let h = hasher.finish() as usize % status_table.len();
        if (!leave && ((h <= dst && h > start) || (start > dst && (h > start || h <= dst))))
            || (leave && ((h <= src && h > start) || (start > src && (h > start || h <= src))))
        {
            // remove old entries (no need to do it now)
            // let old_records = d
            //     .list_get(Key {
            //         key: each_key.to_string(),
            //     })
            //     .await?
            //     .into_inner()
            //     .list;
            // for entry in old_records {
            //     d.list_remove(KeyValue {
            //         key: each_key.to_string(),
            //         value: entry,
            //     })
            //     .await?;
            // }
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

    let _ = data_migration(prev, next_next, next, true, status_table).await?;
    let _ = data_migration(prev_prev, next, prev, true, status_table).await?;
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
    return data_migration(prev, curr, next, false, status_table).await;
}
