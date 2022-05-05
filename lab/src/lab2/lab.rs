use super::server::FrontServer;
use crate::lab2::client::BinStorageClient;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use tokio::{select, time};
// use tonic::codegen::http::status;
use tribbler::err::TribblerError;
use tribbler::rpc::{Clock, Key, KeyValue, Pattern};
use tribbler::{
    config::KeeperConfig, err::TribResult, rpc::trib_storage_client::TribStorageClient,
    storage::BinStorage, trib::Server,
};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct StatusTableEntry {
    pub addr: String,
    pub status: bool,
}

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    Ok(Box::new(BinStorageClient {
        backs,
        server_table: todo!(),
    }))
}

// TODO: migration interrupt, keeper redo log
/// this async function accepts a [KeeperConfig] that should be used to start
/// a new keeper server on the address given in the config.
///
/// This function should block indefinitely and only return upon erroring. Make
/// sure to send the proper signal to the channel in `kc` when the keeper has
/// started.
#[allow(unused_variables)]
pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
    // send a true over the ready channel when the service is ready (when ready is not None),
    match kc.ready {
        Some(channel) => {
            channel.send(true).unwrap();
        }
        None => (),
    }

    // get a initial status table
    let mut status_table: Vec<StatusTableEntry> = Vec::new();
    for addr in kc.backs.iter() {
        let mut addr_http = "http://".to_string();
        addr_http.push_str(addr);
        let client = TribStorageClient::connect(addr_http.to_string()).await;
        match client {
            Ok(value) => status_table.push(StatusTableEntry {
                addr: addr.to_string(),
                status: true,
            }),
            Err(e) => status_table.push(StatusTableEntry {
                addr: addr.to_string(),
                status: false,
            }),
        }
    }

    // try to fetch the previous one
    let mut hasher = DefaultHasher::new();
    hasher.write("BackendStatus".as_bytes());
    let mut status_storage_index = hasher.finish() as usize % kc.backs.len();
    while !status_table[status_storage_index].status {
        status_storage_index = (status_storage_index + 1) % kc.backs.len();
    }

    let mut status_addr = "http://".to_string();
    status_addr.push_str(&status_table[status_storage_index].addr.clone());
    let mut status_client = TribStorageClient::connect(status_addr.to_string()).await?;
    let previous_status_table = status_client
        .get(Key {
            key: "BackendStatus".to_string(),
        })
        .await;

    match previous_status_table {
        Ok(value) => {
            status_table =
                serde_json::from_str::<Vec<StatusTableEntry>>(&value.into_inner().value).unwrap();
        }
        Err(e) => {
            // TODO: store into replica
            if e.message().eq("No key provided") {
                let serialized_table = serde_json::to_string(&status_table).unwrap();
                status_client
                    .set(KeyValue {
                        key: "BackendStatus".to_string(),
                        value: serialized_table,
                    })
                    .await?;
            } else {
                println!("SHOULD NOT APPEAR");
            }
        }
    }

    // now status_table stores the previous recorded backend status table
    let mut clock: u64 = 0;
    select! {
        _ =  async {
                loop {
                    let mut clocks = Vec::new();
                    for i in 0..kc.backs.len() {
                        let mut addr_http = "http://".to_string();
                        addr_http.push_str(&kc.backs[i]);
                        let client = TribStorageClient::connect(addr_http.to_string()).await;
                        match client {
                            Ok(value) => {
                                let mut c = value;
                                match c.clock(Clock { timestamp: clock }).await {
                                    Ok(v0) => {
                                        clocks.push(v0.into_inner().timestamp);
                                    }
                                    Err(e) => {
                                        return Box::new(TribblerError::Unknown(e.to_string()));
                                    }
                                }
                                // newly joined node
                                if !status_table[i].status {
                                    match node_join(i, &status_table).await {
                                        Ok(_) => {println!("Node {} join succeeded", i);},
                                        Err(_) => {println!("Node {} join failed", i);},
                                    }
                                    status_table[i].status = true;
                                }
                                // ============ DEBUG ============
                                println!("***************** backend {} ***************** ", i);
                                match c.keys(Pattern {prefix:"".to_string(), suffix:"".to_string()}).await {
                                    Ok(keys) => {
                                        for k in keys.into_inner().list {
                                            match c.get(Key{ key: k.to_string()}).await {
                                                Ok(vv) => {
                                                    println!("key: {}, value: {}", k.to_string(), vv.into_inner().value);
                                                }
                                                Err(e) => (),
                                            }
                                        }
                                    },
                                    Err(_) => (),
                                }
                                println!("\n");
                                // ============ DEBUG ============
                            }
                            Err(e) => {
                                // node leaves
                                if status_table[i].status {
                                    match node_leave(i, &status_table).await {
                                        Ok(_) => {println!("Node {} leave succeeded", i);},
                                        Err(_) => {println!("Node {} leave failed", i);},
                                    }
                                    status_table[i].status = false;
                                }
                                return Box::new(TribblerError::Unknown(e.to_string()));
                            }
                        }
                    }
                    // write the updated status_table into storage
                    // TODO: store replica
                    let serialized_table = serde_json::to_string(&status_table).unwrap();
                    let mut hasher = DefaultHasher::new();
                    hasher.write("BackendStatus".as_bytes());
                    let mut status_storage_index = hasher.finish() as usize % kc.backs.len();
                    while !status_table[status_storage_index].status {
                        status_storage_index = (status_storage_index + 1) % kc.backs.len();
                    }
                    match TribStorageClient::connect(status_addr.to_string()).await {
                        Ok(mut client) => {
                            match client.set(KeyValue {
                            key: "BackendStatus".to_string(),
                            value: serialized_table,
                        }).await {
                            Ok(_) => (),
                            Err(_) => (),
                        }},
                        Err(_) => (),
                    }

                    clock = *clocks.iter().max().unwrap();
                    for addr in kc.backs.iter() {
                        let mut addr_http = "http://".to_string();
                        addr_http.push_str(addr);
                        match TribStorageClient::connect(addr_http.to_string()).await {
                            Ok(mut c) => {let _ = c.clock(Clock { timestamp: clock });}
                            Err(e) => {return Box::new(TribblerError::Unknown(e.to_string()));}
                        }
                    }
                    time::sleep(time::Duration::from_secs(5)).await;
                }
            } => {}
        _ = async {
            if let Some(mut rx) = kc.shutdown {
                rx.recv().await;
            } else {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
                rx.recv().await;
            }
        } => {}
    }
    return Ok(());
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

/// this function accepts a [BinStorage] client which should be used in order to
/// implement the [Server] trait.
///
/// You'll need to translate calls from the tribbler front-end into storage
/// calls using the [BinStorage] interface.
///
/// Additionally, two trait bounds [Send] and [Sync] are required of your
/// implementation. This should guarantee your front-end is safe to use in the
/// tribbler front-end service launched by the`trib-front` command
#[allow(unused_variables)]
pub async fn new_front(
    bin_storage: Box<dyn BinStorage>,
) -> TribResult<Box<dyn Server + Send + Sync>> {
    Ok(Box::new(FrontServer {
        bin_storage: bin_storage,
    }))
}
