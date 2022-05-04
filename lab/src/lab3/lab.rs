use super::server::FrontServer;
use crate::lab3::client::BinStorageClient;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use tokio::{select, time};
use tonic::Status;
use tribbler::err::TribblerError;
use tribbler::rpc::{Clock, Key, KeyValue};
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
    Ok(Box::new(BinStorageClient { backs: backs }))
}

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

    let status_addr = status_table[status_storage_index].addr.clone();
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
            if e.message().eq("No key provided") {
                let serialized_table = serde_json::to_string(&status_table).unwrap();
                status_client
                    .set(KeyValue {
                        key: "backendStatus".to_string(),
                        value: serialized_table,
                    })
                    .await?;
            } else {
                println!("SHOULD NOT APPEAR");
            }
        }
    }
    let mut clock: u64 = 0;

    select! {
        _ =  async {
                loop {
                    let mut clocks = Vec::new();
                    for addr in kc.backs.iter() {
                        let mut addr_http = "http://".to_string();
                        addr_http.push_str(addr);
                        let client = TribStorageClient::connect(addr_http.to_string()).await;
                        match client {
                            Ok(value) => {
                                // compare with the status table
                                // TODO 2.1: node joins
                                // data migration for joining (table, node)
                                let mut c = value;
                                match c.clock(Clock { timestamp: clock }).await {
                                    Ok(v0) => {
                                        clocks.push(v0.into_inner().timestamp);
                                    }
                                    Err(e) => {
                                        return Box::new(TribblerError::Unknown(e.to_string()));
                                    }
                                }
                            }
                            Err(e) => {
                                // compare with the status table
                                // TODO 2.2: node fails
                                // data migrate for failure (table, node)
                                return Box::new(TribblerError::Unknown(e.to_string()));
                            }
                        }
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
                    time::sleep(time::Duration::from_secs(1)).await;
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

// TODO: data migration
