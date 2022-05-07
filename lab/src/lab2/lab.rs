use super::client::scan_server;
use std::net::ToSocketAddrs;

#[allow(unused_variables)]
use super::server::FrontServer;
use crate::keeper::keeper_work_client::KeeperWorkClient;
use crate::keeper::keeper_work_server::KeeperWorkServer;
use crate::keeper::{Index, Leader};
use crate::lab2::client::BinStorageClient;
use crate::lab2::utils::{node_join, node_leave, StatusTableEntry};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use tokio::{select, time};
use tonic::transport::Server;
use tribbler::err::TribblerError;
use tribbler::rpc::{Clock, Key, KeyValue, Pattern};
use tribbler::{
    config::KeeperConfig, err::TribResult, rpc::trib_storage_client::TribStorageClient,
    storage::BinStorage,
};

use crate::lab3::myKeeper::{Keeper, KeeperClient, KeeperServer};
use tribbler::storage::MemStorage;

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    Ok(Box::new(BinStorageClient { backs }))
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
    // TODO: use tokio::spwan
    // let mut status_table: Vec<StatusTableEntry> = Vec::new();
    // for addr in kc.backs.iter() {
    //     let mut addr_http = "http://".to_string();
    //     addr_http.push_str(addr);
    //     let client = TribStorageClient::connect(addr_http.to_string()).await;
    //     match client {
    //         Ok(value) => status_table.push(StatusTableEntry {
    //             addr: addr.to_string(),
    //             status: true,
    //         }),
    //         Err(e) => status_table.push(StatusTableEntry {
    //             addr: addr.to_string(),
    //             status: false,
    //         }),
    //     }
    // }
    let mut status_table = scan_server(kc.backs.clone()).await;

    let mut kc_addr_http = "http://".to_string();
    kc_addr_http.push_str(kc.addrs.get(kc.this).unwrap());

    // try to fetch the previous one
    let mut hasher = DefaultHasher::new();
    hasher.write("BackendStatus".as_bytes());
    let backend_hash = hasher.finish() as usize % kc.backs.len();
    let mut status_storage_index = backend_hash;
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
            if e.message().eq("No key provided") {
                let serialized_table = serde_json::to_string(&status_table).unwrap();
                status_client
                    .set(KeyValue {
                        key: "BackendStatus".to_string(),
                        value: serialized_table.to_string(),
                    })
                    .await?;
                // store replica
                let mut replica_index = (status_storage_index + 1) % kc.backs.len();
                while !status_table[replica_index].status {
                    replica_index = (replica_index + 1) % kc.backs.len();
                }
                let mut replica_addr = "http://".to_string();
                replica_addr.push_str(&status_table[replica_index].addr.clone());
                let mut replica_client =
                    TribStorageClient::connect(replica_addr.to_string()).await?;
                replica_client
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
            println!("the {} keep server starts", kc_addr_http);
            // the server thread
                // let server starts to work
                let server = KeeperServer {
                    storage: Box::new(MemStorage::new()),
                };
                let keep_server = KeeperWorkServer::new(server);
                let addr = kc.addrs.get(kc.this).unwrap().to_socket_addrs().unwrap().next();

                let res = match addr {
                    Some(value) => {
                        let x = Server::builder()
                            .add_service(keep_server)
                            .serve(value)
                            .await;
                    }
                    None => (),
                };
        } => {}
        _ =  async {
            // the client thread
            time::sleep(time::Duration::from_secs(1)).await;
            // println!("the {} keep client starts", kc_addr_http);
            let keeper = Keeper {
                keepers: kc.addrs.clone(),
                backs: kc.backs.clone(),
            };

            let client = KeeperWorkClient::connect(kc_addr_http.to_string()).await;
            // connect to itself and set an empty leader id
            match client {
                Ok(value) => {
                    let mut c = value;
                    match c.set_leader(Leader{ leader_id: -1 }).await {
                        Ok(_) => (),
                        Err(_) => (),
                    };
                    match c.set_index(Index{index: (kc.this as i64) }).await{
                        Ok(_) => {
                            // println!("{} sets the index is {}", kc_addr_http, kc.this);
                        },
                        Err(e) => {
                            println!("{} set index goes wrong", kc_addr_http);
                            println!("{:?}", e);
                        },
                    };
                }
                Err(e) => {
                    ();
                }
            }
            // println!("the {} keep client check_leader", kc_addr_http);
            // just go online
            let res = keeper.check_leader().await;
            // println!("the current leader is {}",res);
            if res > 0 {
                // println!("{} finds there is a leader", kc_addr_http);
                // start heartbeat
                let mut primary_alive = true;
                while primary_alive {
                    // send heart beat to the leader of current view
                    let res = keeper.keeper_heart_beat(kc.this as i64, res).await;
                    match res {
                        Ok(value) => {
                            // the primary alive wait 1 sec
                            time::sleep(time::Duration::from_secs(1)).await;
                        }
                        Err(e) => {
                            primary_alive = false;
                        }
                    }
                }
            }

            loop {
                // start selecting leader
                let leader_id = keeper.select_leader().await;
                // println!("the {} keep client is not the leader", kc_addr_http);
                let client = KeeperWorkClient::connect(kc_addr_http.to_string()).await;
                match client {
                    Ok(value) => {
                        let mut c = value;
                        match c.set_leader(Leader{ leader_id: leader_id }).await {
                            Ok(_) => (),
                            Err(_) => (),
                        };
                    },
                    Err(e) => {
                        ();
                    }
                }
                // if this keeper is not the leader,
                // block it in a hear_beat
                if leader_id != (kc.this as i64) {
                    // println!("the {} keep client is not the leader", kc_addr_http);
                    // start heartbeat
                    let mut primary_alive = true;
                    while primary_alive {
                        // send heart beat to the leader of current view
                        let res = keeper.keeper_heart_beat(kc.this as i64, leader_id).await;
                        match res {
                            Ok(value) => {
                                // the primary alive wait 1 sec
                                time::sleep(time::Duration::from_secs(1)).await;
                            }
                            Err(e) => {
                                // println!("{} is dead", leader_id);
                                primary_alive = false;
                            }
                        }
                    }
                } else {
                    println!("the {} keep client is the leader", kc_addr_http);
                    loop{
                        println!(" {} starts do its work", kc_addr_http);
                        // **********************************************************************
                        // **********************************************************************
                        // **********************************************************************
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
                                    match c.list_keys(Pattern {prefix:"".to_string(), suffix:"".to_string()}).await {
                                        Ok(keys) => {
                                            for k in keys.into_inner().list {
                                                match c.list_get(Key{ key: k.to_string()}).await {
                                                    Ok(vv) => {
                                                        println!("key: {}, list:", k.to_string());
                                                        for vvv in vv.into_inner().list {
                                                            println!("{}", vvv);
                                                        }
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
                                    println!("Connect to backend {} failed", i);
                                    // return Box::new(TribblerError::Unknown(e.to_string()));
                                }
                            }
                        }
                        // write the updated status_table into storage
                        let serialized_table = serde_json::to_string(&status_table).unwrap();
                        let mut status_storage_index = backend_hash;
                        while !status_table[status_storage_index].status {
                            status_storage_index = (status_storage_index + 1) % kc.backs.len();
                        }
                        let mut new_status_addr = "http://".to_string();
                        new_status_addr.push_str(&status_table[status_storage_index].addr.clone());
                        match TribStorageClient::connect(new_status_addr.to_string()).await {
                            Ok(mut client) => {
                                let _ = client.set(KeyValue {
                                    key: "BackendStatus".to_string(),
                                    value: serialized_table.to_string(),
                                }).await;
                            },
                            Err(_) => { println!("SHOULD NOT APPEAR 2"); },
                        }
                        // store replica
                        let mut replica_index = (status_storage_index + 1) % kc.backs.len();
                        while !status_table[replica_index].status {
                            replica_index = (replica_index + 1) % kc.backs.len();
                        }
                        let mut replica_addr = "http://".to_string();
                        replica_addr.push_str(&status_table[replica_index].addr.clone());
                        match TribStorageClient::connect(replica_addr.to_string()).await {
                            Ok(mut client) => {
                                let _ = client.set(KeyValue {
                                    key: "BackendStatus".to_string(),
                                    value: serialized_table,
                                }).await;
                            },
                            Err(_) => { println!("SHOULD NOT APPEAR 3"); },
                        }
                        clock = *clocks.iter().max().unwrap();
                        for addr in kc.backs.iter() {
                            let mut addr_http = "http://".to_string();
                            addr_http.push_str(addr);
                            match TribStorageClient::connect(addr_http.to_string()).await {
                                Ok(mut c) => {let _ = c.clock(Clock { timestamp: clock });}
                                Err(e) => (),
                            }
                        }
                        time::sleep(time::Duration::from_secs(3)).await;
                        // **********************************************************************
                        // **********************************************************************
                        // **********************************************************************
                    }

                // if this keeper is the leader,
                // do clock sync and data migration
                }

            }

        } => {}
        _ = async {            //the shutdown thread
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
) -> TribResult<Box<dyn tribbler::trib::Server + Send + Sync>> {
    Ok(Box::new(FrontServer {
        bin_storage: bin_storage,
    }))
}
