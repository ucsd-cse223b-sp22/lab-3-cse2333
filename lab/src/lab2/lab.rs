use std::net::ToSocketAddrs;

#[allow(unused_variables)]
use super::server::FrontServer;
use crate::keeper::keeper_work_client::KeeperWorkClient;
use crate::keeper::keeper_work_server::KeeperWorkServer;
use crate::keeper::{Index, Leader};
use crate::lab2::client::BinStorageClient;
use tonic::transport::Server;

use crate::lab3::myKeeper::{Keeper, KeeperClient, KeeperServer};
use tokio::{select, time};
use tribbler::storage::MemStorage;
use tribbler::{config::KeeperConfig, err::TribResult, storage::BinStorage};

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    Ok(Box::new(BinStorageClient { backs }))
}

/// this async function accepts a [KeeperConfig] that should be used to start
/// a new keeper server on the address given in the config.
///
/// This function should block indefinitely and only return upon erroring. Make
/// sure to send the proper signal to the channel in `kc` when the keeper has
/// started.
#[allow(unused_variables)]
pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
    let mut addr_http = "http://".to_string();
    addr_http.push_str(kc.addrs.get(kc.this).unwrap());

    select! {
        _ =  async {
            println!("the {} keep server starts", addr_http);
            // the server thread
                // let server starts to work
                let server = KeeperServer {
                    storage: Box::new(MemStorage::new()),
                };
                let keep_server = KeeperWorkServer::new(server);
                let addr = kc.addrs.get(kc.this).unwrap().to_socket_addrs().unwrap().next();

                let res = match addr {
                    Some(value) => {
                        Server::builder()
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
            println!("the {} keep client starts", addr_http);
            let keeper = Keeper {
                keepers: kc.addrs.clone(),
                backs: kc.backs.clone(),
            };

            let client = KeeperWorkClient::connect(addr_http.to_string()).await;
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
                            println!("{} sets the index is {}", addr_http, kc.this);
                        },
                        Err(e) => {
                            println!("{} set index goes wrong", addr_http);
                            println!("{:?}", e);
                        },
                    };
                }
                Err(e) => {
                    ();
                }
            }
            println!("the {} keep client check_leader", addr_http);
            match kc.ready {
                // send a true over the ready channel when the service is ready (when ready is not None),
                Some(channel) => {
                    channel.send(true).unwrap();
                }
                None => (),
            }
            // just go online
            let res = keeper.check_leader().await;
            println!("the current leader is {}",res);
            if res > 0 {
                println!("{} finds there is a leader", addr_http);
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
                println!("the {} keep client is not the leader", addr_http);
                let client = KeeperWorkClient::connect(addr_http.to_string()).await;
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
                    println!("the {} keep client is not the leader", addr_http);
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
                                println!("{} is dead", leader_id);
                                primary_alive = false;
                            }
                        }
                    }
                } else {
                    println!("the {} keep client is the leader", addr_http);
                    loop{
                        time::sleep(time::Duration::from_secs(1)).await;
                        println!("{} do leader's work", addr_http);
                    }
                // if this keeper is the leader,
                // do clock sync and data migration
                    continue;

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
