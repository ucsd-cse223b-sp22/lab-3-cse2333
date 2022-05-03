use super::server::FrontServer;
use crate::lab2::client::BinStorageClient;
use tokio::{select, time};
use tribbler::err::TribblerError;
use tribbler::rpc::Clock;
use tribbler::{
    config::KeeperConfig, err::TribResult, rpc::trib_storage_client::TribStorageClient,
    storage::BinStorage, trib::Server,
};

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
    match kc.ready {
        // send a true over the ready channel when the service is ready (when ready is not None),
        Some(channel) => {
            channel.send(true).unwrap();
        }
        None => (),
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
                                return Box::new(TribblerError::Unknown(e.to_string()));
                            }
                        }
                    }
                    clock = *clocks.iter().max().unwrap();
                    // println!("{}", clock);
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
