use crate::lab1::client::StorageClient;
use crate::lab1::server::StorageServer;
use std::net::ToSocketAddrs;
use tokio::sync::mpsc::Receiver;
use tonic::transport::Server;
use tribbler::err::TribblerError;
use tribbler::rpc::trib_storage_server::TribStorageServer;
use tribbler::{config::BackConfig, err::TribResult, storage::Storage};

/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    // addr is the address the server should listen on, in the form of <host>:<port>
    let addr = config.addr.to_socket_addrs();
    match addr {
        Ok(value) => (),
        Err(e) => match config.ready {
            Some(channel) => {
                // send a false when you encounter any error on starting your service.
                channel.send(false).unwrap();
                return Err(Box::new(TribblerError::Unknown(
                    "Invalid addr space".to_string(),
                )));
            }
            None => (),
        },
    }

    // ready is a channel for notifying the other parts in the program that the server is ready to accept RPC calls from the network (indicated by the server sending the value true) or if the setup failed (indicated by sending false).
    // ready might be None, which means the caller does not care about when the server is ready.
    match config.ready {
        // send a true over the ready channel when the service is ready (when ready is not None),
        Some(channel) => {
            channel.send(true).unwrap();
        }
        None => (),
    }

    let next_addr = config.addr.to_socket_addrs().unwrap().next().unwrap();
    let trib_storage_server = TribStorageServer::new(StorageServer {
        storage: config.storage,
    });
    let storage_server = Server::builder().add_service(trib_storage_server);

    // shutdown is another type of channel for receiving a shutdown notification.
    // when a message is received on this channel, the server should shut down.
    pub async fn receive(mut receiver: Receiver<()>) {
        receiver.recv().await;
    }

    match config.shutdown {
        Some(channel) => {
            storage_server
                .serve_with_shutdown(next_addr, receive(channel))
                .await?;
        }
        None => {
            storage_server.serve(next_addr).await?;
        }
    };
    Ok(())

    // should block indefinitely unless there is errors or the server is sent a shutdown signal. It is async, you should be able to call .await on futures within it.
}

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<Box<dyn Storage>> {
    Ok(Box::new(StorageClient {
        addr: addr.to_string(),
    }))
}
