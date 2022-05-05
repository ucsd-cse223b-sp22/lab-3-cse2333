use crate::keeper::keeper_work_client::KeeperWorkClient;
use crate::keeper::keeper_work_server::KeeperWork;
use crate::keeper::{Bool, Clock, Empty, Leader};
use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tribbler::config::KeeperConfig;
use tribbler::err::TribResult;
use tribbler::storage::Storage;

pub struct KeeperServer {
    pub storage: Box<dyn Storage>,
    // pub clock: RwLock<u64>,
}

#[async_trait]
impl KeeperWork for KeeperServer {
    async fn get_clock(&self, request: Request<Empty>) -> Result<Response<Clock>, Status> {
        todo!();
    }

    async fn set_clock(&self, request: Request<Clock>) -> Result<Response<Bool>, Status> {
        todo!();
    }

    async fn get_leader(&self, request: Request<Empty>) -> Result<Response<Leader>, Status> {
        todo!();
    }

    async fn set_leader(&self, request: Request<Leader>) -> Result<Response<Bool>, Status> {
        todo!();
    }
}

// #[async_trait]
// /// Bin Storage interface
#[async_trait]
pub trait KeeperClient: Send + Sync {
    // ask current leader from other keepers,
    // if found one, just return one
    // if result is 0, do the select_leader()
    async fn check_leader(&self) -> u64;
    // ask incarnation id of all other keepers,
    // then return the minimum one as the leader
    async fn select_leader(&self) -> u64;
    // heart beat the primary id,
    // if failed, means primary goes down, start selecting leader
    // if success, continue loop block itself
    async fn keeper_heart_beat(&self, primary_id: u64) -> TribResult<Bool>;
    // one of the keeper jobs: sync clock values of all backends
    async fn clock_sync(&self, clock: &str) -> TribResult<Bool>;

    // the heart beat to every backends
    async fn backend_heart_beat(&self, id: u64) -> TribResult<Bool>;
    // from one replica to a new replica
    async fn data_migration(&self, src_id: u64, dst_id: u64) -> TribResult<Bool>;

    // ? when a backend back online, how to do that
}

pub struct Keeper {
    pub keepers: Vec<String>,
    pub backs: Vec<String>,
}

#[async_trait]
impl KeeperClient for Keeper {
    async fn check_leader(&self) -> u64 {
        for elem in self.keepers {
            let mut addr_http = "http://".to_string();
            addr_http.push_str(&elem);
            let client = KeeperWorkClient::connect(addr_http.to_string()).await;
            match client {
                Ok(value) => match value.get_leader(Empty {}).await {
                    Ok(value) => {
                        if value > 0 {
                            return value;
                        }
                    }
                    Err(e) => {
                        // the get leader returns error
                        ();
                    }
                },
                Err(e) => {
                    // it could not connect to this client, just ask next
                    ();
                }
            }
        }
        return 0;
    }

    async fn select_leader(&self) -> u64 {
        todo!();
    }

    async fn keeper_heart_beat(&self, primary_id: u64) -> TribResult<Bool> {
        todo!();
    }

    async fn clock_sync(&self, clock: &str) -> TribResult<Bool> {
        //
        todo!();
    }

    // the heart beat to every backends
    async fn backend_heart_beat(&self, id: u64) -> TribResult<Bool> {
        todo!();
    }

    // from one replica to a new replica
    async fn data_migration(&self, src_id: u64, dst_id: u64) -> TribResult<Bool> {
        todo!();
    }

    // async fn client(&self, id: u64) -> TribResult<KeeperWorkClient<Channel>> {
    //     Ok(KeeperWorkClient::connect(self.keepers.get(id).clone()))
    // }
}
