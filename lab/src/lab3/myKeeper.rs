use crate::keeper::keeper_work_client::KeeperWorkClient;
use crate::keeper::keeper_work_server::KeeperWork;
use crate::keeper::{Bool, Empty, Index, Leader};
use async_trait::async_trait;
use tonic::{Request, Response, Status};
use tribbler::err::{TribResult, TribblerError};
use tribbler::storage::{KeyValue, Storage};

pub struct KeeperServer {
    pub storage: Box<dyn Storage>,
    // pub clock: RwLock<i64>,
}

#[async_trait]
impl KeeperWork for KeeperServer {
    async fn get_index(&self, _: Request<Empty>) -> Result<Response<Index>, Status> {
        let result = self.storage.get("index").await;
        match result {
            Ok(value) => match value {
                Some(v) => Ok(Response::new(Index {
                    index: v.parse::<i64>().unwrap(),
                })),
                None => Err(Status::invalid_argument("No key provided")),
            },
            //err is maybe it doesn't have "index" key
            Err(_) => Err(Status::invalid_argument("index doesn't exist")),
        }
    }

    async fn set_index(&self, request: Request<Index>) -> Result<Response<Bool>, Status> {
        let request_inner = request.into_inner();
        let result = self
            .storage
            .set(&KeyValue {
                key: "index".to_string(),
                value: request_inner.index.to_string().clone(),
            })
            .await;
        match result {
            Ok(value) => Ok(Response::new(Bool { value: value })),
            Err(_) => Err(Status::invalid_argument("set_index failed")),
        }
    }

    async fn get_leader(&self, _: Request<Empty>) -> Result<Response<Leader>, Status> {
        let result = self.storage.get("leader_id").await;
        match result {
            Ok(value) => match value {
                Some(v) => Ok(Response::new(Leader {
                    leader_id: v.parse::<i64>().unwrap(),
                })),
                None => Err(Status::invalid_argument("No key provided")),
            },
            //err is maybe it doesn't have "index" key
            Err(_) => Err(Status::invalid_argument("leader_id doesn't exist")),
        }
    }

    async fn set_leader(&self, request: Request<Leader>) -> Result<Response<Bool>, Status> {
        let request_inner = request.into_inner();
        let result = self
            .storage
            .set(&KeyValue {
                key: "leader_id".to_string(),
                value: request_inner.leader_id.to_string(),
            })
            .await;
        match result {
            Ok(value) => Ok(Response::new(Bool { value })),
            Err(_) => Err(Status::invalid_argument("leader_id failed")),
        }
    }
}

// #[async_trait]
// /// Bin Storage interface
#[async_trait]
pub trait KeeperClient: Send + Sync {
    // ask current leader from other keepers,
    // if found one, just return one
    // if result is 0, do the select_leader()
    async fn check_leader(&self) -> i64;
    // ask incarnation id of all other keepers,
    // then return the minimum one as the leader
    async fn select_leader(&self) -> i64;
    // heart beat the primary id,
    // if failed, means primary goes down, start selecting leader
    // if success, continue loop block itself
    async fn keeper_heart_beat(&self, id: i64, primary_id: i64) -> TribResult<Bool>;

    // // the heart beat to every backends
    // async fn backend_heart_beat(&self, id: i64) -> TribResult<Bool>;
    // // from one replica to a new replica
    // async fn data_migration(&self, src_id: i64, dst_id: i64) -> TribResult<Bool>;

    // ? when a backend back online, how to do that
}

pub struct Keeper {
    pub keepers: Vec<String>,
    pub backs: Vec<String>,
}

#[async_trait]
impl KeeperClient for Keeper {
    async fn check_leader(&self) -> i64 {
        for elem in &self.keepers {
            let mut addr_http = "http://".to_string();
            addr_http.push_str(&elem);
            let client = KeeperWorkClient::connect(addr_http.to_string()).await;
            match client {
                Ok(mut value) => match value.get_leader(Empty {}).await {
                    Ok(value) => {
                        let check = value.into_inner().leader_id;
                        if check > 0 {
                            return check;
                        }
                    }
                    Err(_) => {
                        // the get leader returns error
                        ();
                    }
                },
                Err(_) => {
                    // it could not connect to this client, just ask next
                    ();
                }
            }
        }
        return -1;
    }

    async fn select_leader(&self) -> i64 {
        let mut leaders = Vec::new();
        for elem in &self.keepers {
            let mut addr_http = "http://".to_string();
            addr_http.push_str(&elem);
            let client = KeeperWorkClient::connect(addr_http.to_string()).await;
            match client {
                Ok(mut value) => match value.get_index(Empty {}).await {
                    Ok(value) => {
                        leaders.push(value.into_inner().index);
                    }
                    Err(_) => {}
                },
                Err(_e) => {
                    // it could not connect to this client, just ask next
                    // println!("the {} connect goes wrong", addr_http.to_string());
                    // println!("the reason is {:?}", e);
                    ();
                }
            }
        }
        // println!("current candicates are {:?}", &leaders);
        return *leaders.iter().min().unwrap();
    }

    async fn keeper_heart_beat(&self, id: i64, primary_id: i64) -> TribResult<Bool> {
        let mut addr_http = "http://".to_string();
        addr_http.push_str(self.keepers.get(primary_id as usize).unwrap());
        // println!(
        //     // "{} sends the hearbeat to {}",
        //     self.keepers.get(id as usize).unwrap(),
        //     primary_id
        // );
        let client = KeeperWorkClient::connect(addr_http.to_string()).await;
        match client {
            Ok(_) => Ok(Bool { value: true }),
            Err(e) => {
                return Err(Box::new(TribblerError::Unknown(e.to_string())));
            }
        }
    }

    // the heart beat to every backends
    // check a specific backend is still alive
    // async fn backend_heart_beat(&self, id: i64) -> TribResult<Bool> {
    //     todo!();
    // }

    // // from one replica to a new replica
    // async fn data_migration(&self, src_id: i64, dst_id: i64) -> TribResult<Bool> {
    //     todo!();
    // }

    // async fn client(&self, id: i64) -> TribResult<KeeperWorkClient<Channel>> {
    //     Ok(KeeperWorkClient::connect(self.keepers.get(id).clone()))
    // }
}
