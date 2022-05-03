use async_trait::async_trait;
use tonic::{Request, Response, Status};
use tribbler::rpc::trib_storage_server::TribStorage;
use tribbler::rpc::{
    Bool, Clock, Key, KeyValue as rpcKeyValue, ListRemoveResponse, Pattern as rpcPattern,
    StringList, Value,
};
use tribbler::storage::{KeyValue, Pattern, Storage};

pub struct StorageServer {
    pub storage: Box<dyn Storage>,
}

#[async_trait]
impl TribStorage for StorageServer {
    async fn get(&self, request: Request<Key>) -> Result<Response<Value>, Status> {
        let result = self.storage.get(&request.into_inner().key).await;
        match result {
            Ok(value) => match value {
                Some(v) => Ok(Response::new(Value { value: v })),
                None => Err(Status::invalid_argument("No key provided")),
            },
            Err(e) => Err(Status::invalid_argument("Server get() failed")),
        }
    }

    async fn set(&self, request: Request<rpcKeyValue>) -> Result<Response<Bool>, Status> {
        let request_inner = request.into_inner();
        let result = self
            .storage
            .set(&KeyValue {
                key: request_inner.key,
                value: request_inner.value,
            })
            .await;
        match result {
            Ok(value) => Ok(Response::new(Bool { value: value })),
            Err(e) => Err(Status::invalid_argument("Server set() failed")),
        }
    }

    async fn keys(&self, request: Request<rpcPattern>) -> Result<Response<StringList>, Status> {
        let request_inner = request.into_inner();
        let result = self
            .storage
            .keys(&Pattern {
                prefix: request_inner.prefix,
                suffix: request_inner.suffix,
            })
            .await;
        match result {
            Ok(value) => Ok(Response::new(StringList { list: value.0 })),
            Err(e) => Err(Status::invalid_argument("Server keys() failed")),
        }
    }

    async fn list_get(&self, request: Request<Key>) -> Result<Response<StringList>, Status> {
        let result = self.storage.list_get(&request.into_inner().key).await;
        match result {
            Ok(value) => Ok(Response::new(StringList { list: value.0 })),
            Err(e) => Err(Status::invalid_argument("Server list_get() failed")),
        }
    }

    async fn list_append(&self, request: Request<rpcKeyValue>) -> Result<Response<Bool>, Status> {
        let request_inner = request.into_inner();
        let result = self
            .storage
            .list_append(&KeyValue {
                key: request_inner.key,
                value: request_inner.value,
            })
            .await;

        // let result2 = self
        //     .storage
        //     .list_keys(&Pattern {
        //         prefix: "".to_string(),
        //         suffix: "".to_string(),
        //     })
        //     .await;
        // dbg!(result2);

        match result {
            Ok(value) => Ok(Response::new(Bool { value: value })),
            Err(e) => Err(Status::invalid_argument("Server list_append() failed")),
        }
    }

    async fn list_remove(
        &self,
        request: Request<rpcKeyValue>,
    ) -> Result<Response<ListRemoveResponse>, Status> {
        let request_inner = request.into_inner();
        let result = self
            .storage
            .list_remove(&KeyValue {
                key: request_inner.key,
                value: request_inner.value,
            })
            .await;
        match result {
            Ok(value) => Ok(Response::new(ListRemoveResponse { removed: value })),
            Err(e) => Err(Status::invalid_argument("Server list_remove() failed")),
        }
    }

    async fn list_keys(
        &self,
        request: Request<rpcPattern>,
    ) -> Result<Response<StringList>, Status> {
        let request_inner = request.into_inner();
        let result = self
            .storage
            .list_keys(&Pattern {
                prefix: request_inner.prefix,
                suffix: request_inner.suffix,
            })
            .await;
        match result {
            Ok(value) => Ok(Response::new(StringList { list: value.0 })),
            Err(e) => Err(Status::invalid_argument("server list_keys() failed")),
        }
    }

    async fn clock(&self, request: Request<Clock>) -> Result<Response<Clock>, Status> {
        let result = self.storage.clock(request.into_inner().timestamp).await;
        match result {
            Ok(value) => Ok(Response::new(Clock { timestamp: value })),
            Err(e) => Err(Status::invalid_argument("server clock() failed")),
        }
    }
}
