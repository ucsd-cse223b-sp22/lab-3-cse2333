use async_trait::async_trait;
use tribbler::err::TribResult;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::rpc::{Clock, Key, KeyValue as rpcKeyValue, Pattern as rpcPattern};
use tribbler::storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage};

pub struct StorageClient {
    pub addr: String,
    // pub clock: RwLock<u64>,
}

#[async_trait]
impl KeyString for StorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .get(Key {
                key: key.to_string(),
            })
            .await;
        match r {
            Ok(value) => Ok(Some(value.into_inner().value)),
            Err(e) => Ok(None),
        }
    }

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .set(rpcKeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;

        match r.into_inner().value {
            value => Ok(value),
        }
    }

    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .keys(rpcPattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?;

        match r.into_inner().list {
            value => Ok(List(value)),
        }
    }
}

#[async_trait]
impl KeyList for StorageClient {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_get(Key {
                key: key.to_string(),
            })
            .await?;

        match r.into_inner().list {
            value => Ok(List(value)),
        }
    }

    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_append(rpcKeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;

        match r.into_inner().value {
            value => Ok(value),
        }
    }

    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_remove(rpcKeyValue {
                key: kv.key.to_string(),
                value: kv.value.to_string(),
            })
            .await?;

        match r.into_inner().removed {
            value => Ok(value),
        }
    }

    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_keys(rpcPattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?;

        match r.into_inner().list {
            value => Ok(List(value)),
        }
    }
}

#[async_trait]
impl Storage for StorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .clock(Clock {
                timestamp: at_least,
            })
            .await?;

        match r.into_inner().timestamp {
            value => Ok(value),
        }
    }
}
