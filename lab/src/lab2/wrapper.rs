use crate::lab1::client::StorageClient;
use async_trait::async_trait;
use tribbler::colon::{escape, unescape};
use tribbler::err::TribResult;
use tribbler::storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage};

pub struct StorageClientWrapper {
    pub name: String,
    pub storage_client: StorageClient,
}

#[async_trait]
impl KeyString for StorageClientWrapper {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(key));
        return self.storage_client.get(&key_name).await;
    }

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(kv.key.clone()));
        return self
            .storage_client
            .set(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await;
    }

    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let all_keys = self
            .storage_client
            .keys(&Pattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?
            .0;

        let mut all_keys_unescaped = Vec::new();
        for key in all_keys {
            let tmp: Vec<String> = key.split("::").map(|x| x.to_string()).collect();
            let key_name = unescape(tmp.get(1).unwrap()).to_string();
            all_keys_unescaped.push(key_name.clone());
        }
        Ok(List(all_keys_unescaped))
    }
}

#[async_trait]
impl KeyList for StorageClientWrapper {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(key));

        return self.storage_client.list_get(&key_name).await;
    }

    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(kv.key.clone()));

        return self
            .storage_client
            .list_append(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await;
    }

    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(kv.key.clone()));

        return self
            .storage_client
            .list_remove(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await;
    }

    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let all_keys = self
            .storage_client
            .list_keys(&Pattern {
                prefix: p.prefix.to_string(),
                suffix: p.suffix.to_string(),
            })
            .await?
            .0;

        let mut all_keys_unescaped = Vec::new();
        for key in all_keys {
            let tmp: Vec<String> = key.split("::").map(|x| x.to_string()).collect();
            let key_name = unescape(tmp.get(1).unwrap()).to_string();
            all_keys_unescaped.push(key_name);
        }
        Ok(List(all_keys_unescaped))
    }
}

#[async_trait]
impl Storage for StorageClientWrapper {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        return self.storage_client.clock(at_least).await;
    }
}
