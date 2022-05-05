use crate::lab1::client::StorageClient;
use async_trait::async_trait;
use tribbler::colon::{escape, unescape};
use tribbler::err::TribResult;
use tribbler::storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage};

pub struct StorageClientWrapper {
    pub name: String,
    pub storage_client_primary: StorageClient,
    pub storage_client_backup: StorageClient,
}

#[async_trait]
impl KeyString for StorageClientWrapper {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        // todo: get value twice and compare??
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(key));
        return self.storage_client_primary.get(&key_name).await;
        // let res1 = self.storage_client_primary.get(&key_name).await;
    }

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        // todo: set value twice and compare??
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(kv.key.clone()));
        let res1 = self
            .storage_client_primary
            .set(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        let res2 = self
            .storage_client_backup
            .set(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        if res1 && res2 {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        // todo: get value twice and compare??
        let mut prefix = escape(self.name.to_string());
        prefix.push_str(&"::".to_string());
        prefix.push_str(&escape(p.prefix.to_string()));
        let suffix = escape(p.suffix.to_string());
        let all_keys = self
            .storage_client_primary
            .keys(&Pattern { prefix, suffix })
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
        // todo: get value twice and compare??
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(key));

        return self.storage_client_primary.list_get(&key_name).await;
    }

    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        // todo: append kv twice
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(kv.key.clone()));

        let res1 = self
            .storage_client_primary
            .list_append(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        let res2 = self
            .storage_client_backup
            .list_append(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        if res1 && res2 {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        // todo: remove kv twice
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(kv.key.clone()));

        let res1 = self
            .storage_client_primary
            .list_remove(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        let _res2 = self
            .storage_client_backup
            .list_remove(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        Ok(res1)
    }

    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        // todo: list kv twice
        let mut prefix = escape(self.name.to_string());
        prefix.push_str(&"::".to_string());
        prefix.push_str(&escape(p.prefix.to_string()));
        let suffix = escape(p.suffix.to_string());
        let all_keys = self
            .storage_client_primary
            .list_keys(&Pattern { prefix, suffix })
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
    // todo: return 2 values reparately !!!
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        return self.storage_client_primary.clock(at_least).await;
    }
}
