use crate::lab1::client::StorageClient;
use async_trait::async_trait;
use std::collections::HashSet;
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
    //todo: catch err and update table
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        // todo: get value twice and compare??return combination
        // if backend crash, just return None instead of error
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(key));
        // return self.storage_client_primary.get(&key_name).await;
        let res1 = self.storage_client_primary.get(&key_name).await;
        // let res2 = self.storage_client_backup.get(&key_name).await;
        // let res = match res1{
        //     Some(v1)=>{
        //         match res2{
        //             Some(v2)=>{
        //                 if v1==v2{
        //                     v1
        //                 }else{
        //                     //compare clock and choose bigger one
        //                     v2
        //                 }
        //             }
        //             None=>v1,
        //         }
        //     },
        // }
        return res1;
    }

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        // todo: set value twice and compare?? deal with error??
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
        // todo: get value twice and merge!!!
        let mut prefix = escape(self.name.to_string());
        prefix.push_str(&"::".to_string());
        prefix.push_str(&escape(p.prefix.to_string()));
        let suffix = escape(p.suffix.to_string());
        let all_keys_primary = self
            .storage_client_primary
            .keys(&Pattern {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            })
            .await?
            .0;
        let all_keys_backup = self
            .storage_client_backup
            .keys(&Pattern {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            })
            .await?
            .0;
        // let mut all_keys_primary_set: HashSet<String> = all_keys_primary.into_iter().collect();
        // let mut all_keys_backup_set: HashSet<String> = all_keys_backup.into_iter().collect();
        // let all_keys = all_keys_primary_set.union(&all_keys_backup_set).collect();
        let mut all_keys = Vec::new();
        if all_keys_primary == all_keys_backup {
            all_keys = all_keys_primary;
        } else {
            if all_keys_primary.len() > all_keys_backup.len() {
                //todo: compare by length for now, need to be fixed
                all_keys = all_keys_primary;
            } else {
                all_keys = all_keys_backup;
            }
        }
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

        let res_primary = self.storage_client_primary.list_get(&key_name).await?;
        let res_backup = self
            .storage_client_backup
            .list_get(&key_name.to_string())
            .await?;
        let mut res = Vec::new();
        if res_primary.0 == res_backup.0 {
            res = res_primary.0;
        } else {
            if res_primary.0.len() > res_backup.0.len() {
                //todo: compare by length for now, need to be fixed
                res = res_primary.0;
            } else {
                res = res_backup.0;
            }
        }
        Ok(List(res))
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
        let all_keys_primary = self
            .storage_client_primary
            .list_keys(&Pattern {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            })
            .await?
            .0;
        let all_keys_backup = self
            .storage_client_backup
            .list_keys(&Pattern {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            })
            .await?
            .0;
        let mut all_keys = Vec::new();
        if all_keys_primary == all_keys_backup {
            all_keys = all_keys_primary;
        } else {
            if all_keys_primary.len() > all_keys_backup.len() {
                //todo: compare by length for now, need to be fixed
                all_keys = all_keys_primary;
            } else {
                all_keys = all_keys_backup;
            }
        }
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
        let res1 = self.storage_client_primary.clock(at_least).await?;
        let res2 = self.storage_client_backup.clock(at_least).await?;
        if res1 > res2 {
            let _ = self.storage_client_backup.clock(res1).await;
            Ok(res1)
        } else {
            if res1 < res2 {
                let _ = self.storage_client_primary.clock(res2).await;
                Ok(res2)
            } else {
                Ok(res1)
            }
        }
    }
}
