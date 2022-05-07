use crate::lab1::client::StorageClient;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Instant;
use std::{cmp::min, cmp::Ordering};
use tokio::sync::Mutex;
use tokio::time::error::Elapsed;
use tribbler::colon::{escape, unescape};
use tribbler::err::TribResult;
use tribbler::storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage};

use super::client::{hash_name_ip, scan_server};
use super::utils::StatusTableEntry;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct LogEntry {
    message: String,
    clock: u64,
}
struct OrderLogEntry {
    logentry: Arc<LogEntry>,
}
impl Ord for OrderLogEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        let res = self.logentry.clock.cmp(&other.logentry.clock);
        match res {
            Ordering::Equal => (),
            _ => {
                return res;
            }
        };
        return self.logentry.message.cmp(&other.logentry.message);
    }
}

impl Eq for OrderLogEntry {}

impl PartialOrd for OrderLogEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let res = self.logentry.clock.partial_cmp(&other.logentry.clock);
        match res {
            Some(Ordering::Equal) => (),
            Some(value) => {
                return Some(value);
            }
            None => {
                return None;
            }
        };
        return self.logentry.message.partial_cmp(&other.logentry.message);
    }
}

impl PartialEq for OrderLogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.logentry.clock == other.logentry.clock
            && self.logentry.message == other.logentry.message
    }
}
pub struct StorageClientWrapper {
    pub backs: Vec<String>,
    pub status_table: Arc<Mutex<Vec<StatusTableEntry>>>,
    pub name: String,
    // pub storage_client_primary: StorageClient,
    // pub storage_client_backup: StorageClient,
    pub timestamp: Arc<Mutex<Instant>>,
    pub addr_primary: Arc<Mutex<String>>,
    pub addr_backup: Arc<Mutex<String>>,
}
impl StorageClientWrapper {
    pub async fn update_table(&self) -> (StorageClient, StorageClient) {
        // let time = Instant::now();
        let mut e = self.timestamp.lock().await;
        let elapsed = e.elapsed().as_secs();
        if elapsed >= 5 {
            let table = scan_server(self.backs.clone()).await;
            *e = Instant::now();
            let (tmp_addr_primary, tmp_addr_backup) = hash_name_ip(&self.name, table.clone()).await;
            let mut status_table_lock = self.status_table.lock().await;
            *status_table_lock = table;
            drop(status_table_lock);
            let mut addr_primary_lock = self.addr_primary.lock().await;
            *addr_primary_lock = tmp_addr_primary;
            drop(addr_primary_lock);
            let mut addr_backup_lock = self.addr_backup.lock().await;
            *addr_backup_lock = tmp_addr_backup;
            drop(addr_backup_lock);
        }
        drop(e);
        let addr_primary_lock = &self.addr_primary.lock().await;
        let storage_client_primary = StorageClient {
            addr: (*addr_primary_lock).to_string(),
        };
        drop(addr_primary_lock);
        let addr_backup_lock = self.addr_backup.lock().await;
        let storage_client_backup = StorageClient {
            addr: (*addr_backup_lock).to_string(),
        };
        drop(addr_backup_lock);
        (storage_client_primary, storage_client_backup)
    }
}
#[async_trait]
impl KeyString for StorageClientWrapper {
    // todo: catch err and update table=> no need to do so, as long as migrate within 10s, the data is safe
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        // todo: get value twice and compare??return the newest one!
        // if backend crash, just return None instead of error
        let (storage_client_primary, storage_client_backup) = self.update_table().await;
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(key));
        let res1 = storage_client_primary.get(&key_name).await?;
        let res2 = storage_client_backup.get(&key_name).await?;
        let time1 = storage_client_primary.clock(0).await?;
        let time2 = storage_client_backup.clock(0).await?;
        if time1 > time2 {
            Ok(res1)
        } else {
            Ok(res2)
        }
    }

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        // todo: set value twice and compare?? deal with error??
        let (storage_client_primary, storage_client_backup) = self.update_table().await;

        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(kv.key.clone()));
        let res1 = storage_client_primary
            .set(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        let res2 = storage_client_backup
            .set(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        if res1 || res2 {
            // at least one replica??
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        // todo: get value twice and merge!!!
        let (storage_client_primary, storage_client_backup) = self.update_table().await;

        let mut prefix = escape(self.name.to_string());
        prefix.push_str(&"::".to_string());
        prefix.push_str(&escape(p.prefix.to_string()));
        let suffix = escape(p.suffix.to_string());
        let all_keys_primary = storage_client_primary
            .keys(&Pattern {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            })
            .await?
            .0;
        let all_keys_backup = storage_client_backup
            .keys(&Pattern {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            })
            .await?
            .0;
        let all_keys_primary_set: HashSet<String> = HashSet::from_iter(all_keys_primary);
        let all_keys_backup_set: HashSet<String> = HashSet::from_iter(all_keys_backup);

        // let union_set = all_keys_primary_set
        //     .union(&all_keys_backup_set)
        //     .collect::<HashSet<&String>>();
        // let all_keys: Vec<&String> = Vec::from_iter(union_set);
        let mut all_keys_set = HashSet::new();
        if all_keys_primary_set.len() >= all_keys_backup_set.len() {
            all_keys_set = all_keys_primary_set;
        } else {
            all_keys_set = all_keys_backup_set;
        }
        let all_keys: Vec<String> = Vec::from_iter(all_keys_set);
        let mut all_keys_unescaped = Vec::new();
        all_keys.iter().into_iter().for_each(|key| {
            let tmp: Vec<String> = key.split("::").map(|x| x.to_string()).collect();
            let key_name = unescape(tmp.get(1).unwrap()).to_string();
            all_keys_unescaped.push(key_name.clone());
        });
        Ok(List(all_keys_unescaped))
    }
}

#[async_trait]
impl KeyList for StorageClientWrapper {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        // todo: get value twice and compare??
        let (storage_client_primary, storage_client_backup) = self.update_table().await;

        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(key));

        let res_primary = storage_client_primary
            .list_get(&key_name.to_string())
            .await?
            .0;
        let res_backup = storage_client_backup
            .list_get(&key_name.to_string())
            .await?
            .0;
        let res_primary_set: HashSet<String> = HashSet::from_iter(res_primary);
        let res_backup_set: HashSet<String> = HashSet::from_iter(res_backup);
        let mut all_res_set = HashSet::new();
        if res_primary_set.len() >= res_backup_set.len() {
            all_res_set = res_primary_set;
        } else {
            all_res_set = res_backup_set;
        }
        let all_res: Vec<String> = Vec::from_iter(all_res_set);
        let mut res = all_res
            .to_vec()
            .iter()
            .map(|x| OrderLogEntry {
                logentry: Arc::new(serde_json::from_str::<LogEntry>(x).unwrap()).clone(),
            })
            .collect::<Vec<OrderLogEntry>>();
        res.sort();
        let res0 = res
            .iter()
            .map(|x| x.logentry.message.clone())
            .collect::<Vec<String>>();
        return Ok(List(res0));
    }

    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        // todo: append kv twice
        let (storage_client_primary, storage_client_backup) = self.update_table().await;

        let clock1 = storage_client_primary.clock(0).await?;
        let clock2 = storage_client_backup.clock(0).await?;
        let c = match clock1 > clock2 {
            true => clock1,
            false => clock2,
        };
        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(kv.key.clone()));
        let log_entry = serde_json::to_string(&LogEntry {
            message: kv.value.to_string(),
            clock: c,
        })
        .unwrap();

        let res1 = storage_client_primary
            .list_append(&KeyValue {
                key: key_name.to_string(),
                value: log_entry.to_string(),
            })
            .await?;
        let res2 = storage_client_backup
            .list_append(&KeyValue {
                key: key_name.to_string(),
                value: log_entry.to_string(),
            })
            .await?;
        if res1 || res2 {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        // todo: remove kv twice
        let (storage_client_primary, storage_client_backup) = self.update_table().await;

        let mut key_name = escape(self.name.clone()).to_string();
        key_name.push_str(&"::".to_string());
        key_name.push_str(&escape(kv.key.clone()));

        let res1 = storage_client_primary
            .list_remove(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        let _res2 = storage_client_backup
            .list_remove(&KeyValue {
                key: key_name.to_string(),
                value: kv.value.to_string(),
            })
            .await?;
        Ok(res1)
    }

    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        // todo: list kv twice
        let (storage_client_primary, storage_client_backup) = self.update_table().await;

        let mut prefix = escape(self.name.to_string());
        prefix.push_str(&"::".to_string());
        prefix.push_str(&escape(p.prefix.to_string()));
        let suffix = escape(p.suffix.to_string());
        let all_keys_primary = storage_client_primary
            .list_keys(&Pattern {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            })
            .await?
            .0;
        let all_keys_backup = storage_client_backup
            .list_keys(&Pattern {
                prefix: prefix.to_string(),
                suffix: suffix.to_string(),
            })
            .await?
            .0;
        let all_keys_primary_set: HashSet<String> = HashSet::from_iter(all_keys_primary);
        let all_keys_backup_set: HashSet<String> = HashSet::from_iter(all_keys_backup);
        let mut all_keys_set = HashSet::new();
        if all_keys_primary_set.len() >= all_keys_backup_set.len() {
            all_keys_set = all_keys_primary_set;
        } else {
            all_keys_set = all_keys_backup_set;
        }
        let all_keys: Vec<String> = Vec::from_iter(all_keys_set);
        let mut all_keys_unescaped = Vec::new();
        all_keys.iter().into_iter().for_each(|key| {
            let tmp: Vec<String> = key.split("::").map(|x| x.to_string()).collect();
            let key_name = unescape(tmp.get(1).unwrap()).to_string();
            all_keys_unescaped.push(key_name.clone());
        });
        Ok(List(all_keys_unescaped))
    }
}

#[async_trait]
impl Storage for StorageClientWrapper {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let (storage_client_primary, storage_client_backup) = self.update_table().await;
        let res1 = storage_client_primary.clock(at_least).await?;
        let res2 = storage_client_backup.clock(at_least).await?;
        if res1 > res2 {
            let _ = storage_client_backup.clock(res1).await;
            Ok(res1)
        } else {
            if res1 < res2 {
                let _ = storage_client_primary.clock(res2).await;
                Ok(res2)
            } else {
                Ok(res1)
            }
        }
    }
}
