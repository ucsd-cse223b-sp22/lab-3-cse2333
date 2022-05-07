use super::utils::StatusTableEntry;
use crate::lab1::client::StorageClient;
use crate::lab2::wrapper::StorageClientWrapper;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tribbler::err::TribResult;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::rpc::Key;
use tribbler::storage::{BinStorage, Storage};

pub async fn scan_server(backs: Vec<String>) -> Vec<StatusTableEntry> {
    // scan all servers and establish the table
    // multi-processor
    let timer2 = Instant::now();
    let mut handles = Vec::with_capacity(backs.len());
    let mut status_table_multi: Vec<StatusTableEntry> = Vec::new();
    for i in backs.iter() {
        handles.push(tokio::spawn(scan_single_server(i.to_string())));
    }
    for handle in handles {
        let entry = handle.await;
        match entry {
            Ok(v) => {
                status_table_multi.push(v);
            }
            Err(e) => {
                println!("error when unwrap!!!!{}", e);
            }
        }
    }
    // println!("====muitl-processor===={:?}", status_table_multi);
    println!("multi process timing:{:?} ms", timer2.elapsed().as_millis());
    return status_table_multi;
}
pub async fn scan_single_server(addr: String) -> StatusTableEntry {
    let mut addr_http = "http://".to_string();
    addr_http.push_str(&addr);
    let client = TribStorageClient::connect(addr_http.clone()).await;
    match client {
        Ok(_) => StatusTableEntry {
            addr: addr.clone(),
            status: true,
        },
        Err(_) => {
            println!("some server dies here!!! {}", addr);
            StatusTableEntry {
                addr: addr.clone(),
                status: false,
            }
        }
    }
}
pub async fn hash_name_ip(name: &str, table: Vec<StatusTableEntry>) -> (String, String) {
    let mut hasher = DefaultHasher::new();
    hasher.write(name.as_bytes());
    let index = hasher.finish() as usize % table.len();

    // choose 2 ip
    let mut tmp_addr_primary = "http://".to_string();
    let mut tmp_addr_backup = "http://".to_string();
    let mut cnt = 0;
    let mut curridx = index;
    while cnt < 2 {
        if table[curridx].status == true {
            if cnt == 0 {
                tmp_addr_primary.push_str(&table[curridx].addr);
            } else {
                tmp_addr_backup.push_str(&table[curridx].addr);
            }
            cnt += 1;
        }
        curridx = (curridx + 1) % table.len();
    }
    (tmp_addr_primary, tmp_addr_backup)
}
pub struct BinStorageClient {
    pub backs: Vec<String>,
}
// bin() which takes a bin name and returns a Storage
#[async_trait]
impl BinStorage for BinStorageClient {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        let time = Instant::now();
        let table = scan_server(self.backs.clone()).await;
        let (tmp_addr_primary, tmp_addr_backup) = hash_name_ip(name, table.clone()).await;
        Ok(Box::new(StorageClientWrapper {
            backs: self.backs.clone(),
            status_table: Arc::new(Mutex::new(table.clone())),
            name: name.to_string(),
            timestamp: Arc::new(Mutex::new(time)),
            addr_primary: Arc::new(Mutex::new(tmp_addr_primary)),
            addr_backup: Arc::new(Mutex::new(tmp_addr_backup)),
        }))
    }
}
