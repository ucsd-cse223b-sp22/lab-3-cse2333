use crate::lab1::client::StorageClient;
use crate::lab2::wrapper::StorageClientWrapper;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use tribbler::err::TribResult;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::storage::{BinStorage, Storage};

pub async fn scan_server(backs: Vec<String>) -> Vec<StatusTableEntry> {
    // scan all servers and establish the table
    let mut status_table: Vec<StatusTableEntry> = Vec::new();
    for i in backs.iter() {
        let mut addr_http = "http://".to_string();
        addr_http.push_str(i);
        let client = TribStorageClient::connect(addr_http.clone()).await;
        match client {
            Ok(_) => status_table.push(StatusTableEntry {
                addr: i.clone(),
                status: true,
            }),
            Err(_) => status_table.push(StatusTableEntry {
                addr: i.clone(),
                status: false,
            }),
        }
    }
    return status_table;
}
pub struct StatusTableEntry {
    pub addr: String,
    pub status: bool,
}
pub struct BinStorageClient {
    pub backs: Vec<String>,
    pub status_table: Vec<StatusTableEntry>,
}
impl BinStorageClient {
    async fn update_table(mut self) {
        self.status_table = scan_server(self.backs).await;
    }
    // async fn get_table(mut self){
    //     todo!()
    // }
}
// bin() which takes a bin name and returns a Storage
#[async_trait]
impl BinStorage for BinStorageClient {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        let mut hasher = DefaultHasher::new();
        hasher.write(name.as_bytes());
        let index = hasher.finish() as usize % self.backs.len();
        // let addr = self.backs.get(index).unwrap();
        // let mut tmp_addr = "http://".to_string();
        // tmp_addr.push_str(addr);

        // choose 2 ip
        let mut tmp_addr_primary = "http://".to_string();
        let mut tmp_addr_backup = "http://".to_string();
        let mut cnt = 0;
        let mut curridx = index;
        let table = &self.status_table;
        while cnt < 2 {
            if table[curridx].status == true {
                if cnt < 1 {
                    tmp_addr_primary.push_str(&table[curridx].addr);
                } else {
                    tmp_addr_backup.push_str(&table[curridx].addr);
                }
                cnt += 1;
            }
            curridx = (curridx + 1) % self.backs.len();
        }
        Ok(Box::new(StorageClientWrapper {
            name: name.to_string(),
            storage_client_primary: StorageClient {
                addr: tmp_addr_primary,
            },
            storage_client_backup: StorageClient {
                addr: tmp_addr_backup,
            },
        }))
    }
}
