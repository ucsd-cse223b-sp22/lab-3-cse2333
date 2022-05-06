use crate::lab1::client::StorageClient;
use crate::lab2::wrapper::StorageClientWrapper;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use tokio::time::Instant;
use tribbler::err::TribResult;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::rpc::Key;
use tribbler::storage::{BinStorage, Storage};

use super::utils::StatusTableEntry;

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
        let entry = handle.await.unwrap();
        status_table_multi.push(entry);
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
        Err(_) => StatusTableEntry {
            addr: addr.clone(),
            status: false,
        },
    }
}

// pub async fn update_table(
//     prev_table: Vec<StatusTableEntry>,
//     backs: Vec<String>,
// ) -> Vec<StatusTableEntry> {
//     // prev_table = scan_server(backs).await;
//     let mut hasher = DefaultHasher::new();
//     hasher.write("BackendStatus".as_bytes());
//     let mut backend_hash = hasher.finish() as usize % backs.len();
//     while !prev_table[backend_hash].status {
//         backend_hash = (backend_hash + 1) % backs.len();
//     }

//     let mut status_addr = "http://".to_string();
//     status_addr.push_str(&backs[backend_hash].clone());
//     let mut status_client = match TribStorageClient::connect(status_addr.to_string()).await {
//         Ok(client) => client,
//         Err(e) => {
//             prev_table[backend_hash].status = false;
//             while !prev_table[backend_hash].status {
//                 backend_hash = (backend_hash + 1) % backs.len();
//             }
//             let mut status_addr_temp = "http://".to_string();
//             status_addr_temp.push_str(&backs[backend_hash].clone());
//             match TribStorageClient::connect(status_addr_temp.to_string()).await {
//                 Ok(c) => c,
//                 Err(_) => {
//                     println!(
//                         "!!!unexpected error{:?}, this backend should not be crashed!!!",
//                         e
//                     );
//                     return ();
//                 }
//             }
//         }
//     };
//     //check connection
//     let res_table = match status_client
//         .get(Key {
//             key: "BackendStatus".to_string(),
//         })
//         .await
//     {
//         Ok(value) => {
//             serde_json::from_str::<Vec<StatusTableEntry>>(&value.into_inner().value).unwrap();
//             println!("here I updated the table successfully!!!");
//         }
//         Err(e) => {
//             println!(
//                 "unexpected error{:?}, this backend should have the table!!!",
//                 e
//             );
//         }
//     };
//     return res_table;
// }
pub struct BinStorageClient {
    pub backs: Vec<String>,
    pub status_table: Vec<StatusTableEntry>,
}
// bin() which takes a bin name and returns a Storage
#[async_trait]
impl BinStorage for BinStorageClient {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        let mut hasher = DefaultHasher::new();
        hasher.write(name.as_bytes());
        let index = hasher.finish() as usize % self.backs.len();

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
