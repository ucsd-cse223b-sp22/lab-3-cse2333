use crate::lab1::client::StorageClient;
use crate::lab2::wrapper::StorageClientWrapper;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use tokio::time::Instant;
use tribbler::err::TribResult;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::storage::{BinStorage, Storage};

pub async fn scan_server(backs: Vec<String>) -> Vec<StatusTableEntry> {
    // scan all servers and establish the table

    // // single process
    // let timer1 = Instant::now();
    // let mut status_table: Vec<StatusTableEntry> = Vec::new();
    // for i in backs.iter() {
    //     let mut addr_http = "http://".to_string();
    //     addr_http.push_str(i);
    //     let client = TribStorageClient::connect(addr_http.clone()).await;
    //     match client {
    //         Ok(_) => status_table.push(StatusTableEntry {
    //             addr: i.clone(),
    //             status: true,
    //         }),
    //         Err(_) => status_table.push(StatusTableEntry {
    //             addr: i.clone(),
    //             status: false,
    //         }),
    //     }
    // }
    // // println!("====single-processor===={:?}", status_table);
    // println!(
    //     "single process timing:{:?} ms",
    //     timer1.elapsed().as_millis()
    // );
    // // return status_table;

    // multi-processor
    // todo: check order!!!!
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
#[derive(Debug)]
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
