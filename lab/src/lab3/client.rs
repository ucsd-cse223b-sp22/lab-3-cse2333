use crate::lab1::client::StorageClient;
use crate::lab3::wrapper::StorageClientWrapper;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use tribbler::err::TribResult;
use tribbler::storage::{BinStorage, Storage};
pub struct BinStorageClient {
    pub backs: Vec<String>,
}

// bin() which takes a bin name and returns a Storage
#[async_trait]
impl BinStorage for BinStorageClient {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        let mut hasher = DefaultHasher::new();
        hasher.write(name.as_bytes());
        let index = hasher.finish() as usize % self.backs.len();
        let addr = self.backs.get(index).unwrap();
        let mut tmp_addr = "http://".to_string();
        tmp_addr.push_str(addr);
        Ok(Box::new(StorageClientWrapper {
            name: name.to_string(),
            storage_client: StorageClient { addr: tmp_addr },
        }))
    }
}
