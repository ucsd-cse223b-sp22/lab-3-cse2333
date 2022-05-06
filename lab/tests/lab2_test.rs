use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::Duration, collections::{HashMap, HashSet}, cmp,
};
use rand::Rng;
use lab::{self, lab1, lab2};
use tokio::{sync::mpsc::Sender as MpscSender, time};
use tribbler::{config::KeeperConfig};
#[allow(unused_imports)]
use tribbler::{
    self,
    config::BackConfig,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, MemStorage, Pattern, Storage},
};

fn spawn_back(cfg: BackConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab1::serve_back(cfg))
}

fn spawn_keep(kfg: KeeperConfig) -> tokio::task::JoinHandle<TribResult<()>> {
    tokio::spawn(lab2::serve_keeper(kfg))
}

fn generate_random_string(len: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();

    let password: String = (0..len)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    return password;
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn pat(prefix: &str, suffix: &str) -> Pattern {
    Pattern {
        prefix: prefix.to_string(),
        suffix: suffix.to_string(),
    }
}

async fn setup(backs: Vec<String>, addrs:  Vec<String>) -> TribResult<(MpscSender<()>, 
 MpscSender<()>, MpscSender<()>, MpscSender<()>, MpscSender<()>, MpscSender<()>)> {
    let (shut_tx1, shut_rx1) = tokio::sync::mpsc::channel(1);
    let (shut_tx2, shut_rx2) = tokio::sync::mpsc::channel(1);
    let (shut_tx3, shut_rx3) = tokio::sync::mpsc::channel(1);
    let (shut_tx4, shut_rx4) = tokio::sync::mpsc::channel(1);
    let (shut_tx5, shut_rx5) = tokio::sync::mpsc::channel(1);
    let (shut_tx6, shut_rx6) = tokio::sync::mpsc::channel(1);
    let cfg1 = BackConfig {
        addr: backs[0].to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx1),
    };
    let cfg2 = BackConfig {
        addr: backs[1].to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx2),
    };
    let cfg3 = BackConfig {
        addr: backs[2].to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx3),
    };
    let cfg4 = BackConfig {
        addr: backs[3].to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx4),
    };
    let cfg5 = BackConfig {
        addr: backs[4].to_string(),
        storage: Box::new(MemStorage::default()),
        ready: None,
        shutdown: Some(shut_rx5),
    };
    let kfg1 = KeeperConfig {
        backs: backs.clone(),
        addrs: addrs.clone(),
        this: 0,
        id: 0,
        ready: None,
        shutdown: Some(shut_rx6),
    };
    spawn_back(cfg1);
    spawn_back(cfg2);
    spawn_back(cfg3);
    // spawn_back(cfg4);
    spawn_back(cfg5);
    spawn_keep(kfg1);
    time::sleep(Duration::from_millis(777)).await;
    Ok((shut_tx1.clone(), shut_tx2.clone(), shut_tx3.clone(), shut_tx4.clone(), shut_tx5.clone(),shut_tx6.clone(), ))
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
// async fn test_node_leave() -> TribResult<()> {
//     let backs = vec![
//         "127.0.0.1:33950".to_string(),
//         "127.0.0.1:33951".to_string(),
//         "127.0.0.1:33952".to_string(),
//         "127.0.0.1:33953".to_string(),
//         "127.0.0.1:33954".to_string(),
//     ];
//     let addrs = vec![
//         "127.0.0.1:33955".to_string(),
//     ];

//     let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), addrs.clone()).await?;
//     // let x = setup(backs.clone(), addrs.clone()).await?;

//     let bc = lab2::new_bin_client(backs.clone()).await?;
//     let backend0 = bc.bin("one").await?;
//     let backend1 = bc.bin("2222").await?;
//     let backend2 = bc.bin("thre").await?;
//     let backend3 = bc.bin("four").await?;
//     let backend4 = bc.bin("hello").await?;
    
//     backend0.set(&kv("one", "one")).await?;
//     backend1.set(&kv("2222", "2222")).await?;
//     backend2.set(&kv("thre", "thre")).await?;
//     backend3.set(&kv("four", "four")).await?;
//     backend4.set(&kv("hello", "hello")).await?;
//     time::sleep(time::Duration::from_secs(2)).await;
//     let _ = tx2.send(()).await; // shutdown backend 0

//     time::sleep(time::Duration::from_secs(20)).await;

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
// async fn test_node_join() -> TribResult<()> {
//     let backs = vec![
//         "127.0.0.1:33950".to_string(),
//         "127.0.0.1:33951".to_string(),
//         "127.0.0.1:33952".to_string(),
//         "127.0.0.1:33953".to_string(),
//         "127.0.0.1:33954".to_string(),
//     ];
//     let addrs = vec![
//         "127.0.0.1:33955".to_string(),
//     ];

//     let (tx1, tx2, tx3, tx4, tx5, tx6) = setup(backs.clone(), addrs.clone()).await?;
//     // let x = setup(backs.clone(), addrs.clone()).await?;

//     let bc = lab2::new_bin_client(backs.clone()).await?;
//     let backend0 = bc.bin("one").await?;
//     let backend1 = bc.bin("2222").await?;
//     let backend2 = bc.bin("thre").await?;
//     let backend3 = bc.bin("four").await?;
//     let backend4 = bc.bin("hello").await?;
    
//     backend0.set(&kv("one", "one")).await?;
//     backend1.set(&kv("2222", "2222")).await?;
//     backend2.set(&kv("thre", "thre")).await?;
//     backend3.set(&kv("four", "four")).await?;
//     backend4.set(&kv("hello", "hello")).await?;
//     time::sleep(time::Duration::from_secs(2)).await;
    
//     time::sleep(time::Duration::from_secs(2)).await;
//     // let _ = tx2.send(()).await; // shutdown backend 0

//     // time::sleep(time::Duration::from_secs(20)).await;
//     // let (shut_tx4, shut_rx4) = tokio::sync::mpsc::channel(1);
//     // let cfg44 = BackConfig {
//     //     addr: backs[3].to_string(),
//     //     storage: Box::new(MemStorage::default()),
//     //     ready: None,
//     //     shutdown: Some(shut_rx4),
//     // };
//     // println!("try to start back 3");
//     // let x = spawn_back(cfg44);



//     time::sleep(time::Duration::from_secs(10)).await;

//     Ok(())
// }


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_follow() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33950".to_string(),
        "127.0.0.1:33951".to_string(),
        "127.0.0.1:33952".to_string(),
        "127.0.0.1:33953".to_string(),
        "127.0.0.1:33954".to_string(),
    ];
    let addrs = vec![
        "127.0.0.1:33955".to_string(),
        "127.0.0.1:33956".to_string(),
        "127.0.0.1:33957".to_string(),
    ];
    let (tx1, tx2, tx3, tx4,
        tx5, tx6,) = setup(backs.clone(), addrs.clone()).await?;
        let bc = lab2::new_bin_client(backs.clone()).await?;
        let backend0 = bc.bin("one").await?;
        let backend1 = bc.bin("2222").await?;
        let backend2 = bc.bin("thre").await?;
        let backend3 = bc.bin("four").await?;
        let backend4 = bc.bin("hello").await?;
        
        backend0.set(&kv("one", "one")).await?;
        backend1.set(&kv("2222", "2222")).await?;
        backend2.set(&kv("thre", "thre")).await?;
        backend3.set(&kv("four", "four")).await?;
        backend4.set(&kv("hello", "hello")).await?;
    
    time::sleep(time::Duration::from_secs(20)).await;
    let _ = tx1.send(()).await;
    let _ = tx2.send(()).await;
    let _ = tx3.send(()).await;
    let _ = tx4.send(()).await;
    let _ = tx5.send(()).await;
    let _ = tx6.send(()).await;
    Ok(())
}


// cargo test -p lab --test lab2_test -- --nocapture