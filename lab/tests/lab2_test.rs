use std::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    collections::{HashMap, HashSet}, time::Duration,
};
use tokio:: time;
use rand::Rng;
use lab::{self, lab1, lab2};
use tokio::{sync::mpsc::Sender as MpscSender};
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

async fn setup(backs: Vec<String>, addrs:  Vec<String>) -> TribResult<(MpscSender<()>, MpscSender<()>, MpscSender<()>, MpscSender<()>,
 MpscSender<()>, MpscSender<()>, MpscSender<()>)> {
    let (shut_tx1, shut_rx1) = tokio::sync::mpsc::channel(1);
    let (shut_tx2, shut_rx2) = tokio::sync::mpsc::channel(1);
    let (shut_tx3, shut_rx3) = tokio::sync::mpsc::channel(1);
    let (shut_tx4, shut_rx4) = tokio::sync::mpsc::channel(1);
    let (shut_tx5, shut_rx5) = tokio::sync::mpsc::channel(1);
    let (shut_tx6, shut_rx6) = tokio::sync::mpsc::channel(1);
    let (shut_tx7, shut_rx7) = tokio::sync::mpsc::channel(1);
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
    let kfg1 = KeeperConfig {
        backs: backs.clone(),
        addrs: addrs.clone(),
        this: 0,
        id: 0,
        ready: None,
        shutdown: Some(shut_rx3),
    };
    let kfg2 = KeeperConfig {
        backs: backs.clone(),
        addrs: addrs.clone(),
        this: 1,
        id: 1,
        ready: None,
        shutdown: Some(shut_rx4),
    };
    let kfg3 = KeeperConfig {
        backs: backs.clone(),
        addrs: addrs.clone(),
        this: 2,
        id: 2,
        ready: None,
        shutdown: Some(shut_rx5),
    };
    let kfg4 = KeeperConfig {
        backs: backs.clone(),
        addrs: addrs.clone(),
        this: 3,
        id: 3,
        ready: None,
        shutdown: Some(shut_rx6),
    };
    let kfg5 = KeeperConfig {
        backs: backs.clone(),
        addrs: addrs.clone(),
        this: 4,
        id: 4,
        ready: None,
        shutdown: Some(shut_rx7),
    };
    spawn_back(cfg1);
    spawn_back(cfg2);
    spawn_keep(kfg1);
    spawn_keep(kfg2);
    spawn_keep(kfg3);
    spawn_keep(kfg4);
    spawn_keep(kfg5);
    time::sleep(Duration::from_millis(777)).await;
    Ok((shut_tx1.clone(), shut_tx2.clone(), shut_tx3.clone(), shut_tx4.clone(),shut_tx5.clone(),shut_tx6.clone(),shut_tx7.clone(),))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_follow() -> TribResult<()> {
    let backs = vec![
        "127.0.0.1:33950".to_string(),
        "127.0.0.1:33951".to_string(),
    ];
    let addrs = vec![
        "127.0.0.1:33952".to_string(),
        "127.0.0.1:33953".to_string(),
        "127.0.0.1:33954".to_string(),
        "127.0.0.1:33955".to_string(),
        "127.0.0.1:33956".to_string(),
    ];

    let (tx1, tx2, tx3, tx4, tx5,tx6,tx7) = setup(backs.clone(), addrs.clone()).await?;
 
    time::sleep(time::Duration::from_secs(5)).await;
    // let _ = tx1.send(()).await;
    // let _ = tx2.send(()).await;
    // let _ = tx3.send(()).await;
    // let _ = tx5.send(()).await;
    // let _ = tx6.send(()).await;
    let _ = tx3.send(()).await;
    time::sleep(time::Duration::from_secs(5)).await;
    let _ = tx4.send(()).await;
    time::sleep(time::Duration::from_secs(5)).await;
    let (shut_tx3, shut_rx3) = tokio::sync::mpsc::channel(1);
    let kfg1 = KeeperConfig {
        backs: backs.clone(),
        addrs: addrs.clone(),
        this: 0,
        id: 0,
        ready: None,
        shutdown: Some(shut_rx3),
    };
    println!("try to start back a keeper 0");
    spawn_keep(kfg1);
    time::sleep(time::Duration::from_secs(2)).await;
    let _ = tx5.send(()).await;
    time::sleep(time::Duration::from_secs(5)).await;
    Ok(())
}

// cargo test -p lab --test lab2_test -- --nocapture