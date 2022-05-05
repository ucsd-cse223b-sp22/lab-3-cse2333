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
use tokio::{sync::mpsc::Sender as MpscSender};
use tonic::transport::channel;
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

async fn setup(backs: Vec<String>, keeper_addr: String) -> TribResult<Vec<MpscSender<()>>> {
    let mut res=Vec::new();
    for i in backs.iter(){
        let (shut_tx, shut_rx) = tokio::sync::mpsc::channel(1);
        let cfg = BackConfig {
                addr: i.to_string(),
                storage: Box::new(MemStorage::default()),
                ready: None,
                shutdown: Some(shut_rx),
            };
        spawn_back(cfg);
        res.push(shut_tx.clone());
    }
    let (shut_tx_tmp, shut_rx_tmp) = tokio::sync::mpsc::channel(1);
    let kfg = KeeperConfig {
        backs: backs.clone(),
        addrs: vec![keeper_addr],
        this: 0,
        id: 0,
        ready: None,
        shutdown: Some(shut_rx_tmp),
    };
    res.push(shut_tx_tmp.clone());
    spawn_keep(kfg);
    tokio::time::sleep(Duration::from_millis(777)).await;
    Ok(res)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_multi_backend() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bc = lab2::new_bin_client(backs.clone()).await?;
    let frontend = lab2::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor="alex";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;
    
    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;
    let crazy_cs_following_list = frontend.following(crazy_fan_johnny_su).await?;
    println!("test_simple_follow: {:?}", crazy_cs_following_list);
    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_follow() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bc = lab2::new_bin_client(backs.clone()).await?;
    let frontend = lab2::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;
    
    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;
    let crazy_cs_following_list = frontend.following(crazy_fan_johnny_su).await?;
    println!("test_simple_follow: {:?}", crazy_cs_following_list);
    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_duplicate_follow() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bc = lab2::new_bin_client(backs.clone()).await?;
    let frontend = lab2::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;
    
    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;
    let res = frontend.follow(crazy_fan_johnny_su, speechless_professor).await;
    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    if res.is_ok() {
        assert!(false, "what the fuck dude you are not supposed to follow professor twice you creepy motherfucker!");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_duplicate_unfollow() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bc = lab2::new_bin_client(backs.clone()).await?;
    let frontend = lab2::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;

    let mut res = frontend.unfollow(crazy_fan_johnny_su, speechless_professor).await;
    if res.is_ok() {
        assert!(false, "what the fuck dude you can't unfollow if you aren't following!");
    }

    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;
    frontend.unfollow(crazy_fan_johnny_su, speechless_professor).await?;
    res = frontend.unfollow(crazy_fan_johnny_su, speechless_professor).await;
    if res.is_ok() {
        assert!(false, "what the fuck dude you can't do duplicate unfollow ;( I'm so disappointed at you!");
    }

    let following_list = frontend.following(crazy_fan_johnny_su).await?;
    println!("test_duplicate_unfollow final following list: {:?}", following_list);
    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_follow() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bc = lab2::new_bin_client(backs.clone()).await?;
    let frontend0 = lab2::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend0.sign_up(crazy_fan_johnny_su).await?;
    frontend0.sign_up(speechless_professor).await?;

    let mut promises = vec![];
    let NUM_CONCURRENCY = 4;
    let mut error_count = 0;

    // for i in 0..NUM_CONCURRENCY {
    //     promises.push(frontend.follow(crazy_fan_johnny_su, speechless_professor));
    // }

    // for promise in promises {
    //     let res = promise.await;
    //     if res.is_err() {
    //         error_count += 1;
    //     }
    // }

    for i in 0..NUM_CONCURRENCY {
        let bc = lab2::new_bin_client(backs.clone()).await?;
        let frontend = lab2::new_front(bc).await?;
        promises.push(tokio::task::spawn(async move { frontend.follow(crazy_fan_johnny_su, speechless_professor).await }));
    }

    for promise in promises {
        let res = promise.await?;
        if res.is_err() {
            error_count += 1;
        }
    }

    let bc = lab2::new_bin_client(backs.clone()).await?;
    let client_future_who = bc.bin(crazy_fan_johnny_su);
    let client_who = client_future_who.await?;
    let follow_log_key = format!("{}::{}", crazy_fan_johnny_su, "FOLLOWLOG");
    let follow_log = client_who.list_get(follow_log_key.as_str()).await?.0;
    println!("following log: {:?}", follow_log);

    if error_count != NUM_CONCURRENCY - 1 {
        assert!(false, "{}??? YOU KNOW WHAT IT IS!!! IT'S THE RACE!!! YOU DIDN'T SYNCHRONIZE AND NOW WE HAVE TO FACE THE KARMA!!!", error_count);
    }
    let following_list = frontend0.following(crazy_fan_johnny_su).await?;
    println!("test_concurrent_follow final following list: {:?}", following_list);

    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_bins_diff_keys_massive_set_get() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bin_client = lab2::new_bin_client(backs.clone()).await?;
    // let frontend = lab2::new_front(bc).await?;
    let STRING_LEN = 30;
    let NUM_KEYS = 777;

    let mut key_val_map = HashMap::new();
    let mut key_bin_map = HashMap::new();
    for i in 0..NUM_KEYS {
        let bin_name = generate_random_string(STRING_LEN);
        let key = generate_random_string(STRING_LEN);
        let value = generate_random_string(STRING_LEN);
        key_val_map.insert(key.to_string(), value.to_string());
        key_bin_map.insert(key.to_string(), bin_name.to_string());
        let client = bin_client.bin(bin_name.as_str()).await?;
        client.set(&KeyValue {
            key: key.to_string(),
            value: value.to_string(),
        }).await?;
    }

    for key in key_val_map.keys() {
        let bin_name = key_bin_map.get(key).unwrap();
        let expected_value = key_val_map.get(key).unwrap();
        let client = bin_client.bin(bin_name.as_str()).await?;
        let actual_value = client.get(key).await?.unwrap();
        if actual_value != expected_value.to_string() {
            assert!(false, "OH BOY, OH BOY, OH BOY!!! dude you just failed the mass get set test 
            ;( sry but this is really nothing but a very basic feature yet you failed it")
        }
    }
    
    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_bins_same_key_massive_set_get() -> TribResult<()> {
    // funny story: 根据pigeonhole principle (雀巢定律，鸽笼定律，抽屉原理，whatever you called it blablabla)
    // 我们只需要4个bin!!!就可以测出你有没有分隔开来virtual bins!!!
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bin_client = lab2::new_bin_client(backs.clone()).await?;
    // let frontend = lab2::new_front(bc).await?;
    let STRING_LEN = 30;

    let KEY = "jerkoff";
    let mut bin_val_map = HashMap::new();
    for i in 0..1000 {
        let bin_name = generate_random_string(STRING_LEN);
        let value = generate_random_string(STRING_LEN);
        bin_val_map.insert(bin_name.to_string(), value.to_string());
        let client = bin_client.bin(bin_name.as_str()).await?;
        client.set(&KeyValue {
            key: KEY.to_string(),
            value: value.to_string(),
        }).await?;
    }

    for bin_name in bin_val_map.keys() {
        let expected_value = bin_val_map.get(bin_name).unwrap();
        let client = bin_client.bin(bin_name.as_str()).await?;
        let actual_value = client.get(KEY).await?.unwrap();
        if actual_value != expected_value.to_string() {
            assert!(false, "Where's our friendly little indirection layer that virtually separates the bins? Huh? WHERE IS IT? WHERE IS IT???!!!!");
        }
    }
    
    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_signup_users() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bc = lab2::new_bin_client(backs.clone()).await?;
    let frontend = lab2::new_front(bc).await?;

    let mut hashset = HashSet::new();
    let first_guy = generate_random_string(7);
    frontend.sign_up(first_guy.as_str()).await?;
    hashset.insert(first_guy.to_string());
    for i in 0..30 {
        let user_name = generate_random_string(7);
        hashset.insert(user_name.to_string());
        frontend.sign_up(user_name.as_str()).await?;
    }
    let thirtish_guy = generate_random_string(7);
    frontend.sign_up(thirtish_guy.as_str()).await?;
    hashset.insert(thirtish_guy.to_string());
    
    let register_list = frontend.list_users().await?;
    if register_list.len() < 20 {
        assert!(false, "register list has wrong len");
    }
    println!("register list: {:?}", register_list);
    for user in register_list {
        if !hashset.contains(&user) {
            assert!(false, "duuude you should contain user {}", user.to_string());
        }
    }

    let res_first = frontend.sign_up(first_guy.as_str()).await;
    if res_first.is_ok() {
        assert!(false, "duuude first guy should be an error");
    }

    let res_thir = frontend.sign_up(thirtish_guy.as_str()).await;
    if res_thir.is_ok() {
        assert!(false, "duuude thirtish guy should be an error");
    }

    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_signup_users_less_than_20() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bc = lab2::new_bin_client(backs.clone()).await?;
    let frontend = lab2::new_front(bc).await?;

    let mut hashset = HashSet::new();
    let NUMBER = 15;
    for i in 0..NUMBER {
        let user_name = generate_random_string(7);
        hashset.insert(user_name.to_string());
        frontend.sign_up(user_name.as_str()).await?;
    }
    
    let register_list = frontend.list_users().await?;
    if register_list.len() != NUMBER {
        assert!(false, "register list has wrong len");
    }
    println!("register list: {:?}", register_list);
    for user in register_list.clone() {
        if !hashset.contains(&user) {
            assert!(false, "duuude you should contain user {}", user.to_string());
        }
    }

    for user in register_list.clone() {
        let res = frontend.sign_up(&user).await;
        if res.is_ok() {
            assert!(false, "not supposed to sign up");
        }
    }

    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}

async fn test_simple_tribs() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bc = lab2::new_bin_client(backs.clone()).await?;
    let frontend = lab2::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    
    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simple_tribs_1_and_3() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bc = lab2::new_bin_client(backs.clone()).await?;
    let frontend = lab2::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;
    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;

    frontend.post(speechless_professor, "al_p0", 0).await?;
    frontend.post(speechless_professor, "al_p1", 1).await?;
    frontend.post(speechless_professor, "al_p2", 2).await?;
    frontend.post(speechless_professor, "al_p3", 100).await?;
    
    let home_tribs = frontend.home(crazy_fan_johnny_su).await?;
    let mut max_clock = 0;
    for trib in home_tribs {
        max_clock = cmp::max(max_clock, trib.to_owned().clock);
    }
    frontend.post(crazy_fan_johnny_su, "su_p0", max_clock).await?;
    frontend.post(crazy_fan_johnny_su, "su_p1", max_clock).await?;
    let tribs = frontend.tribs(crazy_fan_johnny_su).await?;
    let last_clock = tribs[tribs.len()-1].to_owned().clock;
    let second_last_clock =  tribs[tribs.len()-2].to_owned().clock;

    if last_clock != 101 || second_last_clock != 100 {
        assert!(false, "Just put a bullet in my fxxking brain plz");
    }

    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_simple_tribs_2() -> TribResult<()> {
    let mut backs=Vec::new();
    for i in 0..4{
        let mut ip_str = "127.0.0.1:".to_string();
        let port_num:i32 = 33951+i;
        ip_str.push_str(&port_num.to_string());
        backs.push(ip_str);
    }
    let keeper_addr = "127.0.0.1:33950".to_string();
    let channels = setup(backs.clone(), keeper_addr).await?;
    let bc = lab2::new_bin_client(backs.clone()).await?;
    let frontend = lab2::new_front(bc).await?;

    let crazy_fan_johnny_su = "johnnysu";
    let speechless_professor = "alexsnoeren";
    frontend.sign_up(crazy_fan_johnny_su).await?;
    frontend.sign_up(speechless_professor).await?;
    frontend.follow(crazy_fan_johnny_su, speechless_professor).await?;

    frontend.post(speechless_professor, "al", 100).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    frontend.post(crazy_fan_johnny_su, "js", 0).await?;
    
    let home_tribs = frontend.home(crazy_fan_johnny_su).await?;
    if home_tribs.len() != 2 {
        assert!(false, "my soul is burning in hell");
    }
    let first_poster = &home_tribs[0].to_owned().user;
    let first_trib = &home_tribs[0].to_owned().message;
    let first_clock = &home_tribs[0].to_owned().clock;
    let second_poster =  &home_tribs[1].to_owned().user;
    let second_trib = &home_tribs[1].to_owned().message;
    let second_clock = &home_tribs[1].to_owned().clock;
    println!("{}, {}, {}, {}, {}, {}", first_poster, first_trib, first_clock, second_poster, second_trib, second_clock);

    if first_poster != speechless_professor 
    || second_poster != crazy_fan_johnny_su 
    || second_clock <= &100 
    || first_trib != "al"
    || second_trib != "js" {
        assert!(false, "highway to hell!!!!");
    }

    for i in channels.iter(){
        let _ = i.send(()).await;
    }
    Ok(())
}
// cargo test -p lab --test lab3_test -- --nocapture