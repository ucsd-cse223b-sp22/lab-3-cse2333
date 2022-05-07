use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::{cmp::min, cmp::Ordering, sync::Arc, time::SystemTime};
use tribbler::err::{TribResult, TribblerError};
use tribbler::storage::{BinStorage, KeyValue};
use tribbler::trib::{
    is_valid_username, Server, Trib, MAX_FOLLOWING, MAX_TRIB_FETCH, MAX_TRIB_LEN, MIN_LIST_USER,
};

pub struct FrontServer {
    pub bin_storage: Box<dyn BinStorage>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct FollowLog {
    name: String,
    follow: bool,
    id: u64,
}

#[derive(Debug, Clone)]
struct OrderTrib {
    trib: Arc<Trib>,
}

impl Ord for OrderTrib {
    fn cmp(&self, other: &Self) -> Ordering {
        let res = self.trib.clock.cmp(&other.trib.clock);
        match res {
            Ordering::Equal => (),
            _ => {
                return res;
            }
        };
        let res1 = self.trib.time.cmp(&other.trib.time);
        match res1 {
            Ordering::Equal => (),
            _ => {
                return res1;
            }
        };
        let res2 = self.trib.user.cmp(&other.trib.user);
        match res2 {
            Ordering::Equal => (),
            _ => {
                return res2;
            }
        };
        return self.trib.message.cmp(&other.trib.message);
    }
}

impl Eq for OrderTrib {}

impl PartialOrd for OrderTrib {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let res = self.trib.clock.partial_cmp(&other.trib.clock);
        match res {
            Some(Ordering::Equal) => (),
            Some(value) => {
                return Some(value);
            }
            None => {
                return None;
            }
        };
        let res1 = self.trib.time.partial_cmp(&other.trib.time);
        match res1 {
            Some(Ordering::Equal) => (),
            Some(value) => {
                return Some(value);
            }
            None => {
                return None;
            }
        };
        let res2 = self.trib.user.partial_cmp(&other.trib.user);
        match res2 {
            Some(Ordering::Equal) => (),
            Some(value) => {
                return Some(value);
            }
            None => {
                return None;
            }
        };
        return self.trib.message.partial_cmp(&other.trib.message);
    }
}

impl PartialEq for OrderTrib {
    fn eq(&self, other: &Self) -> bool {
        self.trib.clock == other.trib.clock
            && self.trib.time == other.trib.time
            && self.trib.user == other.trib.user
            && self.trib.message == other.trib.message
    }
}

#[async_trait]
impl Server for FrontServer {
    async fn sign_up(&self, user: &str) -> TribResult<()> {
        let storage_client = self.bin_storage.bin("Users").await?;
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }

        let user_list = storage_client.list_get("Users").await?.0;
        if user_list.contains(&user.to_string()) {
            return Err(Box::new(TribblerError::UsernameTaken(user.to_string())));
        }

        let flag = storage_client
            .list_append(&KeyValue {
                key: "Users".to_string(),
                value: user.to_string(),
            })
            .await?;

        if !flag {
            return Err(Box::new(TribblerError::Unknown(user.to_string())));
        }
        Ok(())
    }

    async fn list_users(&self) -> TribResult<Vec<String>> {
        let storage_client = self.bin_storage.bin("Users").await?;
        let k = storage_client.list_get("Users").await?.0;
        let all_users_set: HashSet<String> = HashSet::from_iter(k);
        let mut all_users_list: Vec<String> = Vec::from_iter(all_users_set);
        all_users_list.sort();
        println!("{}", all_users_list.len());
        let sorted = all_users_list[..min(MIN_LIST_USER, all_users_list.len())].to_vec();
        Ok(sorted)
    }

    async fn post(&self, who: &str, post: &str, clock: u64) -> TribResult<()> {
        if post.len() > MAX_TRIB_LEN {
            return Err(Box::new(TribblerError::TribTooLong));
        }
        let user_list = self
            .bin_storage
            .bin("Users")
            .await?
            .list_get("Users")
            .await?
            .0;

        if !user_list.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        let post = serde_json::to_string(&Trib {
            user: who.to_string(),
            message: post.to_string(),
            clock: self.bin_storage.bin(who).await?.clock(clock).await?,
            time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs(),
        })
        .unwrap();

        let flag = self
            .bin_storage
            .bin(who)
            .await?
            .list_append(&KeyValue {
                key: "tribs".to_string(), // user::posts
                value: post,
            })
            .await?;
        if !flag {
            return Err(Box::new(TribblerError::Unknown(
                "Post failed due to list_append error.".to_string(),
            )));
        }
        return Ok(());
    }

    async fn tribs(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        let user_list = self
            .bin_storage
            .bin("Users")
            .await?
            .list_get("Users")
            .await?
            .0;
        if !user_list.contains(&user.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }

        let tribs = self.bin_storage.bin(user).await?.list_get("tribs").await?.0;
        let ntrib = tribs.len();
        let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => ntrib - MAX_TRIB_FETCH,
            _ => 0,
        };
        let mut res = tribs
            .to_vec()
            .iter()
            .map(|x| OrderTrib {
                trib: Arc::new(serde_json::from_str::<Trib>(x).unwrap()).clone(),
            })
            .collect::<Vec<OrderTrib>>();
        res.sort();
        let res0 = res[start..]
            .iter()
            .map(|x| x.trib.clone())
            .collect::<Vec<Arc<Trib>>>();
        return Ok(res0);
    }

    async fn follow(&self, who: &str, whom: &str) -> TribResult<()> {
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        let user_list = self
            .bin_storage
            .bin("Users")
            .await?
            .list_get("Users")
            .await?
            .0;
        if !user_list.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        if !user_list.contains(&whom.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }
        let is_following = self.is_following(who, whom).await?;
        if is_following {
            return Err(Box::new(TribblerError::AlreadyFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }
        let following = self.following(who).await?;
        if following.len() == MAX_FOLLOWING {
            return Err(Box::new(TribblerError::FollowingTooMany));
        }

        // follow
        let id = self.bin_storage.bin(who).await?.clock(0).await?;
        let follow_log = serde_json::to_string(&FollowLog {
            name: whom.to_string(),
            follow: true,
            id: id,
        })
        .unwrap();
        let flag = self
            .bin_storage
            .bin(who)
            .await?
            .list_append(&KeyValue {
                key: "log".to_string(),
                value: follow_log,
            })
            .await?;
        if !flag {
            return Err(Box::new(TribblerError::Unknown(
                "Follow failed due to list_append error".to_string(),
            )));
        }

        // check if succeed
        let logs = self.bin_storage.bin(who).await?.list_get("log").await?.0;
        let mut already_follow = false;
        for log in logs.iter() {
            let entry = serde_json::from_str::<FollowLog>(log).unwrap();
            if entry.name.eq(whom) {
                if entry.id == id {
                    if already_follow {
                        return Err(Box::new(TribblerError::Unknown(
                            "Follow failed due to multiple requests.".to_string(),
                        )));
                    } else {
                        return Ok(());
                    }
                } else {
                    if entry.follow {
                        already_follow = true;
                    } else {
                        already_follow = false;
                    }
                }
            }
        }

        return Ok(());
    }

    async fn unfollow(&self, who: &str, whom: &str) -> TribResult<()> {
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        let user_list = self
            .bin_storage
            .bin("Users")
            .await?
            .list_get("Users")
            .await?
            .0;
        if !user_list.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        if !user_list.contains(&whom.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }
        let is_following = self.is_following(who, whom).await?;
        if !is_following {
            return Err(Box::new(TribblerError::NotFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }

        // unfollow
        let id = self.bin_storage.bin(who).await?.clock(0).await?;
        let follow_log = serde_json::to_string(&FollowLog {
            name: whom.to_string(),
            follow: false,
            id: id,
        })
        .unwrap();
        let flag = self
            .bin_storage
            .bin(who)
            .await?
            .list_append(&KeyValue {
                key: "log".to_string(),
                value: follow_log,
            })
            .await?;
        if !flag {
            return Err(Box::new(TribblerError::Unknown(
                "Unollow failed due to list_append error".to_string(),
            )));
        }

        // check if succeed
        let logs = self.bin_storage.bin(who).await?.list_get("log").await?.0;
        let mut already_follow = false;
        for log in logs.iter() {
            let entry = serde_json::from_str::<FollowLog>(log).unwrap();
            if entry.name.eq(whom) {
                if entry.id == id {
                    if already_follow {
                        // already_follow, then unfollow
                        return Ok(());
                    } else {
                        // already unfollow, then fail
                        return Err(Box::new(TribblerError::Unknown(
                            "Unfollow failed due to multiple requests.".to_string(),
                        )));
                    }
                } else {
                    if entry.follow {
                        already_follow = true;
                    } else {
                        already_follow = false;
                    }
                }
            }
        }
        return Ok(());
    }

    async fn is_following(&self, who: &str, whom: &str) -> TribResult<bool> {
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        let user_list = self
            .bin_storage
            .bin("Users")
            .await?
            .list_get("Users")
            .await?
            .0;
        if !user_list.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        if !user_list.contains(&whom.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        let logs = self.bin_storage.bin(who).await?.list_get("log").await?.0;
        let mut res = false;
        for log in logs.iter().rev() {
            let entry = serde_json::from_str::<FollowLog>(log).unwrap();
            if entry.name.eq(whom) {
                res = entry.follow;
                break;
            }
        }
        return Ok(res);
    }

    async fn following(&self, who: &str) -> TribResult<Vec<String>> {
        let user_list = self
            .bin_storage
            .bin("Users")
            .await?
            .list_get("Users")
            .await?
            .0;
        if !user_list.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        let mut res: HashSet<String> = HashSet::new();
        let logs = self.bin_storage.bin(who).await?.list_get("log").await?.0;
        for log in logs.iter() {
            let entry = serde_json::from_str::<FollowLog>(log).unwrap();
            if entry.follow {
                res.insert(entry.name);
            } else {
                res.remove(&entry.name); // whether succeed or fail do not matter
            }
        }
        return Ok(res.iter().map(String::clone).collect());
    }

    async fn home(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        let user_list = self
            .bin_storage
            .bin("Users")
            .await?
            .list_get("Users")
            .await?
            .0;
        if !user_list.contains(&user.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }

        let following_list = self.following(user).await?;
        let mut all_tribs: Vec<Arc<Trib>> = Vec::new();
        for name in following_list {
            let mut each_tribs = self.tribs(&name).await?;
            all_tribs.append(&mut each_tribs);
        }
        all_tribs.append(&mut self.tribs(user).await?);
        let ntrib = all_tribs.len();
        let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => ntrib - MAX_TRIB_FETCH,
            _ => 0,
        };

        let mut res = all_tribs
            .to_vec()
            .iter()
            .map(|x| OrderTrib { trib: x.clone() })
            .collect::<Vec<OrderTrib>>();
        res.sort();
        let res0 = res[start..]
            .iter()
            .map(|x| x.trib.clone())
            .collect::<Vec<Arc<Trib>>>();
        return Ok(res0);
    }
}
