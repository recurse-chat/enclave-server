use std::path::PathBuf;

use crate::data::{messages::MessageStore, users::UserMetaStore};

pub struct DataStore {
    pub messages: MessageStore,
    pub users: UserMetaStore,
}

impl DataStore {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            messages: MessageStore::new(PathBuf::from("messages"))?,
            users: UserMetaStore::new(PathBuf::from("users.db"))?,
        })
    }
}