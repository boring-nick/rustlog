use dashmap::DashMap;
use std::{sync::Arc, time::Instant};
use tracing::trace;

const EXPIRY_INTERVAL: u64 = 7200;

// Banned users are stored as None
#[derive(Clone, Default)]
pub struct UsersCache {
    ids: Arc<DashMap<String, (Instant, Option<String>)>>,
    logins: Arc<DashMap<String, (Instant, Option<String>)>>,
}

impl UsersCache {
    pub fn insert(&self, id: String, name: String) {
        self.insert_optional(Some(id), Some(name));
    }

    pub fn insert_optional(&self, id: Option<String>, name: Option<String>) {
        let inserted_at = Instant::now();

        if let Some(id) = id.clone() {
            self.ids.insert(id, (inserted_at, name.clone()));
        }

        if let Some(name) = name {
            self.logins.insert(name, (inserted_at, id));
        }
    }

    pub fn get_login(&self, id: &str) -> Option<Option<String>> {
        if let Some(entry) = self.ids.get(id) {
            if entry.value().0.elapsed().as_secs() > EXPIRY_INTERVAL {
                drop(entry);
                trace!("Removing {id} from cache");
                self.ids.remove(id);
                None
            } else {
                trace!("Using cached value for id {id}");
                Some(entry.value().1.clone())
            }
        } else {
            None
        }
    }

    pub fn get_id(&self, name: &str) -> Option<Option<String>> {
        if let Some(entry) = self.logins.get(name) {
            if entry.value().0.elapsed().as_secs() > EXPIRY_INTERVAL {
                let key = entry.key().clone();
                drop(entry);
                trace!("Removing {name} from cache");
                self.logins.remove(&key);
                None
            } else {
                trace!("Using cached value for name {name}");
                Some(entry.value().1.clone())
            }
        } else {
            None
        }
    }
}
