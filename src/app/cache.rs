use dashmap::DashMap;
use std::{sync::Arc, time::Instant};
use tracing::trace;

const EXPIRY_INTERVAL: u64 = 600;

#[derive(Clone, Default)]
pub struct UsersCache {
    store: Arc<DashMap<String, (Instant, String)>>, // User id, login name
}

impl UsersCache {
    pub fn insert(&self, id: String, name: String) {
        let inserted_at = Instant::now();
        self.store.insert(id, (inserted_at, name));
    }

    pub fn get_login(&self, id: &str) -> Option<String> {
        self.store.get(id).and_then(|entry| {
            if entry.value().0.elapsed().as_secs() > EXPIRY_INTERVAL {
                drop(entry);
                trace!("Removing {id} from cache");
                self.store.remove(id);
                None
            } else {
                trace!("Using cached value for id {id}");
                Some(entry.value().1.clone())
            }
        })
    }

    pub fn get_id(&self, name: &str) -> Option<String> {
        // Iter has to be a separate variable to that it can be explicitly dropped to avoid deadlock
        let mut store_iter = self.store.iter();
        if let Some(entry) = store_iter.find(|entry| entry.value().1 == name) {
            if entry.value().0.elapsed().as_secs() > EXPIRY_INTERVAL {
                let key = entry.key().clone();
                drop(entry);
                drop(store_iter);
                trace!("Removing {name} from cache");
                self.store.remove(&key);
                None
            } else {
                trace!("Using cached value for name {name}");
                Some(entry.key().clone())
            }
        } else {
            None
        }
    }
}
