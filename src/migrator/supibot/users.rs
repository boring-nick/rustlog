use anyhow::{anyhow, Context};
use futures::future::join_all;
use serde::Deserialize;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fs::File,
    io::BufReader,
    path::Path,
};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

#[derive(Default)]
pub struct UsersClient {
    client: reqwest::Client,
    users: HashMap<String, String>,
    // Names mapped to ids
    names: HashMap<String, Option<String>>,
}

#[derive(Deserialize)]
struct FileUser {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Twitch_ID")]
    id: String,
}

impl UsersClient {
    pub fn add_from_file(&mut self, file_path: &Path) -> anyhow::Result<()> {
        info!("Loading users from {file_path:?}");

        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let rdr = csv::Reader::from_reader(reader);

        let mut duplicate_names = HashSet::new();
        let mut duplicate_ids = HashSet::new();

        for user in rdr.into_deserialize::<FileUser>() {
            let user = user?;

            if !duplicate_ids.contains(&user.id) {
                match self.users.entry(user.id.clone()) {
                    Entry::Occupied(o) => {
                        let (id, _) = o.remove_entry();
                        duplicate_ids.insert(id);
                    }
                    Entry::Vacant(v) => {
                        v.insert(user.name.clone());
                    }
                }
            }

            if !duplicate_names.contains(&user.name) {
                match self.names.entry(user.name) {
                    Entry::Occupied(o) => {
                        let (name, _) = o.remove_entry();
                        duplicate_names.insert(name);
                    }
                    Entry::Vacant(v) => {
                        v.insert(Some(user.id));
                    }
                }
            }
        }

        info!(
            "{} users loaded ({} duplicate names, {} duplicate ids)",
            self.users.len(),
            duplicate_names.len(),
            duplicate_ids.len()
        );

        Ok(())
    }

    pub async fn get_users(
        &mut self,
        ids: &[impl AsRef<str>],
    ) -> anyhow::Result<HashMap<String, String>> {
        let mut ids_to_request = Vec::with_capacity(ids.len());
        let mut response_users = HashMap::with_capacity(ids.len());

        for id in ids {
            match self.users.get(id.as_ref()) {
                Some(name) => {
                    response_users.insert(id.as_ref().to_owned(), name.clone());
                }
                None => {
                    ids_to_request.push(id.as_ref());
                }
            }
        }

        let concurrent_limit = Semaphore::new(5);

        let request_futures = ids_to_request.chunks(50).map(|chunk| {
            info!("Requesting a chunk of {} users", chunk.len());
            debug!("{chunk:?}");

            async {
                let _lock = concurrent_limit.acquire().await.unwrap();

                let response = self
                    .client
                    .get("https://api.ivr.fi/v2/twitch/user")
                    .query(&[("id", chunk.join(","))])
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!(
                        "Got an error from IVR API: {} {}",
                        response.status(),
                        response.text().await?
                    ));
                }
                Ok(response.json::<Vec<IvrUser>>().await?)
            }
        });
        let results = join_all(request_futures).await;
        // let mut results = Vec::with_capacity(request_futures.len());
        // for future in request_futures {
        //     results.push(future.await);
        // }

        for result in results {
            let api_response = result?;
            for user in api_response {
                self.users.insert(user.id.clone(), user.login.clone());
                response_users.insert(user.id.clone(), user.login);
            }
        }

        Ok(response_users)
    }

    pub async fn get_user_id_by_name(&mut self, name: &str) -> anyhow::Result<Option<String>> {
        match self.names.get(name) {
            Some(id) => Ok(id.clone()),
            None => {
                debug!("Fetching info for name {name}");
                let response = self
                    .client
                    .get("https://api.ivr.fi/v2/twitch/user")
                    .query(&[("login", name)])
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!(
                        "Got an error from IVR API: {} {}",
                        response.status(),
                        response.text().await?
                    ));
                }

                let users: Vec<IvrUser> = response
                    .json()
                    .await
                    .context("Could not deserialize IVR response")?;

                match users.into_iter().next() {
                    Some(user) => {
                        self.names.insert(user.login.clone(), Some(user.id.clone()));
                        self.users.insert(user.id.clone(), user.login.clone());
                        Ok(Some(user.id))
                    }
                    None => {
                        warn!("User {name} cannot be retrieved");
                        self.names.insert(name.to_owned(), None);
                        Ok(None)
                    }
                }
            }
        }
    }

    pub fn get_cached_user_login(&self, id: &str) -> Option<&str> {
        self.users.get(id).map(|s| s.as_str())
    }
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IvrUser {
    pub id: String,
    pub display_name: String,
    pub login: String,
    pub chat_color: Option<String>,
}
