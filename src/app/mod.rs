pub mod cache;

use self::cache::UsersCache;
use crate::{config::Config, error::Error, Result};
use dashmap::DashSet;
use std::{collections::HashMap, sync::Arc};
use tracing::debug;
use twitch_api2::{helix::users::GetUsersRequest, twitch_oauth2::AppAccessToken, HelixClient};

#[derive(Clone)]
pub struct App {
    pub helix_client: HelixClient<'static, reqwest::Client>,
    pub token: Arc<AppAccessToken>,
    pub users: UsersCache,
    pub optout_codes: Arc<DashSet<String>>,
    pub db: Arc<clickhouse::Client>,
    pub config: Arc<Config>,
}

impl App {
    pub async fn get_users(
        &self,
        ids: Vec<String>,
        names: Vec<String>,
    ) -> Result<HashMap<String, String>> {
        let mut users = HashMap::new();
        let mut ids_to_request = Vec::new();
        let mut names_to_request = Vec::new();

        for id in ids {
            match self.users.get_login(&id) {
                Some(Some(login)) => {
                    users.insert(id, login);
                }
                Some(None) => (),
                None => ids_to_request.push(id.into()),
            }
        }

        for name in names {
            match self.users.get_id(&name) {
                Some(Some(id)) => {
                    users.insert(id, name);
                }
                Some(None) => (),
                None => names_to_request.push(name.into()),
            }
        }

        let mut new_users = Vec::with_capacity(ids_to_request.len() + names_to_request.len());

        // There are no chunks if the vec is empty, so there is no empty request made
        for chunk in ids_to_request.chunks(100) {
            debug!("Requesting user info for ids {chunk:?}");

            let request = GetUsersRequest::builder().id(chunk.to_vec()).build();
            let response = self.helix_client.req_get(request, &*self.token).await?;
            new_users.extend(response.data);
        }

        for chunk in names_to_request.chunks(100) {
            debug!("Requesting user info for names {chunk:?}");

            let request = GetUsersRequest::builder().login(chunk.to_vec()).build();
            let response = self.helix_client.req_get(request, &*self.token).await?;
            new_users.extend(response.data);
        }

        for user in new_users {
            let id = user.id.into_string();
            let login = user.login.into_string();

            self.users.insert(id.clone(), login.clone());

            users.insert(id, login);
        }

        // Banned users which were not returned by the api
        for id in ids_to_request {
            if !users.contains_key(id.as_str()) {
                self.users.insert_optional(Some(id.into_string()), None);
            }
        }
        for name in names_to_request {
            if !users.values().any(|login| login == name.as_str()) {
                self.users.insert_optional(None, Some(name.into_string()));
            }
        }

        Ok(users)
    }

    pub async fn get_user_id_by_name(&self, name: &str) -> Result<String> {
        match self.users.get_id(name) {
            Some(Some(id)) => Ok(id),
            Some(None) => Err(Error::NotFound),
            None => {
                let request = GetUsersRequest::builder().login(vec![name.into()]).build();
                let response = self.helix_client.req_get(request, &*self.token).await?;
                match response.data.into_iter().next() {
                    Some(user) => {
                        let user_id = user.id.into_string();
                        self.users.insert(user_id.clone(), user.login.into_string());
                        Ok(user_id)
                    }
                    None => {
                        self.users.insert_optional(None, Some(name.to_owned()));
                        Err(Error::NotFound)
                    }
                }
            }
        }
    }

    pub fn check_opted_out(&self, channel_id: &str, user_id: Option<&str>) -> Result<()> {
        if self.config.opt_out.contains_key(channel_id) {
            return Err(Error::OptedOut);
        }

        if let Some(user_id) = user_id {
            if self.config.opt_out.contains_key(user_id) {
                return Err(Error::OptedOut);
            }
        }

        Ok(())
    }
}
