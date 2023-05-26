use crate::{config::Config, Result};
use anyhow::Context;
use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use tracing::debug;
use twitch_api2::{helix::users::GetUsersRequest, twitch_oauth2::AppAccessToken, HelixClient};

#[derive(Clone)]
pub struct App<'a> {
    pub helix_client: HelixClient<'a, reqwest::Client>,
    pub token: Arc<AppAccessToken>,
    pub users: Arc<DashMap<String, String>>, // User id, login name
    pub db: Arc<clickhouse::Client>,
    pub config: Arc<Config>,
}

impl App<'_> {
    pub async fn get_users(
        &self,
        ids: Vec<String>,
        names: Vec<String>,
    ) -> Result<HashMap<String, String>> {
        let mut users = HashMap::new();
        let mut ids_to_request = Vec::new();
        let mut names_to_request = Vec::new();

        for id in ids {
            if let Some(login) = self.users.get(&id) {
                // TODO dont clone
                users.insert(id, login.clone());
            } else {
                ids_to_request.push(id.into())
            }
        }

        for name in names {
            if let Some(id) = self.users.iter().find_map(|entry| {
                if entry.value() == &name {
                    Some(entry.key().clone())
                } else {
                    None
                }
            }) {
                users.insert(id, name);
            } else {
                names_to_request.push(name.into());
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

        Ok(users)
    }

    pub async fn get_user_id_by_name(&self, name: &str) -> Result<String> {
        if let Some(id) = self.users.iter().find_map(|item| {
            if item.value() == name {
                Some(item.key().clone())
            } else {
                None
            }
        }) {
            Ok(id)
        } else {
            let request = GetUsersRequest::builder().login(vec![name.into()]).build();
            let response = self.helix_client.req_get(request, &*self.token).await?;
            let user = response
                .data
                .into_iter()
                .next()
                .context("Could not get user")?;
            let user_id = user.id.into_string();

            self.users.insert(user_id.clone(), user.login.into_string());

            Ok(user_id)
        }
    }
}
