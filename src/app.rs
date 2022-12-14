use crate::{config::Config, logs::Logs, Result};
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
    pub logs: Logs,
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

        if !ids_to_request.is_empty() || !names_to_request.is_empty() {
            debug!(
                "Requesting user info for ids {ids_to_request:?} and names {names_to_request:?}"
            );
            let request = GetUsersRequest::builder()
                .id(ids_to_request)
                .login(names_to_request)
                .build();

            let response = self.helix_client.req_get(request, &*self.token).await?;

            for user in response.data {
                let id = user.id.into_string();
                let login = user.login.into_string();

                self.users.insert(id.clone(), login.clone());

                users.insert(id, login);
            }
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
