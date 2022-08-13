use crate::{error::Error, logs::Logs};
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
}

impl App<'_> {
    pub async fn get_users(
        &self,
        ids: Vec<String>,
        names: Vec<String>,
    ) -> Result<HashMap<String, String>, Error> {
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
}
