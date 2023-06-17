use crate::{
    config::Config,
    db::{read_all_available_channel_logs, AvailableChannelLogs},
    error::Error,
    Result,
};
use anyhow::Context;
use arc_swap::ArcSwap;
use dashmap::{DashMap, DashSet};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tracing::{debug, error};
use twitch_api2::{helix::users::GetUsersRequest, twitch_oauth2::AppAccessToken, HelixClient};

#[derive(Clone)]
pub struct App<'a> {
    pub helix_client: HelixClient<'a, reqwest::Client>,
    pub token: Arc<AppAccessToken>,
    pub users: Arc<DashMap<String, String>>, // User id, login name
    pub optout_codes: Arc<DashSet<String>>,
    pub channel_log_dates_cache: Arc<ArcSwap<AvailableChannelLogs>>,
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

    pub fn start_channel_log_dates_cacher(&self) {
        let app = self.clone();

        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(
                    app.config.channel_logs_date_cache_interval,
                ))
                .await;

                let started_at = Instant::now();
                match read_all_available_channel_logs(&app.db).await {
                    Ok(new_dates) => {
                        app.channel_log_dates_cache.store(Arc::new(new_dates));
                        debug!(
                            "Updated channel log dates cache (took {}ms)",
                            started_at.elapsed().as_millis()
                        );
                    }
                    Err(err) => {
                        error!("Could not update available channel logs: {err}");
                    }
                }
            }
        });
    }
}
