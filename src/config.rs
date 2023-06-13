use anyhow::Context;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::{collections::HashSet, sync::RwLock};
use tracing::info;

const CONFIG_FILE_NAME: &str = "config.json";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub clickhouse_url: String,
    pub clickhouse_db: String,
    pub clickhouse_username: Option<String>,
    pub clickhouse_password: Option<String>,
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
    pub channels: RwLock<HashSet<String>>,
    #[serde(rename = "clientID")]
    pub client_id: String,
    pub client_secret: String,
    pub admins: Vec<String>,
    #[serde(default)]
    pub opt_out: DashMap<String, bool>,
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        let contents = fs::read_to_string(CONFIG_FILE_NAME)
            .with_context(|| format!("Failed to load config from {CONFIG_FILE_NAME}"))?;
        serde_json::from_str(&contents).context("Config deserializtion error")
    }

    pub fn save(&self) -> anyhow::Result<()> {
        info!("Updating config");
        let json = serde_json::to_string_pretty(self)?;
        fs::write(CONFIG_FILE_NAME, json)?;

        Ok(())
    }
}

fn default_listen_address() -> String {
    String::from("0.0.0.0:8025")
}
