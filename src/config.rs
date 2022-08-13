use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::fs;

const CONFIG_FILE_NAME: &str = "config.json";

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub logs_directory: String,
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
    pub channels: Vec<String>,
    #[serde(rename = "clientID")]
    pub client_id: String,
    pub client_secret: String,
}

impl Config {
    pub async fn load() -> anyhow::Result<Self> {
        let contents = fs::read_to_string(CONFIG_FILE_NAME)
            .await
            .with_context(|| format!("Failed to load config from {CONFIG_FILE_NAME}"))?;
        Ok(serde_json::from_str(&contents).context("Config deserializtion error")?)
    }

    /*pub async fn save(&self) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        fs::write(CONFIG_FILE_NAME, &json).await?;

        Ok(())
    }*/
}

fn default_listen_address() -> String {
    String::from("0.0.0.0:8025")
}
