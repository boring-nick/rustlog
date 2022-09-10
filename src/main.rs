mod app;
mod args;
mod bot;
mod config;
mod error;
mod logs;
mod migrator;
mod reindexer;
mod web;

pub type Result<T> = std::result::Result<T, error::Error>;

use app::App;
use args::{Args, Command};
use clap::Parser;
use config::Config;
use dashmap::DashMap;
use logs::{
    index::{self, Index},
    Logs,
};
use migrator::Migrator;
use std::{path::PathBuf, sync::Arc};
use tokio::{
    fs::{read_dir, File},
    io::AsyncReadExt,
    try_join,
};
use tracing::info;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};
use twitch_api2::{
    twitch_oauth2::{AppAccessToken, Scope},
    HelixClient,
};
use twitch_irc::login::StaticLoginCredentials;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();

    let config = Config::load().await?;
    let logs = Logs::new(&config.logs_directory).await?;

    let args = Args::parse();

    match args.subcommand {
        None => run(config, logs).await,
        Some(Command::Reindex { channels }) => {
            let channel_list = channels.map_or_else(Vec::new, |channels| {
                channels.split(',').map(str::to_owned).collect()
            });

            reindex(config, logs, channel_list).await
        }
        Some(Command::PrintIndex { file_path }) => print_index(file_path).await,
        Some(Command::Migrate { source_dir }) => migrate(config, logs, source_dir).await,
    }
}

async fn run(config: Config, logs: Logs) -> anyhow::Result<()> {
    let helix_client: HelixClient<reqwest::Client> = HelixClient::default();
    let token = generate_token(&config).await?;

    let app = App {
        helix_client,
        token: Arc::new(token),
        users: Arc::new(DashMap::new()),
        logs: logs.clone(),
        config: Arc::new(config),
    };

    let login_credentials = StaticLoginCredentials::anonymous();
    let bot_handle = tokio::spawn(bot::run(login_credentials, app.clone()));
    let web_handle = tokio::spawn(web::run(app));

    try_join!(bot_handle, web_handle)?;

    Ok(())
}

async fn reindex(config: Config, logs: Logs, mut channels: Vec<String>) -> anyhow::Result<()> {
    let helix_client: HelixClient<reqwest::Client> = HelixClient::default();
    let token = generate_token(&config).await?;

    let app = App {
        helix_client,
        token: Arc::new(token),
        users: Arc::new(DashMap::new()),
        config: Arc::new(config),
        logs,
    };

    populate_channel_list(&mut channels, &app).await?;

    reindexer::run(app, &channels).await
}

async fn print_index(file_path: PathBuf) -> anyhow::Result<()> {
    let mut file = File::open(&file_path).await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;

    for bytes in buf.chunks_exact(index::SIZE) {
        let index = Index::from_bytes(bytes)?;
        info!("Index: {index:?}");
    }

    Ok(())
}

async fn migrate(config: Config, logs: Logs, source_logs_path: String) -> anyhow::Result<()> {
    let helix_client: HelixClient<reqwest::Client> = HelixClient::default();
    let token = generate_token(&config).await?;

    let app = App {
        helix_client,
        token: Arc::new(token),
        users: Arc::new(DashMap::new()),
        logs,
        config: Arc::new(config),
    };

    let migrator = Migrator::new(app, &source_logs_path).await?;
    migrator.run().await
}

async fn generate_token(config: &Config) -> anyhow::Result<AppAccessToken> {
    let helix_client: HelixClient<reqwest::Client> = HelixClient::default();
    let token = AppAccessToken::get_app_access_token(
        &helix_client,
        config.client_id.clone().into(),
        config.client_secret.clone().into(),
        Scope::all(),
    )
    .await?;
    info!("Generated new app token");

    Ok(token)
}

async fn populate_channel_list(channels: &mut Vec<String>, app: &App<'_>) -> anyhow::Result<()> {
    if channels.is_empty() {
        let mut dir = read_dir(&*app.logs.root_path).await?;

        while let Some(entry) = dir.next_entry().await? {
            if entry.metadata().await?.is_dir() {
                let channel = entry
                    .file_name()
                    .to_str()
                    .expect("invalid folder name")
                    .to_owned();
                channels.push(channel);
            }
        }
    }
    Ok(())
}
