mod app;
mod args;
mod bot;
mod config;
mod db;
mod error;
mod logs;
mod migrator;
mod web;

pub type Result<T> = std::result::Result<T, error::Error>;

use anyhow::Context;
use app::App;
use args::{Args, Command};
use clap::Parser;
use config::Config;
use dashmap::DashMap;
use db::{setup_db, writer::create_writer};
use migrator::Migrator;
use std::sync::Arc;
use tokio::try_join;
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
    let mut db = clickhouse::Client::default()
        .with_url(&config.clickhouse_url)
        .with_database(&config.clickhouse_db);

    if let Some(user) = &config.clickhouse_username {
        db = db.with_user(user);
    }

    if let Some(password) = &config.clickhouse_password {
        db = db.with_user(password);
    }

    let args = Args::parse();

    setup_db(&db).await.context("Could not run DB migrations")?;

    match args.subcommand {
        None => run(config, db).await,
        Some(Command::Migrate { source_dir }) => {
            migrate(db, config.clickhouse_write_batch_size, source_dir).await
        }
    }
}

async fn run(config: Config, db: clickhouse::Client) -> anyhow::Result<()> {
    let helix_client: HelixClient<reqwest::Client> = HelixClient::default();
    let token = generate_token(&config).await?;

    let writer_tx = create_writer(&db).await?;

    let app = App {
        helix_client,
        token: Arc::new(token),
        users: Arc::new(DashMap::new()),
        config: Arc::new(config),
        db: Arc::new(db),
    };

    let login_credentials = StaticLoginCredentials::anonymous();
    let bot_handle = tokio::spawn(bot::run(login_credentials, app.clone(), writer_tx));
    let web_handle = tokio::spawn(web::run(app));

    try_join!(bot_handle, web_handle)?;

    Ok(())
}

async fn migrate(
    db: clickhouse::Client,
    batch_size: usize,
    source_logs_path: String,
) -> anyhow::Result<()> {
    let migrator = Migrator::new(db, batch_size, &source_logs_path).await?;
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

/*async fn populate_channel_list(channels: &mut Vec<String>, app: &App<'_>) -> anyhow::Result<()> {
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
}*/
