mod app;
mod bot;
mod config;
mod error;
mod logs;
mod reindexer;
mod web;

pub type Result<T> = std::result::Result<T, error::Error>;

use anyhow::anyhow;
use app::App;
use clap::{Parser, Subcommand};
use config::Config;
use dashmap::DashMap;
use logs::{offsets::Offset, Logs};
use std::{path::PathBuf, sync::Arc};
use tokio::{fs::File, io::BufReader, try_join};
use tracing::info;
use tracing_subscriber::EnvFilter;
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
        // .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
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
    };

    let login_credentials = StaticLoginCredentials::anonymous();
    let bot_handle = tokio::spawn(bot::run(login_credentials, app.clone(), config.clone()));

    let web_handle = tokio::spawn(web::run(config, app));

    try_join!(bot_handle, web_handle)?;

    Ok(())
}

async fn reindex(config: Config, logs: Logs, mut channels: Vec<String>) -> anyhow::Result<()> {
    if channels.is_empty() {
        channels = config.channels;
        info!("Reindexing all channels");
    } else {
        for channel in &channels {
            if !config.channels.contains(channel) {
                return Err(anyhow!("unknown channel: {channel}"));
            }
        }
        info!("Reindexing channels: {channels:?}");
    }

    reindexer::run(logs, &channels).await
}

async fn print_index(file_path: PathBuf) -> anyhow::Result<()> {
    let file = File::open(&file_path).await?;
    let mut reader = BufReader::new(file);

    while let Some(offset) = Offset::read_one(&mut reader).await? {
        info!("Offset: {offset:?}");
    }

    Ok(())
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

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    subcommand: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Rebuild user indexes
    Reindex {
        /// Comma-separated list of channels to reindex
        #[clap(short, long, value_parser)]
        channels: Option<String>,
    },
    /// Print index
    PrintIndex { file_path: PathBuf },
}
