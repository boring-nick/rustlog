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
pub type ShutdownRx = watch::Receiver<()>;

use anyhow::{anyhow, Context};
use app::App;
use args::{Args, Command};
use clap::Parser;
use config::Config;
use db::{setup_db, writer::create_writer};
use futures::{future::try_join_all, stream::FuturesUnordered, StreamExt};
use migrator::Migrator;
use mimalloc::MiMalloc;
use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::{mpsc, watch},
    time::timeout,
};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
use twitch_api::{
    twitch_oauth2::{AppAccessToken, Scope},
    HelixClient,
};
use twitch_irc::login::StaticLoginCredentials;

use crate::app::cache::UsersCache;

const SHUTDOWN_TIMEOUT_SECONDS: u64 = 8;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let use_ansi = env::var("RUST_LOG_ANSI")
        .ok()
        .and_then(|ansi| ansi.parse().ok())
        .unwrap_or(true);
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_ansi(use_ansi)
        .init();

    let config = Config::load()?;
    let mut db = clickhouse::Client::default()
        .with_url(&config.clickhouse_url)
        .with_database(&config.clickhouse_db)
        .with_compression(clickhouse::Compression::None);

    if let Some(user) = &config.clickhouse_username {
        db = db.with_user(user);
    }

    if let Some(password) = &config.clickhouse_password {
        db = db.with_password(password);
    }

    let args = Args::parse();

    setup_db(&db, &config.clickhouse_db)
        .await
        .context("Could not run DB migrations")?;

    match args.subcommand {
        None => run(config, db).await,
        Some(Command::Migrate {
            source_dir,
            channel_id,
            jobs,
        }) => migrate(db, source_dir, channel_id, jobs).await,
    }
}

async fn run(config: Config, db: clickhouse::Client) -> anyhow::Result<()> {
    let mut shutdown_rx = listen_shutdown().await;

    let helix_client: HelixClient<reqwest::Client> = HelixClient::default();
    let token = generate_token(&config).await?;

    let (writer_tx, flush_buffer, mut writer_handle) = create_writer(
        db.clone(),
        shutdown_rx.clone(),
        config.clickhouse_flush_interval,
    )
    .await?;

    let app = App {
        helix_client,
        token: Arc::new(token),
        users: UsersCache::default(),
        config: Arc::new(config),
        db: Arc::new(db),
        optout_codes: Arc::default(),
        flush_buffer,
    };

    let (bot_tx, bot_rx) = mpsc::channel(1);

    let login_credentials = StaticLoginCredentials::anonymous();
    let mut bot_handle = tokio::spawn(bot::run(
        login_credentials,
        app.clone(),
        writer_tx,
        shutdown_rx.clone(),
        bot_rx,
    ));
    let mut web_handle = tokio::spawn(web::run(app, shutdown_rx.clone(), bot_tx));

    tokio::select! {
        _ = shutdown_rx.changed() => {
            debug!("Waiting for tasks to shut down");

            let started_at = Instant::now();

            let shutdown_future = try_join_all([bot_handle, web_handle, writer_handle]);
            match timeout(Duration::from_secs(SHUTDOWN_TIMEOUT_SECONDS), shutdown_future).await {
                Ok(Ok(_)) => {
                    debug!("Cleanup finished in {}ms", started_at.elapsed().as_millis());
                    Ok(())
                }
                Ok(Err(err)) => Err(anyhow!("Could not shut down properly: {err}")),
                Err(_) => {
                    Err(anyhow!("Tasks did not shut down after {} seconds", SHUTDOWN_TIMEOUT_SECONDS))
                }
            }

        }
        _ = &mut bot_handle => {
            Err(anyhow!("Bot task exited unexpectedly"))
        }
        _ = &mut web_handle => {
            Err(anyhow!("Web task exited unexpectedly"))
        }
        _ = &mut writer_handle => {
            Err(anyhow!("Writer task exited unexpectedly"))
        }
    }
}

async fn migrate(
    db: clickhouse::Client,
    source_logs_path: String,
    channel_ids: Vec<String>,
    jobs: usize,
) -> anyhow::Result<()> {
    let migrator = Migrator::new(db, source_logs_path, channel_ids).await?;
    migrator.run(jobs).await
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

async fn listen_shutdown() -> watch::Receiver<()> {
    let shutdown_signals = [SignalKind::interrupt(), SignalKind::terminate()];
    let mut futures = FuturesUnordered::new();

    for signal_kind in shutdown_signals {
        let mut listener = signal(signal_kind).unwrap();
        futures.push(async move {
            listener.recv().await;
            signal_kind
        });
    }

    let (tx, rx) = watch::channel(());

    tokio::spawn(async move {
        futures.next().await;
        info!("Received shutdown signal");
        tx.send(()).unwrap();
    });

    rx
}
