use crate::{
    app::App,
    db::{delete_user_logs, schema::Message},
    logs::extract::{extract_channel_and_user_from_raw, extract_raw_timestamp},
    ShutdownRx,
};
use anyhow::{anyhow, Context};
use chrono::Utc;
use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec, IntCounterVec};
use std::{borrow::Cow, time::Duration};
use tokio::{sync::mpsc::Sender, time::sleep};
use tracing::{debug, error, info, trace};
use twitch_irc::{
    login::LoginCredentials,
    message::{AsRawIRC, IRCMessage, ServerMessage},
    ClientConfig, SecureTCPTransport, TwitchIRCClient,
};

const CHANNEL_REJOIN_INTERVAL_SECONDS: u64 = 3600;
const CHANENLS_REFETCH_RETRY_INTERVAL_SECONDS: u64 = 5;

type TwitchClient<C> = TwitchIRCClient<SecureTCPTransport, C>;

lazy_static! {
    static ref MESSAGES_RECEIVED_COUNTERS: IntCounterVec = register_int_counter_vec!(
        "rustlog_messages_received",
        "How many messages were written",
        &["channel_id"]
    )
    .unwrap();
}

const COMMAND_PREFIX: &str = "!rustlog ";

pub async fn run<C: LoginCredentials>(
    login_credentials: C,
    app: App<'static>,
    writer_tx: Sender<Message<'static>>,
    shutdown_rx: ShutdownRx,
) {
    let bot = Bot::new(app, writer_tx);
    bot.run(login_credentials, shutdown_rx).await;
}

struct Bot {
    app: App<'static>,
    writer_tx: Sender<Message<'static>>,
}

impl Bot {
    pub fn new(app: App<'static>, writer_tx: Sender<Message<'static>>) -> Bot {
        Self { app, writer_tx }
    }

    pub async fn run<C: LoginCredentials>(self, login_credentials: C, mut shutdown_rx: ShutdownRx) {
        let client_config = ClientConfig::new_simple(login_credentials);
        let (mut receiver, client) = TwitchIRCClient::<SecureTCPTransport, C>::new(client_config);

        let app = self.app.clone();
        let join_client = client.clone();
        tokio::spawn(async move {
            loop {
                let channel_ids = app.config.channels.read().unwrap().clone();

                let interval = match app.get_users(Vec::from_iter(channel_ids), vec![]).await {
                    Ok(users) => {
                        info!("Joining {} channels", users.len());
                        for channel_login in users.into_values() {
                            debug!("Logging channel {channel_login}");
                            join_client
                                .join(channel_login)
                                .expect("Failed to join channel");
                        }
                        CHANNEL_REJOIN_INTERVAL_SECONDS
                    }
                    Err(err) => {
                        error!("Could not fetch users list: {err}");
                        CHANENLS_REFETCH_RETRY_INTERVAL_SECONDS
                    }
                };
                sleep(Duration::from_secs(interval)).await;
            }
        });

        loop {
            tokio::select! {
                Some(msg) = receiver.recv() => {
                    if let Err(e) = self.handle_message(msg, &client).await {
                        error!("Could not handle message: {e}");
                    }
                }
                _ = shutdown_rx.changed() => {
                    debug!("Shutting down bot task");
                    break;
                }
            }
        }
    }

    async fn handle_message<C: LoginCredentials>(
        &self,
        msg: ServerMessage,
        client: &TwitchClient<C>,
    ) -> anyhow::Result<()> {
        if let ServerMessage::Privmsg(privmsg) = &msg {
            trace!("Processing message {}", privmsg.message_text);
            if let Some(cmd) = privmsg.message_text.strip_prefix(COMMAND_PREFIX) {
                if self.app.config.admins.contains(&privmsg.sender.login) {
                    self.handle_command(cmd, client, &privmsg.sender.id).await?;
                } else {
                    info!(
                        "User {} is not an admin to use commands",
                        privmsg.sender.login
                    );
                }
            }
        }

        self.write_message(msg).await?;

        Ok(())
    }

    async fn write_message(&self, msg: ServerMessage) -> anyhow::Result<()> {
        // Ignore
        if matches!(msg, ServerMessage::RoomState(_)) {
            return Ok(());
        }

        let irc_message = IRCMessage::from(msg);

        if let Some((channel_id, maybe_user_id)) = extract_channel_and_user_from_raw(&irc_message) {
            if !channel_id.is_empty() {
                MESSAGES_RECEIVED_COUNTERS
                    .with_label_values(&[channel_id])
                    .inc();
            }

            let timestamp = extract_raw_timestamp(&irc_message)
                .unwrap_or_else(|| Utc::now().timestamp_millis().try_into().unwrap());
            let user_id = maybe_user_id.unwrap_or_default().to_owned();

            if self.app.config.opt_out.contains_key(&user_id) {
                return Ok(());
            }

            let message = Message {
                channel_id: Cow::Owned(channel_id.to_owned()),
                user_id: Cow::Owned(user_id),
                timestamp,
                raw: Cow::Owned(irc_message.as_raw_irc()),
            };
            self.writer_tx.send(message).await?;
        }

        Ok(())
    }

    async fn handle_command<C: LoginCredentials>(
        &self,
        cmd: &str,
        client: &TwitchClient<C>,
        sender_id: &str,
    ) -> anyhow::Result<()> {
        debug!("Processing command {cmd}");
        let mut split = cmd.split_whitespace();
        if let Some(action) = split.next() {
            let args: Vec<&str> = split.collect();

            match action {
                "join" => {
                    self.update_channels(client, &args, ChannelAction::Join)
                        .await?
                }
                "leave" | "part" => {
                    self.update_channels(client, &args, ChannelAction::Part)
                        .await?
                }
                "optout" => {
                    self.optout_user(&args, sender_id).await?;
                }
                _ => (),
            }
        }

        Ok(())
    }

    async fn optout_user(&self, args: &[&str], sender_id: &str) -> anyhow::Result<()> {
        let code = args.first().context("No optout code provided")?;
        if self.app.optout_codes.remove(*code).is_some() {
            delete_user_logs(&self.app.db, sender_id)
                .await
                .context("Could not delete logs")?;

            self.app.config.opt_out.insert(sender_id.to_owned(), true);
            self.app.config.save()?;
            info!("User {sender_id} opted out");
            Ok(())
        } else {
            Err(anyhow!("Invalid optout code"))
        }
    }

    async fn update_channels<C: LoginCredentials>(
        &self,
        client: &TwitchClient<C>,
        channels: &[&str],
        action: ChannelAction,
    ) -> anyhow::Result<()> {
        if channels.is_empty() {
            return Err(anyhow!("no channels specified"));
        }

        let channels = self
            .app
            .get_users(vec![], channels.iter().map(ToString::to_string).collect())
            .await?;

        {
            let mut config_channels = self.app.config.channels.write().unwrap();

            for (channel_id, channel_name) in channels {
                match action {
                    ChannelAction::Join => {
                        info!("Joining channel {channel_name}");
                        config_channels.insert(channel_id);
                        client.join(channel_name)?;
                    }
                    ChannelAction::Part => {
                        info!("Parting channel {channel_name}");
                        config_channels.remove(&channel_id);
                        client.part(channel_name);
                    }
                }
            }
        }

        self.app.config.save()?;

        Ok(())
    }
}

enum ChannelAction {
    Join,
    Part,
}
