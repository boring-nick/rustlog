use std::borrow::Cow;

use crate::{
    app::App,
    db::schema::Message,
    logs::{extract_channel_and_user_from_raw, extract_timestamp},
    ShutdownRx,
};
use anyhow::anyhow;
use chrono::Utc;
use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec, IntCounterVec};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info, trace};
use twitch_irc::{
    login::LoginCredentials,
    message::{AsRawIRC, IRCMessage, ServerMessage},
    ClientConfig, SecureTCPTransport, TwitchIRCClient,
};

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
    app: App<'_>,
    writer_tx: Sender<Message<'static>>,
    shutdown_rx: ShutdownRx,
) {
    let bot = Bot::new(app, writer_tx);
    bot.run(login_credentials, shutdown_rx).await;
}

struct Bot<'a> {
    app: App<'a>,
    writer_tx: Sender<Message<'static>>,
}

impl<'a> Bot<'a> {
    pub fn new(app: App<'a>, writer_tx: Sender<Message<'static>>) -> Bot<'a> {
        Self { app, writer_tx }
    }

    pub async fn run<C: LoginCredentials>(self, login_credentials: C, mut shutdown_rx: ShutdownRx) {
        let client_config = ClientConfig::new_simple(login_credentials);
        let (mut receiver, client) = TwitchIRCClient::<SecureTCPTransport, C>::new(client_config);

        let channel_ids = self.app.config.channels.read().unwrap().clone();

        match self
            .app
            .get_users(Vec::from_iter(channel_ids), vec![])
            .await
        {
            Ok(users) => {
                info!("Joining {} channels", users.len());
                for channel_login in users.into_values() {
                    info!("Logging channel {channel_login}");
                    client.join(channel_login).expect("Failed to join channel");
                }

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
            Err(err) => {
                error!("Could not fetch channel list: {err}");
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
                    self.handle_command(cmd, client).await?;
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
        let irc_message = IRCMessage::from(msg);

        if let Some((channel_id, maybe_user_id)) = extract_channel_and_user_from_raw(&irc_message) {
            MESSAGES_RECEIVED_COUNTERS
                .with_label_values(&[channel_id])
                .inc();

            let timestamp = extract_timestamp(&irc_message)
                .unwrap_or_else(|| Utc::now().timestamp_millis().try_into().unwrap());
            let user_id = maybe_user_id.unwrap_or_default().to_owned();

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
                _ => (),
            }
        }

        Ok(())
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

        self.app.config.save().await?;

        Ok(())
    }
}

enum ChannelAction {
    Join,
    Part,
}
