use crate::{app::App, logs::extract_channel_and_user_from_raw};
use anyhow::anyhow;
use tracing::{debug, error, info, trace};
use twitch_irc::{
    login::LoginCredentials,
    message::{AsRawIRC, IRCMessage, ServerMessage},
    ClientConfig, SecureTCPTransport, TwitchIRCClient,
};

type TwitchClient<C> = TwitchIRCClient<SecureTCPTransport, C>;

const COMMAND_PREFIX: &str = "!rustlog ";

pub async fn run<C: LoginCredentials>(login_credentials: C, app: App<'_>) {
    let bot = Bot::new(app);
    bot.run(login_credentials).await;
}

struct Bot<'a> {
    app: App<'a>,
}

impl<'a> Bot<'a> {
    pub fn new(app: App<'a>) -> Bot<'a> {
        Self { app }
    }

    pub async fn run<C: LoginCredentials>(self, login_credentials: C) {
        let client_config = ClientConfig::new_simple(login_credentials);
        let (mut receiver, client) = TwitchIRCClient::<SecureTCPTransport, C>::new(client_config);

        let channel_ids = self.app.config.channels.read().unwrap().clone();

        match self
            .app
            .get_users(Vec::from_iter(channel_ids), vec![])
            .await
        {
            Ok(users) => {
                for channel_login in users.into_values() {
                    info!("Logging channel {channel_login}");
                    client.join(channel_login).expect("Failed to join channel");
                }

                while let Some(msg) = receiver.recv().await {
                    if let Err(e) = self.handle_message(msg, &client).await {
                        error!("Could not handle message: {e}");
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
            self.app
                .logs
                .write_message(irc_message.as_raw_irc(), &channel_id, maybe_user_id)
                .await?;
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
