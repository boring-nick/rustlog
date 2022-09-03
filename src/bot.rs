use crate::{
    app::App,
    config::Config,
    logs::{
        extract_channel_and_user,
        schema::{ChannelIdentifier, UserIdentifier},
    },
};
use tracing::{error, info};
use twitch_irc::{
    login::LoginCredentials, message::ServerMessage, ClientConfig, SecureTCPTransport,
    TwitchIRCClient,
};

pub async fn run<C: LoginCredentials>(login_credentials: C, app: App<'_>, config: Config) {
    let client_config = ClientConfig::new_simple(login_credentials);
    let (mut receiver, client) = TwitchIRCClient::<SecureTCPTransport, C>::new(client_config);

    match app.get_users(config.channels.clone(), vec![]).await {
        Ok(users) => {
            for (_, channel_login) in users {
                info!("Logging channel {channel_login}");
                client.join(channel_login).expect("Failed to join channel");
            }

            while let Some(msg) = receiver.recv().await {
                if let Err(e) = write_message(msg, &app).await {
                    error!("Could not write log: {e}");
                }
            }
        }
        Err(err) => {
            error!("Could not fetch channel list: {err}");
        }
    }
}

async fn write_message(msg: ServerMessage, app: &App<'_>) -> anyhow::Result<()> {
    if let Some((channel, maybe_user)) = extract_channel_and_user(&msg) {
        let channel_id = match channel {
            ChannelIdentifier::Channel(name) => app.get_user_id_by_name(name).await?,
            ChannelIdentifier::ChannelId(id) => id.to_owned(),
        };
        let maybe_user_id = if let Some(user) = maybe_user {
            Some(match user {
                UserIdentifier::User(name) => app.get_user_id_by_name(name).await?,
                UserIdentifier::UserId(id) => id.to_owned(),
            })
        } else {
            None
        };

        app.logs
            .write_server_message(msg, &channel_id, maybe_user_id.as_deref())
            .await?;
    }
    Ok(())
}
