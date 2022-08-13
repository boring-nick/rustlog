use crate::{app::App, config::Config};
use tracing::{error, trace};
use twitch_irc::{login::LoginCredentials, ClientConfig, SecureTCPTransport, TwitchIRCClient};

pub async fn run<C: LoginCredentials>(login_credentials: C, app: App<'_>, config: Config) {
    let client_config = ClientConfig::new_simple(login_credentials);
    let (mut receiver, client) = TwitchIRCClient::<SecureTCPTransport, C>::new(client_config);

    match app.get_users(config.channels.clone(), vec![]).await {
        Ok(users) => {
            for (_, channel_login) in users {
                client.join(channel_login).expect("Failed to join channel");
            }

            while let Some(msg) = receiver.recv().await {
                trace!("Received message {msg:?}");
                if let Err(err) = app.logs.write_server_message(msg).await {
                    error!("Could not log message: {err}");
                }
            }
        }
        Err(err) => {
            error!("Could not fetch channel list: {err}");
        }
    }
}
