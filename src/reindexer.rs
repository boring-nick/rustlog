use crate::{
    app::App,
    logs::{
        extract_channel_and_user, get_channel_path, get_users_path, index::Index,
        schema::UserIdentifier,
    },
};
use anyhow::Context;
use tokio::{
    fs::{self, File},
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader},
};
use tracing::{debug, error, info, warn};
use twitch_irc::message::{IRCMessage, ServerMessage};

pub async fn run(app: App<'_>, channels: &[String]) -> anyhow::Result<()> {
    for channel_id in channels {
        match app.logs.get_available_channel_logs(channel_id).await {
            Ok(available_logs) => {
                for (year, months) in available_logs {
                    for (month, days) in months {
                        let users_path = get_users_path(
                            &app.logs.root_path,
                            channel_id,
                            &year.to_string(),
                            &month.to_string(),
                        );

                        if users_path.exists() {
                            info!("Clearing existing indexes in channel {channel_id} for {year}-{month}");
                            fs::remove_dir_all(&users_path)
                                .await
                                .context("could not clear old indexes")?;
                        }

                        for day in days {
                            info!("Reindexing channel {channel_id} date {year}-{month}-{day}");

                            let channel_file_path = get_channel_path(
                                &app.logs.root_path,
                                channel_id,
                                &year.to_string(),
                                &month.to_string(),
                                &day.to_string(),
                            );

                            let file = File::open(channel_file_path)
                                .await
                                .context("could not open channel file")?;
                            let mut reader = BufReader::new(file);
                            let mut offset;

                            let mut line = String::with_capacity(50);

                            loop {
                                offset = reader.stream_position().await?;
                                let read_bytes = reader.read_line(&mut line).await?;
                                if read_bytes == 0 {
                                    break;
                                }

                                match IRCMessage::parse(line.trim_end())
                                    .map_err(|err| err.to_string())
                                    .and_then(|irc_msg| {
                                        ServerMessage::try_from(irc_msg)
                                            .map_err(|err| err.to_string())
                                    }) {
                                    Ok(server_msg) => {
                                        if let Some((_, Some(user))) =
                                            extract_channel_and_user(&server_msg)
                                        {
                                            let user_id = match user {
                                                UserIdentifier::UserId(id) => id.to_owned(),
                                                UserIdentifier::User(name) => {
                                                    app.get_user_id_by_name(name).await?
                                                }
                                            };

                                            let index = Index {
                                                day,
                                                offset,
                                                len: read_bytes as u32 - 1, // exclude newline
                                            };

                                            debug!("Writing index {index:?} for {user_id}");

                                            app.logs
                                                .write_user_log(
                                                    channel_id,
                                                    &user_id,
                                                    &year.to_string(),
                                                    &month.to_string(),
                                                    index,
                                                )
                                                .await
                                                .context("could not write user log")?;
                                        } else {
                                            warn!("Unexpected log entry {line}, ignoring");
                                        }
                                    }
                                    Err(err) => error!("Malformed message {line}: {err}"),
                                }

                                line.clear();
                            }
                        }
                    }
                }
            }
            Err(err) => {
                warn!("Could not get logs for channel {channel_id}: {err}");
            }
        }
    }

    info!("Reindex finished");

    Ok(())
}
