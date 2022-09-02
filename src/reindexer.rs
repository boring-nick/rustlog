use crate::logs::{extract_channel_and_user, get_channel_path, get_users_path, index::Index, Logs};
use anyhow::Context;
use tokio::{
    fs::{self, File},
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader},
};
use tracing::{debug, error, info, warn};
use twitch_irc::message::{IRCMessage, ServerMessage};

pub async fn run(logs: Logs, channels: &[String]) -> anyhow::Result<()> {
    for channel_id in channels {
        match logs.get_available_channel_logs(channel_id).await {
            Ok(available_logs) => {
                for (year, months) in available_logs {
                    for (month, days) in months {
                        let users_path = get_users_path(
                            &logs.root_path,
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
                                &logs.root_path,
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
                                        if let Some((_, user_id)) =
                                            extract_channel_and_user(&server_msg)
                                        {
                                            let index = Index {
                                                day,
                                                offset,
                                                len: read_bytes as u32 - 1, // exclude newline
                                            };

                                            debug!("Writing index {index:?} for {user_id}");

                                            logs.write_user_log(
                                                channel_id,
                                                user_id,
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
                            info!("Finished day");
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
