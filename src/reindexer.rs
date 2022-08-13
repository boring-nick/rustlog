use crate::logs::{
    extract_channel_and_user, get_channel_path, get_users_path, offsets::Offset, Logs,
};
use tokio::{
    fs::{self, File},
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader},
};
use tracing::{debug, error, info, warn};
use twitch_irc::message::{IRCMessage, ServerMessage};

pub async fn run(logs: Logs, channels: &[String]) -> anyhow::Result<()> {
    for channel_id in channels {
        let available_logs = logs.get_available_channel_logs(channel_id).await?;

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
                    fs::remove_dir_all(&users_path).await?;
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

                    let file = File::open(channel_file_path).await?;
                    let mut reader = BufReader::new(file);
                    let mut cursor = 0;

                    let mut line = String::with_capacity(50);

                    while reader.read_line(&mut line).await? != 0 {
                        match IRCMessage::parse(line.trim_end())
                            .map_err(|err| err.to_string())
                            .and_then(|irc_msg| {
                                ServerMessage::try_from(irc_msg).map_err(|err| err.to_string())
                            }) {
                            Ok(server_msg) => {
                                if let Some((_, user_id)) = extract_channel_and_user(&server_msg) {
                                    let start_pos = cursor;
                                    cursor = reader.stream_position().await?;
                                    let end_pos = cursor;

                                    let offset = Offset {
                                        day,
                                        start_pos,
                                        end_pos,
                                    };

                                    debug!("Writing offset {offset:?} for {user_id}");

                                    logs.write_user_log(
                                        channel_id,
                                        user_id,
                                        &year.to_string(),
                                        &month.to_string(),
                                        offset,
                                    )
                                    .await?;
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

    info!("Reindex finished");

    Ok(())
}
