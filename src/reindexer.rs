use crate::{
    app::App,
    logs::{
        extract_channel_and_user, get_channel_path, get_users_path,
        index::Index,
        schema::{ChannelLogDateMap, UserIdentifier},
    },
};
use anyhow::Context;
use chrono::{TimeZone, Utc};
use std::{
    collections::{hash_map::Entry, HashMap},
    convert::{TryFrom, TryInto},
    io::{BufRead, BufWriter, Seek, Write},
    time::Instant,
};
use std::{
    fs::{self, File, OpenOptions},
    io::BufReader,
};
use tracing::{debug, error, info, trace, warn};
use twitch_irc::message::{IRCMessage, ServerMessage};

pub async fn run(app: App<'_>, channels: &[String]) -> anyhow::Result<()> {
    info!("Reindexing all channels");

    let started_at = Instant::now();

    for channel_id in channels {
        match app.logs.get_available_channel_logs(channel_id, false).await {
            Ok(available_logs) => {
                reindex_channel(&app, &available_logs, channel_id).await?;
            }
            Err(err) => {
                error!("Could not get logs for channel {channel_id}: {err}");
            }
        }
    }

    let elapsed = started_at.elapsed();
    info!("Reindex finished in {elapsed:?}");

    Ok(())
}

pub async fn reindex_channel(
    app: &App<'_>,
    available_logs: &ChannelLogDateMap,
    channel_id: &str,
) -> anyhow::Result<()> {
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
                fs::remove_dir_all(&users_path).context("could not clear old indexes")?;
            }

            for day in days {
                info!("Reindexing channel {channel_id} date {year}-{month}-{day}");

                let date = Utc.ymd((*year).try_into().unwrap(), *month, *day);

                let channel_file_path =
                    get_channel_path(&app.logs.root_path, channel_id, date, false);

                let file = File::open(channel_file_path).context("could not open channel file")?;
                let mut reader = BufReader::new(file);
                let mut offset;

                let mut line = String::with_capacity(50);

                let mut user_writers = HashMap::new();

                loop {
                    offset = reader.stream_position()?;
                    let read_bytes = reader.read_line(&mut line)?;
                    if read_bytes == 0 {
                        break;
                    }

                    match IRCMessage::parse(line.trim_end()) {
                        Ok(irc_message) => {
                            let maybe_user_id =
                                if let Some(user_id) = irc_message.tags.0.get("user-id") {
                                    user_id.clone()
                                } else {
                                    match ServerMessage::try_from(irc_message) {
                                        Ok(server_msg) => {
                                            if let Some((_, Some(user))) =
                                                extract_channel_and_user(&server_msg)
                                            {
                                                Some(match user {
                                                    UserIdentifier::UserId(id) => id.to_owned(),
                                                    UserIdentifier::User(name) => {
                                                        app.get_user_id_by_name(name).await?
                                                    }
                                                })
                                            } else {
                                                None
                                            }
                                        }
                                        Err(err) => {
                                            debug!("{err}");
                                            None
                                        }
                                    }
                                };

                            if let Some(user_id) = maybe_user_id {
                                let index = Index {
                                    day: *day,
                                    offset,
                                    len: read_bytes as u32 - 1, // exclude newline
                                };

                                debug!("Writing index {index:?} for {user_id}");

                                let user_writer = match user_writers.entry(user_id.clone()) {
                                    Entry::Occupied(occupied) => occupied.into_mut(),
                                    Entry::Vacant(vacant) => {
                                        let users_folder = get_users_path(
                                            &app.logs.root_path,
                                            channel_id,
                                            &year.to_string(),
                                            &month.to_string(),
                                        );
                                        if !users_folder.exists() {
                                            fs::create_dir_all(&users_folder)?;
                                        }

                                        let user_file_path =
                                            users_folder.join(format!("{user_id}.indexes"));
                                        trace!("Opening user file at {user_file_path:?}");

                                        let user_file = OpenOptions::new()
                                            .create(true)
                                            .append(true)
                                            .open(user_file_path)?;
                                        let writer = BufWriter::new(user_file);

                                        vacant.insert(writer)
                                    }
                                };

                                user_writer
                                    .write_all(&index.as_bytes())
                                    .context("Could not write to user log")?;
                            } else {
                                debug!("Skipping message {line:?}");
                            }
                        }
                        Err(err) => warn!("Malfromed message: {line} {err}"),
                    }
                    line.clear();
                }

                for mut writer in user_writers.into_values() {
                    writer.flush()?;
                }
            }
        }
    }

    Ok(())
}
