// pub mod offsets;
// pub mod formatter;
pub mod index;
pub mod schema;

use self::schema::{
    ChannelIdentifier, ChannelLogDate, ChannelLogDateMap, UserIdentifier, UserLogDateMap,
};
use crate::{error::Error, logs::index::Index, Result};
use chrono::{Date, Datelike, Utc};
use itertools::Itertools;
use std::{
    collections::{BTreeMap, HashMap},
    io::SeekFrom,
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    fs::{self, read_dir, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
};
use tracing::{debug, trace};
use twitch_irc::message::{AsRawIRC, ClearChatAction, ServerMessage};

#[derive(Debug, Clone)]
pub struct Logs {
    pub root_path: Arc<PathBuf>,
}

impl Logs {
    pub async fn new(logs_path: &str) -> Result<Self> {
        let root_folder = PathBuf::from(logs_path);

        if !root_folder.exists() {
            fs::create_dir_all(&root_folder).await?;
        }

        Ok(Self {
            root_path: Arc::new(root_folder),
        })
    }

    pub async fn write_server_message(
        &self,
        msg: ServerMessage,
        channel_id: &str,
        maybe_user_id: Option<&str>,
    ) -> Result<()> {
        trace!("Logging message {msg:?}");

        let today = Utc::today();

        let day_folder = get_day_folder(&self.root_path, channel_id, today);
        if !day_folder.exists() {
            fs::create_dir_all(&day_folder).await?;
        }

        let channel_file_path = day_folder.join("channel.txt");
        trace!("Opening channel file at {channel_file_path:?}");

        let channel_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(channel_file_path)
            .await?;
        let mut channel_writer = BufWriter::new(channel_file);

        let offset = channel_writer.seek(SeekFrom::End(0)).await?;

        let raw_msg = msg.as_raw_irc();
        let msg_bytes = raw_msg.as_bytes();

        channel_writer.write_all(msg_bytes).await?;
        channel_writer.write_all(b"\n").await?;
        channel_writer.flush().await?;

        if let Some(user_id) = maybe_user_id {
            let index = Index {
                day: today.day(),
                offset,
                len: raw_msg.len().try_into().unwrap(),
            };

            self.write_user_log(
                channel_id,
                &user_id,
                &today.year().to_string(),
                &today.month().to_string(),
                index,
            )
            .await?;
        }

        Ok(())
    }

    pub async fn write_user_log(
        &self,
        channel_id: &str,
        user_id: &str,
        year: &str,
        month: &str,
        index: Index,
    ) -> Result<()> {
        let users_folder = get_users_path(&self.root_path, channel_id, year, month);
        if !users_folder.exists() {
            fs::create_dir_all(&users_folder).await?;
        }

        let user_file_path = users_folder.join(format!("{user_id}.indexes"));
        trace!("Opening user file at {user_file_path:?}");

        let mut user_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(user_file_path)
            .await?;

        user_file.write_all(&index.as_bytes()).await?;
        user_file.flush().await?;

        Ok(())
    }

    pub async fn get_available_channel_logs(&self, channel_id: &str) -> Result<ChannelLogDateMap> {
        let channel_path = self.root_path.join(channel_id);
        let mut channel_dir = read_dir(channel_path).await?;

        let mut years = BTreeMap::new();

        while let Some(year_entry) = channel_dir.next_entry().await? {
            if year_entry.metadata().await?.is_dir() {
                let mut year_dir = read_dir(year_entry.path()).await?;
                let mut months = BTreeMap::new();

                while let Some(month_entry) = year_dir.next_entry().await? {
                    if month_entry.metadata().await?.is_dir() {
                        let mut month_dir = read_dir(month_entry.path()).await?;
                        let mut days = Vec::new();

                        while let Some(day_entry) = month_dir.next_entry().await? {
                            if day_entry.metadata().await?.is_dir() {
                                let channel_file_path = day_entry.path().join("channel.txt");

                                if let Ok(metadata) = fs::metadata(channel_file_path).await {
                                    if metadata.is_file() {
                                        let day = day_entry
                                            .file_name()
                                            .to_str()
                                            .and_then(|name| name.parse().ok())
                                            .expect("invalid log entry name");

                                        days.push(day);
                                    }
                                }
                            }
                        }

                        days.sort_unstable();

                        let month = month_entry
                            .file_name()
                            .to_str()
                            .and_then(|name| name.parse().ok())
                            .expect("invalid log entry name");

                        months.insert(month, days);
                    }
                }

                let year = year_entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse().ok())
                    .expect("invalid log entry name");

                years.insert(year, months);
            }
        }

        Ok(years)
    }

    pub async fn get_available_user_logs(
        &self,
        channel_id: &str,
        user_id: &str,
    ) -> Result<UserLogDateMap> {
        let user_index_name = format!("{user_id}.indexes");

        let channel_path = self.root_path.join(channel_id);
        let mut channel_dir = read_dir(channel_path).await?;

        let mut years = BTreeMap::new();

        while let Some(year_entry) = channel_dir.next_entry().await? {
            if year_entry.metadata().await?.is_dir() {
                let mut year_dir = read_dir(year_entry.path()).await?;
                let mut months = Vec::new();

                while let Some(month_entry) = year_dir.next_entry().await? {
                    let user_index_path = month_entry.path().join("users").join(&user_index_name);
                    if fs::metadata(user_index_path).await.is_ok() {
                        let month = month_entry
                            .file_name()
                            .to_str()
                            .expect("invalid month name")
                            .to_owned();
                        months.push(month);
                    }
                }

                months.sort_unstable();
                let year = year_entry
                    .file_name()
                    .to_str()
                    .expect("invalid year")
                    .to_owned();
                years.insert(year, months);
            }
        }

        Ok(years)
    }

    pub async fn read_channel(
        &self,
        channel_id: &str,
        ChannelLogDate { year, month, day }: ChannelLogDate,
    ) -> Result<Vec<String>> {
        let channel_file_path = get_channel_path(
            &self.root_path,
            channel_id,
            &year.to_string(),
            &month.to_string(),
            &day.to_string(),
        );
        trace!("Reading logs from {channel_file_path:?}");

        let mut file = File::open(channel_file_path).await?;
        let file_len = file.metadata().await?.len();

        let mut contents = String::with_capacity(file_len.try_into().unwrap());
        file.read_to_string(&mut contents).await?;

        Ok(contents.lines().map(str::to_owned).collect())
    }

    pub async fn read_user(
        &self,
        channel_id: &str,
        user_id: &str,
        year: &str,
        month: &str,
    ) -> Result<Vec<String>> {
        let index_path = get_user_index_path(&self.root_path, channel_id, user_id, year, month);
        trace!("Reading user index from {index_path:?}");

        if !index_path.exists() {
            return Err(Error::NotFound);
        }

        let mut file = File::open(index_path).await?;
        let metadata = file.metadata().await?;
        let file_len = metadata.len().try_into().unwrap_or(0);

        let mut buf = Vec::with_capacity(file_len);
        file.read_to_end(&mut buf).await?;

        let indexes: Vec<Index> = buf
            .chunks_exact(index::SIZE)
            .map(|bytes| Index::from_bytes(bytes))
            .try_collect()?;

        let mut lines = Vec::new();
        let mut readers = HashMap::new();

        for index in indexes {
            let reader = if let Some(reader) = readers.get_mut(&index.day) {
                reader
            } else {
                let channel_file_path = get_channel_path(
                    &self.root_path,
                    channel_id,
                    year,
                    month,
                    &index.day.to_string(),
                );
                debug!("Creating new reader for {channel_file_path:?}");
                let file = File::open(channel_file_path).await?;
                let reader = BufReader::new(file);

                readers.entry(index.day).or_insert(reader)
            };

            reader.seek(SeekFrom::Start(index.offset)).await?;
            let mut handle = reader.take(index.len as u64);

            let mut buf = String::with_capacity(index.len as usize);
            handle.read_to_string(&mut buf).await?;

            lines.push(buf);
        }

        Ok(lines)
    }
}

fn get_day_folder(root_path: &PathBuf, channel_id: &str, date: Date<Utc>) -> PathBuf {
    root_path
        .join(channel_id)
        .join(date.year().to_string())
        .join(date.month().to_string())
        .join(date.day().to_string())
}

pub fn get_channel_path(
    root_path: &PathBuf,
    channel_id: &str,
    year: &str,
    month: &str,
    day: &str,
) -> PathBuf {
    root_path
        .join(channel_id)
        .join(year)
        .join(month)
        .join(day)
        .join("channel.txt")
}

pub fn get_users_path(root_path: &PathBuf, channel_id: &str, year: &str, month: &str) -> PathBuf {
    root_path
        .join(channel_id)
        .join(year)
        .join(month)
        .join("users")
}

fn get_user_index_path(
    root_path: &PathBuf,
    channel_id: &str,
    user_id: &str,
    year: &str,
    month: &str,
) -> PathBuf {
    root_path
        .join(channel_id)
        .join(year)
        .join(month)
        .join("users")
        .join(format!("{user_id}.indexes"))
}

pub fn extract_channel_and_user(
    server_msg: &ServerMessage,
) -> Option<(ChannelIdentifier, Option<UserIdentifier>)> {
    match server_msg {
        ServerMessage::Privmsg(privmsg) => Some((
            ChannelIdentifier::ChannelId(&privmsg.channel_id),
            Some(UserIdentifier::UserId(&privmsg.sender.id)),
        )),
        ServerMessage::ClearChat(clear_chat) => {
            let channel_id = &clear_chat.channel_id;
            let user_id = match &clear_chat.action {
                ClearChatAction::UserBanned {
                    user_login: _,
                    user_id,
                } => Some(user_id),
                ClearChatAction::UserTimedOut {
                    user_login: _,
                    user_id,
                    timeout_length: _,
                } => Some(user_id),
                ClearChatAction::ChatCleared => None,
            };

            Some((
                ChannelIdentifier::ChannelId(channel_id),
                user_id.map(|id| UserIdentifier::UserId(id)),
            ))
        }
        ServerMessage::ClearMsg(clearmsg) => Some((
            ChannelIdentifier::Channel(&clearmsg.channel_login),
            Some(UserIdentifier::User(&clearmsg.sender_login)),
        )),
        ServerMessage::HostTarget(host_target) => {
            Some((ChannelIdentifier::Channel(&host_target.channel_login), None))
        }
        /*ServerMessage::RoomState(room_state) => {
            Some((ChannelIdentifier::ChannelId(&room_state.channel_id), None))
        }*/
        ServerMessage::UserNotice(user_notice) => Some((
            ChannelIdentifier::ChannelId(&user_notice.channel_id),
            Some(UserIdentifier::UserId(&user_notice.sender.id)),
        )),
        _ => None,
    }
}
