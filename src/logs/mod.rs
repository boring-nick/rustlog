pub mod index;
pub mod schema;

use self::schema::{ChannelLogDate, ChannelLogDateMap, OpenWriter, UserLogDate, UserLogDateMap};
use crate::{error::Error, logs::index::Index, Result};
use anyhow::Context;
use chrono::{Date, Datelike, TimeZone, Utc};
use dashmap::{
    mapref::{entry::Entry, one::RefMut},
    DashMap,
};
use itertools::Itertools;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use std::{
    collections::BTreeMap,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::{self, read_dir, File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt, BufWriter},
};
use tracing::{debug, error, trace};
use twitch_irc::message::IRCMessage;

pub const COMPRESSED_CHANNEL_FILE: &str = "channel.txt.gz";
pub const UNCOMPRESSED_CHANNEL_FILE: &str = "channel.txt";

#[derive(Debug, Clone)]
pub struct Logs {
    pub root_path: Arc<PathBuf>,
    channel_file_handles: Arc<DashMap<String, OpenWriter>>,
}

impl Logs {
    pub async fn new(logs_path: &str) -> Result<Self> {
        let root_folder = PathBuf::from(logs_path);

        if !root_folder.exists() {
            fs::create_dir_all(&root_folder).await?;
        }

        Ok(Self {
            root_path: Arc::new(root_folder),
            channel_file_handles: Arc::new(DashMap::new()),
        })
    }

    pub async fn get_stored_channels(&self) -> Result<Vec<String>> {
        let mut entries = read_dir(&*self.root_path).await?;

        let mut channels = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry.metadata().await?.is_dir() {
                let channel = entry
                    .file_name()
                    .into_string()
                    .expect("invalid channel folder name");
                channels.push(channel);
            }
        }

        Ok(channels)
    }

    pub async fn write_message(
        &self,
        raw_msg: String,
        channel_id: &str,
        maybe_user_id: Option<&str>,
    ) -> Result<()> {
        trace!("Logging message {raw_msg:?}");

        let today = Utc::today();

        let day_folder = get_day_path(&self.root_path, channel_id, today);
        if !day_folder.exists() {
            fs::create_dir_all(&day_folder).await?;
        }

        let mut writer_entry = self.get_channel_writer(channel_id.to_owned()).await?;
        let channel_writer = &mut writer_entry.writer;

        let offset = channel_writer.seek(SeekFrom::End(0)).await?;

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
                user_id,
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

    pub async fn get_available_channel_logs(
        &self,
        channel_id: &str,
        include_compressed: bool,
    ) -> Result<ChannelLogDateMap> {
        debug!("Gettings logs for channel {channel_id}");
        let channel_path = self.root_path.join(channel_id);
        if !channel_path.exists() {
            return Err(Error::NotFound);
        }

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
                            if day_entry.metadata().await?.is_dir()
                                && day_entry.file_name().to_str() != Some("users")
                            {
                                let day = day_entry
                                    .file_name()
                                    .to_str()
                                    .and_then(|name| name.parse().ok())
                                    .expect("invalid log entry name");

                                let uncompressed_channel_file_path =
                                    day_entry.path().join(UNCOMPRESSED_CHANNEL_FILE);

                                if fs::metadata(&uncompressed_channel_file_path)
                                    .await
                                    .map_or(false, |metadata| metadata.is_file())
                                {
                                    days.push(day);
                                } else if include_compressed {
                                    let compressed_channel_file_path =
                                        day_entry.path().join(COMPRESSED_CHANNEL_FILE);

                                    if fs::metadata(&compressed_channel_file_path)
                                        .await
                                        .map_or(false, |metadata| metadata.is_file())
                                    {
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
                            .and_then(|month| month.parse::<u32>().ok())
                            .expect("invalid month name")
                            .to_owned();
                        months.push(month);
                    }
                }

                months.sort_unstable();
                let year = year_entry
                    .file_name()
                    .to_str()
                    .and_then(|year| year.parse::<u32>().ok())
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
        let date = Utc.ymd(year.try_into().unwrap(), month, day);

        let channel_file_path = get_channel_path(&self.root_path, channel_id, date, false);
        trace!("Reading logs from {channel_file_path:?}");

        let file = File::open(channel_file_path).await?;
        let mut file = file.into_std().await;

        let contents = tokio::task::spawn_blocking(move || {
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            Result::Ok(contents)
        })
        .await
        .expect("failed to join task")?;

        Ok(contents.lines().map(str::to_owned).collect())
    }

    pub async fn read_user(
        &self,
        channel_id: &str,
        user_id: &str,
        log_date: UserLogDate,
    ) -> Result<Vec<String>> {
        let index_path = get_user_index_path(&self.root_path, channel_id, user_id, log_date);
        trace!("Reading user index from {index_path:?}");

        if !index_path.exists() {
            return Err(Error::NotFound);
        }

        let file = File::open(index_path).await?;
        let mut file = file.into_std().await;

        let buf = tokio::task::spawn_blocking(move || {
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            Result::Ok(buf)
        })
        .await
        .expect("failed to join task")?;

        let indexes: Vec<Index> = buf
            .chunks_exact(index::SIZE)
            .map(Index::from_bytes)
            .try_collect()?;

        let mut indexes_by_days = BTreeMap::new();

        for index in indexes {
            let day_indexes = indexes_by_days.entry(index.day).or_insert_with(Vec::new);
            day_indexes.push(index);
        }

        let mut lines = Vec::new();

        for (day, indexes) in indexes_by_days.into_iter() {
            let date = Utc.ymd(log_date.year.try_into().unwrap(), log_date.month, day);
            let channel_file_path = get_channel_path(&self.root_path, channel_id, date, false);

            debug!("Creating reader for {channel_file_path:?}");
            let file = File::open(channel_file_path).await?.into_std().await;
            let mut reader = std::io::BufReader::new(file);

            let day_lines = tokio::task::spawn_blocking(move || {
                let mut lines = Vec::new();
                for index in indexes {
                    reader.seek(SeekFrom::Start(index.offset))?;

                    let mut buf = vec![0; index.len as usize];
                    reader.read_exact(&mut buf)?;

                    let msg = String::from_utf8(buf).map_err(|err| {
                        error!("Could not read message buffer: {err}");
                        Error::Internal
                    })?;
                    lines.push(msg);
                }
                Result::Ok(lines)
            })
            .await
            .expect("failed to join task")?;

            lines.extend(day_lines)
        }

        Ok(lines)
    }

    pub async fn random_channel_line(&self, channel_id: &str) -> Result<String> {
        let mut rng = StdRng::from_entropy();

        let log_date = self
            .get_available_channel_logs(channel_id, false)
            .await?
            .into_iter()
            .flat_map(|(year, months)| {
                months.into_iter().flat_map(move |(month, days)| {
                    days.into_iter()
                        .map(move |day| ChannelLogDate { year, month, day })
                })
            })
            .choose(&mut rng)
            .ok_or(Error::NotFound)?;

        Ok(self
            .read_channel(channel_id, log_date)
            .await?
            .into_iter()
            .choose(&mut rng)
            .ok_or(Error::NotFound)?)
    }

    pub async fn random_user_line(&self, channel_id: &str, user_id: &str) -> Result<String> {
        let mut rng = StdRng::from_entropy();

        let date = self
            .get_available_user_logs(channel_id, user_id)
            .await?
            .into_iter()
            .flat_map(|(year, months)| {
                months
                    .into_iter()
                    .map(move |month| UserLogDate { year, month })
            })
            .choose(&mut rng)
            .ok_or(Error::NotFound)?;

        Ok(self
            .read_user(channel_id, user_id, date)
            .await?
            .into_iter()
            .choose(&mut rng)
            .ok_or(Error::NotFound)?)
    }

    /// Gets a write handle for a given channel for today, creates/opens one if it doesn't exist
    async fn get_channel_writer(
        &self,
        channel_id: String,
    ) -> anyhow::Result<RefMut<String, OpenWriter>> {
        let today = Utc::today();
        let path = get_channel_path(&self.root_path, &channel_id, today, false);

        match self.channel_file_handles.entry(channel_id) {
            Entry::Occupied(mut occupied) => {
                if occupied.get().date == today {
                    Ok(occupied.into_ref())
                } else {
                    debug!("Channel writer handle at {path:?} is outdated, replacing");
                    let writer = create_writer(path, today).await?;
                    occupied.insert(writer);
                    Ok(occupied.into_ref())
                }
            }
            Entry::Vacant(vacant) => {
                debug!("Opening new channel writer at {path:?}");
                let writer = create_writer(path, today).await?;
                Ok(vacant.insert(writer))
            }
        }
    }
}

async fn create_writer(path: PathBuf, date: Date<Utc>) -> anyhow::Result<OpenWriter> {
    fs::create_dir_all(path.parent().context("given path has no parent")?).await?;

    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .await?;
    let writer = BufWriter::new(file);

    Ok(OpenWriter { writer, date })
}

pub fn get_day_path(root_path: &Path, channel_id: &str, date: Date<Utc>) -> PathBuf {
    root_path
        .join(channel_id)
        .join(date.year().to_string())
        .join(date.month().to_string())
        .join(date.day().to_string())
}

pub fn get_channel_path(
    root_path: &Path,
    channel_id: &str,
    date: Date<Utc>,
    compressed: bool,
) -> PathBuf {
    let base_path = get_day_path(root_path, channel_id, date);
    if compressed {
        base_path.join(COMPRESSED_CHANNEL_FILE)
    } else {
        base_path.join(UNCOMPRESSED_CHANNEL_FILE)
    }
}

pub fn get_users_path(root_path: &Path, channel_id: &str, year: &str, month: &str) -> PathBuf {
    root_path
        .join(channel_id)
        .join(year)
        .join(month)
        .join("users")
}

pub fn get_user_index_path(
    root_path: &Path,
    channel_id: &str,
    user_id: &str,
    UserLogDate { year, month }: UserLogDate,
) -> PathBuf {
    root_path
        .join(channel_id)
        .join(&year.to_string())
        .join(&month.to_string())
        .join("users")
        .join(format!("{user_id}.indexes"))
}

pub fn extract_channel_and_user_from_raw(
    raw_msg: &'_ IRCMessage,
) -> Option<(&'_ str, Option<&'_ str>)> {
    let tags = &raw_msg.tags.0;
    tags.get("room-id")
        .and_then(|item| item.as_deref())
        .map(|channel_id| {
            let user_id = tags.get("user-id").and_then(|user_id| user_id.as_deref());
            (channel_id, user_id)
        })
}
