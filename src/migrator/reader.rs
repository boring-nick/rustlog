use crate::{error::Error, Result};
use std::{
    collections::BTreeMap,
    fs::{self, read_dir},
    path::PathBuf,
    sync::Arc,
};
use tracing::debug;

pub const COMPRESSED_CHANNEL_FILE: &str = "channel.txt.gz";
pub const UNCOMPRESSED_CHANNEL_FILE: &str = "channel.txt";

type ChannelLogDateMap = BTreeMap<u32, BTreeMap<u32, Vec<u32>>>;

#[derive(Debug, Clone)]
pub struct LogsReader {
    pub root_path: Arc<PathBuf>,
}

impl LogsReader {
    pub fn new(logs_path: &str) -> Result<Self> {
        let root_folder = PathBuf::from(logs_path);

        if !root_folder.exists() {
            fs::create_dir_all(&root_folder)?;
        }

        Ok(Self {
            root_path: Arc::new(root_folder),
        })
    }

    pub async fn get_stored_channels(&self) -> Result<Vec<String>> {
        let mut entries = read_dir(&*self.root_path)?;

        let mut channels = Vec::new();
        while let Some(entry) = entries.next() {
            let entry = entry?;
            if entry.metadata()?.is_dir() {
                let channel = entry
                    .file_name()
                    .into_string()
                    .expect("invalid channel folder name");
                channels.push(channel);
            }
        }

        Ok(channels)
    }

    pub fn get_available_channel_logs(
        &self,
        channel_id: &str,
        include_compressed: bool,
    ) -> Result<ChannelLogDateMap> {
        debug!("Getting logs for channel {channel_id}");
        let channel_path = self.root_path.join(channel_id);
        if !channel_path.exists() {
            return Err(Error::NotFound);
        }

        let mut channel_dir = read_dir(channel_path)?;

        let mut years = BTreeMap::new();

        while let Some(year_entry) = channel_dir.next() {
            let year_entry = year_entry?;

            if year_entry.metadata()?.is_dir() {
                let mut year_dir = read_dir(year_entry.path())?;
                let mut months = BTreeMap::new();

                while let Some(month_entry) = year_dir.next() {
                    let month_entry = month_entry?;

                    if month_entry.metadata()?.is_dir() {
                        let mut month_dir = read_dir(month_entry.path())?;
                        let mut days = Vec::new();

                        while let Some(day_entry) = month_dir.next() {
                            let day_entry = day_entry?;

                            if day_entry.metadata()?.is_dir()
                                && day_entry.file_name().to_str() != Some("users")
                            {
                                let day = day_entry
                                    .file_name()
                                    .to_str()
                                    .and_then(|name| name.parse().ok())
                                    .expect("invalid log entry day name");

                                let uncompressed_channel_file_path =
                                    day_entry.path().join(UNCOMPRESSED_CHANNEL_FILE);

                                if fs::metadata(&uncompressed_channel_file_path)
                                    .map_or(false, |metadata| metadata.is_file())
                                {
                                    days.push(day);
                                } else if include_compressed {
                                    let compressed_channel_file_path =
                                        day_entry.path().join(COMPRESSED_CHANNEL_FILE);

                                    if fs::metadata(&compressed_channel_file_path)
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
                            .expect("invalid log entry month name");

                        months.insert(month, days);
                    }
                }

                let year = year_entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse().ok())
                    .expect("invalid log entry year name");

                years.insert(year, months);
            }
        }

        Ok(years)
    }
}
