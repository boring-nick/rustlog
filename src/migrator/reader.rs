use crate::{error::Error, Result};
use std::{
    collections::BTreeMap,
    fs::{self, read_dir},
    path::PathBuf,
    sync::Arc,
};
use tracing::info;

pub const COMPRESSED_CHANNEL_FILE: &str = "channel.txt.gz";
pub const UNCOMPRESSED_CHANNEL_FILE: &str = "channel.txt";

pub type ChannelLogDateMap = BTreeMap<u32, BTreeMap<u32, Vec<u32>>>;

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
        let entries = read_dir(&*self.root_path)?;

        let mut channels = Vec::new();
        for entry in entries {
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

    pub fn get_available_channel_logs(&self, channel_id: &str) -> Result<(ChannelLogDateMap, u64)> {
        info!("Getting logs for channel {channel_id}");
        let channel_path = self.root_path.join(channel_id);
        if !channel_path.exists() {
            return Err(Error::NotFound);
        }

        let channel_dir = read_dir(channel_path)?;

        let mut years = BTreeMap::new();
        let mut total_size = 0;

        for year_entry in channel_dir {
            let year_entry = year_entry?;

            if year_entry.metadata()?.is_dir() {
                let mut months = BTreeMap::new();

                for month in 1..=12u32 {
                    let mut days = Vec::with_capacity(31);

                    for day in 1..=31u32 {
                        let day_path = year_entry
                            .path()
                            .join(month.to_string())
                            .join(day.to_string());

                        let compressed_channel_file_path = day_path.join(COMPRESSED_CHANNEL_FILE);
                        let uncompressed_channel_file_path =
                            day_path.join(UNCOMPRESSED_CHANNEL_FILE);

                        if let Ok(metadata) = fs::metadata(uncompressed_channel_file_path)
                            .or_else(|_| fs::metadata(compressed_channel_file_path))
                        {
                            if metadata.is_file() {
                                total_size += metadata.len();
                                days.push(day);
                            }
                        }
                    }

                    if !days.is_empty() {
                        months.insert(month, days);
                    }
                }

                let year = year_entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse().ok())
                    .expect("invalid log entry year name");

                if !months.is_empty() {
                    years.insert(year, months);
                }
            }
        }

        Ok((years, total_size))
    }
}
