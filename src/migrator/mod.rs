mod reader;

use self::reader::{LogsReader, COMPRESSED_CHANNEL_FILE, UNCOMPRESSED_CHANNEL_FILE};
use crate::{
    db::schema::{Message, MESSAGES_TABLE},
    logs::{extract_channel_and_user_from_raw, extract_timestamp},
};
use anyhow::anyhow;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use clickhouse::inserter::Inserter;
use flate2::bufread::GzDecoder;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::{
    borrow::Cow,
    convert::TryInto,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};
use twitch_irc::message::IRCMessage;

const LINES_BATCH_SIZE: usize = 32768;

#[derive(Clone)]
pub struct Migrator {
    db: clickhouse::Client,
    source_logs_path: String,
    channel_ids: Arc<Vec<String>>,
}

impl Migrator {
    pub async fn new(
        db: clickhouse::Client,
        source_logs_path: String,
        channel_ids: Vec<String>,
    ) -> anyhow::Result<Migrator> {
        Ok(Self {
            db,
            source_logs_path,
            channel_ids: Arc::new(channel_ids),
        })
    }

    pub async fn run(self, parallel_count: usize) -> anyhow::Result<()> {
        let source_logs = LogsReader::new(&self.source_logs_path).await?;

        info!("Migrating channels {:?}", self.channel_ids);

        let started_at = Instant::now();
        let channels = source_logs.get_stored_channels().await?;

        let semaphore = Arc::new(Semaphore::new(parallel_count));
        let mut handles = Vec::with_capacity(parallel_count);

        for channel_id in channels {
            if !self.channel_ids.is_empty() && !self.channel_ids.contains(&channel_id) {
                info!("Skipping channel {channel_id}");
                continue;
            }

            let available_logs = source_logs
                .get_available_channel_logs(&channel_id, true)
                .await?;

            for (year, months) in available_logs {
                for (month, days) in months {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    let migrator = self.clone();
                    let channel_id = channel_id.clone();
                    let root_path = source_logs.root_path.clone();

                    let handle = tokio::spawn(async move {
                        let mut inserter = migrator
                            .db
                            .inserter(MESSAGES_TABLE)?
                            .with_timeouts(
                                Some(Duration::from_secs(5)),
                                Some(Duration::from_secs(20)),
                            )
                            .with_max_entries(750_000)
                            .with_period(Some(Duration::from_secs(15)));

                        for day in days {
                            let date = Utc
                                .with_ymd_and_hms(year.try_into().unwrap(), month, day, 0, 0, 0)
                                .unwrap();
                            info!(
                                "Migrating channel {channel_id} date {date}",
                                date = date.format("%Y-%m-%d")
                            );
                            migrator
                                .migrate_day(&root_path, &channel_id, date, &mut inserter)
                                .await?;
                        }

                        inserter.end().await?;

                        drop(permit);
                        Result::<_, anyhow::Error>::Ok(())
                    });
                    handles.push(handle);
                }
            }
        }

        for handle in handles {
            handle.await.unwrap()?;
        }

        let elapsed = started_at.elapsed();
        info!("Migration finished in {elapsed:?}");

        Ok(())
    }

    async fn migrate_day<'a>(
        &self,
        root_path: &Path,
        channel_id: &'a str,
        date: DateTime<Utc>,
        inserter: &mut Inserter<Message<'a>>,
    ) -> anyhow::Result<()> {
        let day_path = get_day_path(root_path, channel_id, date);

        let compressed_file_path = day_path.join(COMPRESSED_CHANNEL_FILE);
        let uncompressed_file_path = day_path.join(UNCOMPRESSED_CHANNEL_FILE);

        if compressed_file_path.exists() {
            debug!("Reading compressed log {compressed_file_path:?}");
            let file_reader = BufReader::new(File::open(&compressed_file_path)?);
            let gz = BufReader::new(GzDecoder::new(file_reader));

            self.migrate_reader(gz, date, channel_id, inserter).await
        } else if uncompressed_file_path.exists() {
            debug!("Reading uncompressed log {uncompressed_file_path:?}");
            let file_reader = BufReader::new(File::open(&uncompressed_file_path)?);

            self.migrate_reader(file_reader, date, channel_id, inserter)
                .await
        } else {
            Err(anyhow!("File does not exist"))
        }
    }

    async fn migrate_reader<'a, R: BufRead>(
        &self,
        reader: R,
        datetime: DateTime<Utc>,
        channel_id: &'a str,
        inserter: &mut Inserter<Message<'a>>,
    ) -> anyhow::Result<()> {
        let mut buffer = Vec::with_capacity(LINES_BATCH_SIZE);

        for line in reader.lines() {
            let line = line?;
            buffer.push(line);

            if buffer.len() >= LINES_BATCH_SIZE {
                write_lines_buffer(channel_id, buffer, inserter, datetime).await?;
                buffer = Vec::with_capacity(LINES_BATCH_SIZE);
            }
        }

        write_lines_buffer(channel_id, buffer, inserter, datetime).await?;

        Ok(())
    }
}

async fn write_lines_buffer<'a>(
    channel_id: &'a str,
    buffer: Vec<String>,
    inserter: &mut Inserter<Message<'a>>,
    datetime: DateTime<Utc>,
) -> anyhow::Result<()> {
    let messages: Vec<Message> = buffer
        .into_par_iter()
        .filter_map(|raw| match IRCMessage::parse(&raw) {
            Ok(irc_message) => {
                let timestamp = extract_timestamp(&irc_message)
                    .unwrap_or_else(|| datetime.timestamp_millis() as u64);

                let user_id = extract_channel_and_user_from_raw(&irc_message)
                    .and_then(|(_, user_id)| user_id)
                    .map(str::to_owned)
                    .unwrap_or_default();

                let message = Message {
                    channel_id: Cow::Borrowed(channel_id),
                    user_id: Cow::Owned(user_id),
                    timestamp,
                    raw: Cow::Owned(raw),
                };
                Some(message)
            }
            Err(err) => {
                warn!("Could not parse message {raw}: {err}");
                None
            }
        })
        .collect();

    for message in messages {
        inserter.write(&message).await?;
    }

    let stats = inserter.commit().await?;
    if stats.entries > 0 {
        info!(
            "DB: {} entries ({} transactions) have been inserted",
            stats.entries, stats.transactions,
        );
    }

    Ok(())
}

fn get_day_path(root_path: &Path, channel_id: &str, date: DateTime<Utc>) -> PathBuf {
    root_path
        .join(channel_id)
        .join(date.year().to_string())
        .join(date.month().to_string())
        .join(date.day().to_string())
}
