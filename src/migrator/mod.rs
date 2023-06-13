mod reader;

use self::reader::{LogsReader, COMPRESSED_CHANNEL_FILE, UNCOMPRESSED_CHANNEL_FILE};
use crate::{
    db::schema::{Message, MESSAGES_TABLE},
    logs::extract::{extract_raw_timestamp, extract_user_id},
};
use anyhow::{anyhow, Context};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use clickhouse::inserter::Inserter;
use flate2::bufread::GzDecoder;
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

const INSERT_BATCH_SIZE: u64 = 10_000_000;

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
        let source_logs = LogsReader::new(&self.source_logs_path)?;

        let started_at = Instant::now();
        let channels = source_logs.get_stored_channels().await?;

        let semaphore = Arc::new(Semaphore::new(parallel_count));
        let mut handles = Vec::with_capacity(parallel_count);

        let filtered_channels: Vec<_> = channels
            .into_iter()
            .filter(|channel| self.channel_ids.is_empty() || self.channel_ids.contains(channel))
            .collect();

        info!("Migrating channels {filtered_channels:?}");

        let mut i = 1;

        for channel_id in &filtered_channels {
            let channel_started_at = Instant::now();
            info!(
                "Reading channel {channel_id} ({i}/{})",
                filtered_channels.len()
            );

            let available_logs = source_logs.get_available_channel_logs(channel_id, true)?;

            debug!(
                "Reading available logs took {:?}",
                channel_started_at.elapsed()
            );

            for (year, months) in available_logs {
                for (month, days) in months {
                    debug!("Waiting for free job slot");
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    let migrator = self.clone();
                    let channel_id = channel_id.clone();
                    let root_path = source_logs.root_path.clone();

                    let handle = tokio::spawn(async move {
                        let mut read_bytes = 0;

                        let mut inserter = migrator
                            .db
                            .inserter(MESSAGES_TABLE)?
                            .with_timeouts(
                                Some(Duration::from_secs(30)),
                                Some(Duration::from_secs(180)),
                            )
                            .with_max_entries(INSERT_BATCH_SIZE)
                            .with_period(Some(Duration::from_secs(15)));

                        for day in days {
                            let date = Utc
                                .with_ymd_and_hms(year.try_into().unwrap(), month, day, 0, 0, 0)
                                .unwrap();
                            info!(
                                "Migrating channel {channel_id} date {date}",
                                date = date.format("%Y-%m-%d")
                            );
                            read_bytes += migrator
                                .migrate_day(&root_path, &channel_id, date, &mut inserter)
                                .await?;
                        }

                        debug!("Flushing messages");
                        let stats = inserter.end().await.context("Could not flush messages")?;
                        if stats.entries > 0 {
                            info!(
                                "DB: {} entries ({} transactions) have been inserted",
                                stats.entries, stats.transactions,
                            );
                        }

                        drop(permit);
                        Result::<_, anyhow::Error>::Ok(read_bytes)
                    });
                    handles.push(handle);
                }
            }
            i += 1;
        }

        let mut total_read_bytes = 0;
        for handle in handles {
            total_read_bytes += handle.await.unwrap()?;
        }

        let elapsed = started_at.elapsed();
        info!("Migration finished in {elapsed:?}");

        let throughput = (total_read_bytes / 1024 / 1024) as u64 / (elapsed.as_secs());
        info!("Average migration speed: {throughput} MiB/s");

        Ok(())
    }

    // Returns the number of read bytes
    async fn migrate_day<'a>(
        &self,
        root_path: &Path,
        channel_id: &'a str,
        date: DateTime<Utc>,
        inserter: &mut Inserter<Message<'a>>,
    ) -> anyhow::Result<usize> {
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
    ) -> anyhow::Result<usize> {
        let mut read_bytes = 0;

        for line in reader.lines() {
            let line = line?;
            read_bytes += line.len() + 1; // Add 1 byte for newline symbol
            write_line(channel_id, line, inserter, datetime).await?;
        }

        let stats = inserter.commit().await?;
        if stats.entries > 0 {
            info!(
                "DB: {} entries ({} transactions) have been inserted",
                stats.entries, stats.transactions,
            );
        }

        Ok(read_bytes)
    }
}

async fn write_line<'a>(
    channel_id: &'a str,
    raw: String,
    inserter: &mut Inserter<Message<'_>>,
    datetime: DateTime<Utc>,
) -> anyhow::Result<()> {
    match twitch::Message::parse_with_whitelist(
        raw,
        twitch::whitelist!(TmiSentTs, UserId, TargetUserId),
    ) {
        Ok(irc_message) => {
            let timestamp = extract_raw_timestamp(&irc_message)
                .unwrap_or_else(|| datetime.timestamp_millis() as u64);
            let user_id = extract_user_id(&irc_message).unwrap_or_default();

            let message = Message {
                channel_id: Cow::Borrowed(channel_id),
                user_id: Cow::Borrowed(user_id),
                timestamp,
                raw: Cow::Borrowed(irc_message.raw()),
            };
            // This is safe because despite the function signature,
            // `inserter.write` only uses the value for serialization at the time of the method call, and not later
            let message: Message<'static> = unsafe { std::mem::transmute(message) };
            inserter.write(&message).await?;
        }
        Err(msg) => {
            warn!("Could not parse message `{msg}`");
        }
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
