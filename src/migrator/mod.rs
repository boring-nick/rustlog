mod reader;

use self::reader::{LogsReader, COMPRESSED_CHANNEL_FILE, UNCOMPRESSED_CHANNEL_FILE};
use crate::{
    db::schema::{Message, MESSAGES_TABLE},
    logs::{extract_channel_and_user_from_raw, extract_timestamp, get_day_path},
};
use anyhow::anyhow;
use chrono::{Date, TimeZone, Utc};
use clickhouse::inserter::Inserter;
use flate2::bufread::GzDecoder;
use itertools::Itertools;
use std::{
    borrow::Cow,
    convert::TryInto,
    fs::File,
    io::{BufRead, BufReader},
    time::{Duration, Instant},
};
use tracing::{debug, info, warn};
use twitch_irc::message::IRCMessage;

pub struct Migrator {
    db: clickhouse::Client,
    batch_size: usize,
    source_logs: LogsReader,
    channel_ids: Vec<String>,
}

impl Migrator {
    pub async fn new(
        db: clickhouse::Client,
        batch_size: usize,
        source_logs_path: &str,
        channel_ids: Vec<String>,
    ) -> anyhow::Result<Migrator> {
        let source_logs = LogsReader::new(source_logs_path).await?;

        Ok(Self {
            db,
            batch_size,
            source_logs,
            channel_ids,
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        info!("Migrating channels {:?}", self.channel_ids);

        let started_at = Instant::now();

        let channels = self.source_logs.get_stored_channels().await?;

        for channel_id in channels {
            if !self.channel_ids.is_empty() && !self.channel_ids.contains(&channel_id) {
                info!("Skipping channel {channel_id}");
                continue;
            }

            let available_logs = self
                .source_logs
                .get_available_channel_logs(&channel_id, true)
                .await?;

            for (year, months) in available_logs {
                for (month, days) in months {
                    let mut inserter = self
                        .db
                        .inserter(MESSAGES_TABLE)?
                        .with_timeouts(Some(Duration::from_secs(5)), Some(Duration::from_secs(20)))
                        .with_max_entries(750_000)
                        .with_period(Some(Duration::from_secs(15)));

                    for day in days {
                        let date = Utc.ymd(year.try_into().unwrap(), month, day);
                        info!(
                            "Migrating channel {channel_id} date {date}",
                            date = date.format("%Y-%m-%d")
                        );
                        self.migrate_day(&channel_id, date, &mut inserter).await?;
                    }

                    inserter.end().await?;
                }
            }
        }

        let elapsed = started_at.elapsed();
        info!("Migration finished in {elapsed:?}");

        Ok(())
    }

    async fn migrate_day<'a>(
        &self,
        channel_id: &'a str,
        date: Date<Utc>,
        inserter: &mut Inserter<Message<'a>>,
    ) -> anyhow::Result<()> {
        let day_path = get_day_path(&self.source_logs.root_path, channel_id, date);

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
        date: Date<Utc>,
        channel_id: &'a str,
        inserter: &mut Inserter<Message<'a>>,
    ) -> anyhow::Result<()> {
        for chunk in reader.lines().chunks(self.batch_size).into_iter() {
            for raw in chunk {
                let raw = raw?;

                match IRCMessage::parse(&raw) {
                    Ok(irc_message) => {
                        let timestamp = extract_timestamp(&irc_message).unwrap_or_else(|| {
                            let datetime = date.and_hms_opt(0, 0, 0).unwrap();
                            datetime.timestamp_millis() as u64
                        });

                        let user_id = extract_channel_and_user_from_raw(&irc_message)
                            .and_then(|(_, user_id)| user_id)
                            .map(str::to_owned)
                            .unwrap_or_default();

                        let message = Message {
                            channel_id: Cow::Borrowed(channel_id),
                            user_id: Cow::Owned(user_id.to_owned()),
                            timestamp,
                            raw: Cow::Owned(raw),
                        };
                        inserter.write(&message).await?;
                    }
                    Err(err) => {
                        warn!("Could not parse message {raw}: {err}")
                    }
                }
            }

            let stats = inserter.commit().await?;
            if stats.entries > 0 {
                info!(
                    "DB: {} entries ({} transactions) have been inserted",
                    stats.entries, stats.transactions,
                );
            }
        }

        Ok(())
    }
}
