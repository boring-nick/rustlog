use crate::{
    app::App,
    logs::{
        extract_channel_and_user_from_raw, get_day_path, get_users_path, index::Index, Logs,
        COMPRESSED_CHANNEL_FILE, UNCOMPRESSED_CHANNEL_FILE,
    },
};
use anyhow::{anyhow, Context};
use chrono::{Date, Datelike, TimeZone, Utc};
use flate2::bufread::GzDecoder;
use std::{
    collections::HashMap,
    convert::TryInto,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    time::Instant,
};
use tracing::{debug, info, warn};
use twitch_irc::message::IRCMessage;

pub struct Migrator<'a> {
    app: App<'a>,
    source_logs: Logs,
}

impl<'a> Migrator<'a> {
    pub async fn new(app: App<'a>, source_logs_path: &str) -> anyhow::Result<Migrator<'a>> {
        let source_logs = Logs::new(source_logs_path).await?;

        Ok(Self { app, source_logs })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let started_at = Instant::now();

        let channels = self.source_logs.get_stored_channels().await?;

        for channel_id in channels {
            let available_logs = self
                .source_logs
                .get_available_channel_logs(&channel_id, true)
                .await?;

            for (year, months) in available_logs {
                for (month, days) in months {
                    let users_path = get_users_path(
                        &self.app.logs.root_path,
                        &channel_id,
                        &year.to_string(),
                        &month.to_string(),
                    );
                    if users_path.exists() {
                        fs::remove_dir_all(users_path)?;
                    }

                    for day in days {
                        let date = Utc.ymd(year.try_into().unwrap(), month, day);
                        info!(
                            "Migrating channel {channel_id} date {}",
                            date = date.format("%Y-%m-%d")
                        );
                        self.migrate_day(&channel_id, date)?;
                    }
                }
            }
        }

        let elapsed = started_at.elapsed();
        info!("Migration finished in {elapsed:?}");

        Ok(())
    }

    fn migrate_day(&self, channel_id: &str, date: Date<Utc>) -> anyhow::Result<()> {
        let day_path = get_day_path(&self.source_logs.root_path, channel_id, date);

        let compressed_file_path = day_path.join(COMPRESSED_CHANNEL_FILE);
        let uncompressed_file_path = day_path.join(UNCOMPRESSED_CHANNEL_FILE);

        if compressed_file_path.exists() {
            debug!("Reading compressed log {compressed_file_path:?}");
            let file_reader = BufReader::new(File::open(&compressed_file_path)?);
            let gz = BufReader::new(GzDecoder::new(file_reader));

            self.migrate_reader(gz, channel_id, date)
        } else if uncompressed_file_path.exists() {
            debug!("Reading uncompressed log {uncompressed_file_path:?}");
            let file_reader = BufReader::new(File::open(&uncompressed_file_path)?);

            self.migrate_reader(file_reader, channel_id, date)
        } else {
            Err(anyhow!("File does not exist"))
        }
    }

    fn migrate_reader<R: BufRead>(
        &self,
        mut reader: R,
        channel_id: &str,
        date: Date<Utc>,
    ) -> anyhow::Result<()> {
        let day_path = get_day_path(&self.app.logs.root_path, channel_id, date);
        fs::create_dir_all(&day_path)?;
        let mut target_channel_writer =
            BufWriter::new(File::create(&day_path.join(UNCOMPRESSED_CHANNEL_FILE))?);

        let users_folder = get_users_path(
            &self.app.logs.root_path,
            channel_id,
            &date.year().to_string(),
            &date.month().to_string(),
        );
        fs::create_dir_all(&users_folder)?;
        let mut user_writers = HashMap::new();

        let mut buf = String::with_capacity(1024);
        let mut offset = 0;

        while reader.read_line(&mut buf)? != 0 {
            target_channel_writer.write_all(buf.as_bytes())?;

            buf.pop().context("empty buffer")?;

            match IRCMessage::parse(&buf) {
                Ok(irc_message) => {
                    if let Some((_, Some(user_id))) =
                        extract_channel_and_user_from_raw(&irc_message)
                    {
                        let index = Index {
                            day: date.day(),
                            offset,
                            len: buf.len().try_into().unwrap(),
                        };

                        let user_writer =
                            user_writers.entry(user_id.to_owned()).or_insert_with(|| {
                                let user_file_path =
                                    users_folder.join(format!("{user_id}.indexes"));
                                let file = OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open(user_file_path)
                                    .expect("could not open indexes file");
                                BufWriter::new(file)
                            });

                        user_writer.write_all(&index.as_bytes())?;
                    }
                }
                Err(err) => {
                    warn!("Malformed message: {buf} {err}")
                }
            }

            offset += buf.len() as u64 + 1;
            buf.clear();
        }

        for mut writer in user_writers.into_values() {
            writer.flush()?;
        }

        target_channel_writer.flush()?;

        Ok(())
    }
}
