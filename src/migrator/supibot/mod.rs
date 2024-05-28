mod users;

use super::INSERT_BATCH_SIZE;
use crate::{
    config::Config,
    db::schema::{Message, MESSAGES_TABLE},
    error::Error,
    migrator::supibot::users::UsersClient,
};
use anyhow::Context;
use chrono::{NaiveDate, NaiveDateTime};
use clickhouse::{inserter::Inserter, Row};
use serde::Deserialize;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fs::File,
    io::BufReader,
    path::Path,
    time::Duration,
};
use tracing::{error, info};

// Exmaple: 2023-06-23 14:46:26.588
const DATE_FMT: &str = "%F %X%.3f";
const USERS_REQUEST_CHUNK_SIZE: usize = 50;

pub async fn run(
    config: Config,
    db: clickhouse::Client,
    logs_path: &Path,
    users_file_path: Option<&Path>,
) -> anyhow::Result<()> {
    let mut users_client = UsersClient::default();
    if let Some(path) = users_file_path {
        users_client
            .add_from_file(path)
            .context("Could not read the users file")?;
    }

    let read_dir = std::fs::read_dir(logs_path)?;

    for entry in read_dir {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name = file_name.to_str().expect("File name is invalid UTF-8");

        if let Some(channel_name) = file_name.strip_suffix(".csv") {
            let channel_id = users_client
                .get_user_id_by_name(channel_name)
                .await?
                .context("Could not get channel id")?;
            info!("Migrating file {file_name}, channel id {channel_id}");

            let inserter = db
                .inserter(MESSAGES_TABLE)?
                .with_timeouts(
                    Some(Duration::from_secs(30)),
                    Some(Duration::from_secs(180)),
                )
                .with_max_entries(INSERT_BATCH_SIZE)
                .with_period(Some(Duration::from_secs(15)));

            let existing_dates = get_existing_dates(&db, &channel_id).await?;

            let migrator = SupibotMigrator {
                non_cached_messages: HashMap::with_capacity(USERS_REQUEST_CHUNK_SIZE),
                inserter,
                channel_id: channel_id.to_owned(),
                channel_login: channel_name.to_owned(),
                invalid_user_ids: HashSet::new(),
                existing_dates,
                imported_count: 0,
                skipped_count: 0,
            };

            info!("Adding channel {channel_id} to config");
            config
                .channels
                .write()
                .unwrap()
                .insert(channel_id.to_owned());
            config.save()?;

            match migrator
                .migrate_channel(&entry.path(), &mut users_client)
                .await
            {
                Ok((imported_count, skipped_count)) => {
                    info!("Channel {channel_name} successfully migrated:");
                    info!("Imported {imported_count} messages");
                    info!("{skipped_count} messages were skipped due to duplicate dates",);
                }
                Err(err) => {
                    error!("Could not migrate channel {channel_name}: {err:#}");
                }
            }
        }
    }
    Ok(())
}

async fn get_existing_dates(
    db: &clickhouse::Client,
    channel_id: &str,
) -> Result<HashSet<NaiveDate>, Error> {
    info!("Getting existing log dates");

    #[derive(Row, Deserialize)]
    struct DateRow {
        datetime: u32,
    }

    let raw_dates = db
        .query(
            "SELECT DISTINCT toStartOfDay(timestamp) as datetime FROM message WHERE channel_id = ?",
        )
        .bind(channel_id)
        .fetch_all::<DateRow>()
        .await?;
    let dates: HashSet<NaiveDate> = raw_dates
        .into_iter()
        .map(|row| {
            let datetime = NaiveDateTime::from_timestamp_opt(row.datetime as i64, 0)
                .expect("Invalid timestamp returned by the db");
            datetime.date()
        })
        .collect();
    info!(
        "Found {} dates where the channel already has logs",
        dates.len()
    );

    Ok(dates)
}

struct SupibotMigrator {
    existing_dates: HashSet<NaiveDate>,
    /// Messages whose users are not cached
    /// Indexed by user id
    non_cached_messages: HashMap<String, Vec<SupibotMessage>>,
    inserter: Inserter<Message<'static>>,
    channel_id: String,
    channel_login: String,
    invalid_user_ids: HashSet<String>,
    imported_count: u64,
    skipped_count: u64,
}

impl SupibotMigrator {
    async fn migrate_channel(
        mut self,
        file_path: &Path,
        users_client: &mut UsersClient,
    ) -> anyhow::Result<(u64, u64)> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let rdr = csv::Reader::from_reader(reader);

        for (i, result) in rdr.into_deserialize::<SupibotMessage>().enumerate() {
            if i % 100_000 == 0 {
                info!("Processing message {}", i + 1);
            }
            let supibot_message = result?;

            if supibot_message.historic == 0 {
                if let Some(user_login) =
                    users_client.get_cached_user_login(&supibot_message.platform_id)
                {
                    self.write_message(&supibot_message, user_login, &supibot_message.platform_id)
                        .await?;
                } else {
                    self.non_cached_messages
                        .entry(supibot_message.platform_id.clone())
                        .or_default()
                        .push(supibot_message);

                    if self.non_cached_messages.len() >= USERS_REQUEST_CHUNK_SIZE {
                        self.flush_non_cached(users_client).await?;
                    }
                }
            } else {
                let user_id = users_client
                    .get_user_id_by_name(&supibot_message.platform_id)
                    .await?
                    // Used when the user id cannot be retrieved
                    .unwrap_or_default();

                self.write_message(&supibot_message, &supibot_message.platform_id, &user_id)
                    .await?;
            }

            let stats = self
                .inserter
                .commit()
                .await
                .context("Could not flush messages")?;
            if stats.entries > 0 {
                info!(
                    "DB: {} entries ({} transactions) have been inserted",
                    stats.entries, stats.transactions,
                );
            }
        }
        self.flush_non_cached(users_client).await?;

        let stats = self
            .inserter
            .end()
            .await
            .context("Could not flush messages")?;
        if stats.entries > 0 {
            info!(
                "DB: {} entries ({} transactions) have been inserted",
                stats.entries, stats.transactions,
            );
        }

        if !self.invalid_user_ids.is_empty() {
            error!("Invalid user ids: {:?}", self.invalid_user_ids);
        }

        Ok((self.imported_count, self.skipped_count))
    }

    async fn flush_non_cached(&mut self, users_client: &mut UsersClient) -> anyhow::Result<()> {
        let user_ids = self.non_cached_messages.keys().collect::<Vec<_>>();
        let users = users_client.get_users(&user_ids).await?;

        let non_cached_messages = std::mem::take(&mut self.non_cached_messages);
        for (user_id, messages) in non_cached_messages {
            match users.get(&user_id) {
                Some(user_login) => {
                    for message in messages {
                        self.write_message(&message, user_login, &user_id).await?;
                        // write_message(message, user, &self.channel_user, &mut self.inserter)
                        //     .await?;
                    }
                }
                None => {
                    self.invalid_user_ids.insert(user_id);
                }
            }
        }

        Ok(())
    }

    async fn write_message(
        &mut self,
        supibot_message: &SupibotMessage,
        user_login: &str,
        user_id: &str,
    ) -> anyhow::Result<()> {
        let text = &supibot_message.text;
        let datetime = NaiveDateTime::parse_from_str(&supibot_message.posted, DATE_FMT).unwrap();

        if self.existing_dates.contains(&datetime.date()) {
            self.skipped_count += 1;
            return Ok(());
        }

        let timestamp = datetime.timestamp_millis() as u64;
        let raw = format!(
            "@id=;returning-chatter=0;turbo=0;mod=0;room-id={channel_id};subscriber=;tmi-sent-ts={timestamp};badge-info=;user-id={user_id};badges=;user-type=;display-name={display_name};flags=;emotes=;first-msg=0;color={color} :{login}!{login}@{login}.tmi.twitch.tv PRIVMSG #{channel_login} :{text}",
            channel_id = self.channel_id,
            channel_login = self.channel_login,
            display_name = user_login,
            user_id = user_id,
            login = user_login,
            color = "",
        );

        let message = Message {
            channel_id: Cow::Owned(self.channel_id.to_owned()),
            user_id: Cow::Owned(user_id.to_owned()),
            timestamp,
            raw: Cow::Owned(raw),
        };

        self.inserter.write(&message).await?;
        self.imported_count += 1;

        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SupibotMessage {
    #[serde(rename = "ID")]
    pub _id: u64,
    #[serde(rename = "Platform_ID")]
    pub platform_id: String,
    pub historic: u8,
    pub text: String,
    pub posted: String,
}

#[cfg(test)]
mod tests {
    use super::DATE_FMT;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    #[test]
    fn parse_date() {
        let date_str = "2023-06-23 14:46:26.588";
        let datetime = NaiveDateTime::parse_from_str(date_str, DATE_FMT).unwrap();
        assert_eq!(
            datetime,
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2023, 6, 23).unwrap(),
                NaiveTime::from_hms_milli_opt(14, 46, 26, 588).unwrap()
            )
        );
    }
}
