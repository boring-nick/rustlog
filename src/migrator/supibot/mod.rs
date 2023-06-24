mod users;

use self::users::IvrUser;

use super::INSERT_BATCH_SIZE;
use crate::{
    db::schema::{Message, MESSAGES_TABLE},
    migrator::supibot::users::UsersClient,
};
use anyhow::Context;
use chrono::NaiveDateTime;
use clickhouse::inserter::Inserter;
use serde::Deserialize;
use std::{borrow::Cow, collections::HashMap, fs::File, io::BufReader, path::Path, time::Duration};
use tracing::info;

// Exmaple: 2023-06-23 14:46:26.588
const DATE_FMT: &str = "%F %X%.3f";
const USERS_REQUEST_CHUNK_SIZE: usize = 50;

pub async fn run(db: clickhouse::Client, file_path: &Path, channel_id: &str) -> anyhow::Result<()> {
    info!("Loading file {file_path:?}");

    let inserter = db
        .inserter(MESSAGES_TABLE)?
        .with_timeouts(
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(180)),
        )
        .with_max_entries(INSERT_BATCH_SIZE)
        .with_period(Some(Duration::from_secs(15)));

    let mut users_client = UsersClient::default();
    let channel_user = users_client.get_user(channel_id).await?;

    let migrator = SupibotMigrator {
        users_client,
        non_cached_messages: HashMap::with_capacity(USERS_REQUEST_CHUNK_SIZE),
        inserter,
        channel_user,
    };

    migrator.migrate_channel(file_path).await
}

struct SupibotMigrator {
    users_client: UsersClient,
    /// Messages whose users are not cached
    /// Indexed by user id
    non_cached_messages: HashMap<String, Vec<SupibotMessage>>,
    inserter: Inserter<Message<'static>>,
    channel_user: IvrUser,
}

impl SupibotMigrator {
    async fn migrate_channel(mut self, file_path: &Path) -> anyhow::Result<()> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let rdr = csv::Reader::from_reader(reader);

        for (i, result) in rdr.into_deserialize::<SupibotMessage>().enumerate() {
            if i % 10000 == 0 {
                info!("Processing message {}", i + 1);
            }
            let supibot_message = result?;

            if supibot_message.historic == 0 {
                if let Some(user) = self
                    .users_client
                    .get_cached_user(&supibot_message.platform_id)
                {
                    write_message(
                        supibot_message,
                        user,
                        &self.channel_user,
                        &mut self.inserter,
                    )
                    .await?;
                } else {
                    self.non_cached_messages
                        .entry(supibot_message.platform_id.clone())
                        .or_default()
                        .push(supibot_message);

                    if self.non_cached_messages.len() >= USERS_REQUEST_CHUNK_SIZE {
                        self.flush_non_cached().await?;
                    }
                }
            } else {
                let user = self
                    .users_client
                    .get_user_by_name(&supibot_message.platform_id)
                    .await?
                    // Used when the user id cannot be retrieved
                    .unwrap_or_else(|| IvrUser {
                        id: String::new(),
                        display_name: supibot_message.platform_id.clone(),
                        login: supibot_message.platform_id.clone(),
                        chat_color: None,
                    });

                write_message(
                    supibot_message,
                    &user,
                    &self.channel_user,
                    &mut self.inserter,
                )
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
        self.flush_non_cached().await?;

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

        Ok(())
    }

    async fn flush_non_cached(&mut self) -> anyhow::Result<()> {
        let user_ids = self.non_cached_messages.keys().collect::<Vec<_>>();
        let users = self.users_client.get_users(&user_ids).await?;

        for (user_id, messages) in self.non_cached_messages.drain() {
            let user = users
                .get(&user_id)
                .with_context(|| format!("User {user_id} is not in the users response",))?;

            for message in messages {
                write_message(message, user, &self.channel_user, &mut self.inserter).await?;
            }
        }

        Ok(())
    }
}

async fn write_message(
    supibot_message: SupibotMessage,
    user: &IvrUser,
    channel_user: &IvrUser,
    inserter: &mut Inserter<Message<'_>>,
) -> anyhow::Result<()> {
    let text = &supibot_message.text;

    let timestamp = NaiveDateTime::parse_from_str(&supibot_message.posted, DATE_FMT)
        .unwrap()
        .timestamp_millis() as u64;

    let raw = format!(
        "@id=;returning-chatter=0;turbo=0;mod=0;room-id={channel_id};subscriber=;tmi-sent-ts={timestamp};badge-info=;user-id={user_id};badges=;user-type=;display-name={display_name};flags=;emotes=;first-msg=0;color={color} :{login}!{login}@{login}.tmi.twitch.tv PRIVMSG #{channel_login} :{text}",
        channel_id = &channel_user.id,
        channel_login = &channel_user.login,
        display_name = &user.display_name,
        user_id = &user.id,
        login = &user.login,
        color = user.chat_color.as_deref().unwrap_or_default(),
    );

    let message = Message {
        channel_id: Cow::Owned(channel_user.id.clone()),
        user_id: Cow::Owned(user.id.clone()),
        timestamp,
        raw: Cow::Owned(raw),
    };

    inserter.write(&message).await?;
    Ok(())
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
