mod users;

use super::INSERT_BATCH_SIZE;
use crate::{
    db::schema::{Message, MESSAGES_TABLE},
    migrator::supibot::users::UsersClient,
};
use anyhow::Context;
use chrono::NaiveDateTime;
use itertools::Itertools;
use serde::Deserialize;
use std::{borrow::Cow, fs::File, io::BufReader, path::Path, time::Duration};
use tracing::info;

// Exmaple: 2023-06-23 14:46:26.588
const DATE_FMT: &str = "%F %X%.3f";
const CHUNK_SIZE: usize = 5000;

pub async fn run(db: clickhouse::Client, file_path: &Path, channel_id: &str) -> anyhow::Result<()> {
    info!("Loading file {file_path:?}");

    let mut inserter = db
        .inserter(MESSAGES_TABLE)?
        .with_timeouts(
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(180)),
        )
        .with_max_entries(INSERT_BATCH_SIZE)
        .with_period(Some(Duration::from_secs(15)));

    let mut users_client = UsersClient::default();
    let channel_user = users_client.get_user(channel_id).await?;
    let channel_login = &channel_user.login;

    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let rdr = csv::Reader::from_reader(reader);

    for chunk in &rdr
        .into_deserialize::<SupibotMessage>()
        .enumerate()
        .chunks(CHUNK_SIZE)
    {
        let supibot_messages: Vec<_> = chunk
            .map(|(i, result)| {
                if i % 10000 == 0 {
                    info!("Processing message {}", i + 1);
                }
                result
            })
            .filter_ok(|message| message.historic == 0)
            .try_collect()?;

        let mut user_ids: Vec<&str> = supibot_messages
            .iter()
            .map(|message| message.platform_id.as_str())
            .collect();
        user_ids.dedup();
        let users = users_client.get_users(&user_ids).await?;

        for supibot_message in supibot_messages {
            let text = &supibot_message.text;

            let user = users.get(&supibot_message.platform_id).with_context(|| {
                format!(
                    "User {} is not in the users response",
                    supibot_message.platform_id
                )
            })?;
            let display_name = &user.display_name;
            let user_id = &user.id;
            let login = &user.login;

            let timestamp = NaiveDateTime::parse_from_str(&supibot_message.posted, DATE_FMT)
                .unwrap()
                .timestamp_millis() as u64;

            let raw = format!(
            "@badges=;color=;display-name={display_name};emotes=;mod=0;room-id={channel_id};subscriber=0;tmi-sent-ts={timestamp};turbo=0;user-id={user_id};user-type= :{login}!{login}@rutntutn.tmi.twitch.tv PRIVMSG #{channel_login} :{text}"
        );

            let message = Message {
                channel_id: Cow::Borrowed(channel_id),
                user_id: Cow::Owned(user.id.clone()),
                timestamp,
                raw: Cow::Owned(raw),
            };

            inserter.write(&message).await?;
        }

        let stats = inserter
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

    let stats = inserter.end().await.context("Could not flush messages")?;
    if stats.entries > 0 {
        info!(
            "DB: {} entries ({} transactions) have been inserted",
            stats.entries, stats.transactions,
        );
    }

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
