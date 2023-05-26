pub mod schema;
pub mod writer;

use std::collections::BTreeMap;

use crate::{
    logs::schema::{ChannelLogDate, ChannelLogDateMap, UserLogDate, UserLogDateMap},
    Result,
};
use chrono::{Datelike, NaiveDateTime};
use clickhouse::Client;
use tracing::debug;

pub async fn read_channel(
    db: &Client,
    channel_id: &str,
    log_date: ChannelLogDate,
) -> Result<Vec<String>> {
    let messages = db
        .query("SELECT raw FROM message WHERE channel_id = ? AND toStartOfDay(timestamp) = ? ORDER BY timestamp ASC")
        .bind(channel_id)
        .bind(log_date.to_string())
        .fetch_all()
        .await?;
    debug!("Read {} messages from DB", messages.len());
    Ok(messages)
}

pub async fn read_user(
    db: &Client,
    channel_id: &str,
    user_id: &str,
    log_date: UserLogDate,
) -> Result<Vec<String>> {
    let messages = db
        .query("SELECT raw FROM message WHERE channel_id = ? AND user_id = ? AND toStartOfMonth(timestamp) = ? ORDER BY timestamp ASC")
        .bind(channel_id)
        .bind(user_id)
        .bind(format!("{}-{:0>2}-1", log_date.year, log_date.month))
        .fetch_all()
        .await?;
    debug!("Read {} messages from DB", messages.len());
    Ok(messages)
}

pub async fn read_available_channel_logs(
    db: &Client,
    channel_id: &str,
) -> Result<ChannelLogDateMap> {
    let mut years: ChannelLogDateMap = BTreeMap::new();

    let mut cursor = db
        .query(
            "SELECT DISTINCT toDateTime(toStartOfDay(timestamp)) FROM message WHERE channel_id = ?",
        )
        .bind(channel_id)
        .fetch::<i32>()?;

    while let Some(timestamp) = cursor.next().await? {
        debug!("Fetched channel log date {timestamp}");
        let naive =
            NaiveDateTime::from_timestamp_opt(timestamp.into(), 0).expect("Invalid DateTime");
        years
            .entry(naive.year() as u32)
            .or_default()
            .entry(naive.month())
            .or_default()
            .push(naive.day());
    }

    Ok(years)
}

pub async fn read_available_user_logs(
    db: &Client,
    channel_id: &str,
    user_id: &str,
) -> Result<UserLogDateMap> {
    let mut years: UserLogDateMap = BTreeMap::new();

    let mut cursor = db
        .query("SELECT DISTINCT toDateTime(toStartOfMonth(timestamp)) FROM message WHERE channel_id = ? AND user_id = ?")
        .bind(channel_id)
        .bind(user_id)
        .fetch::<i32>()?;

    while let Some(timestamp) = cursor.next().await? {
        debug!("Fetched user log date {timestamp}");
        let naive =
            NaiveDateTime::from_timestamp_opt(timestamp.into(), 0).expect("Invalid DateTime");
        years
            .entry(naive.year() as u32)
            .or_default()
            .push(naive.month());
    }

    Ok(years)
}

pub async fn setup_db(db: &Client) -> Result<()> {
    db.query(
        "
CREATE TABLE IF NOT EXISTS message
(
    channel_id LowCardinality(String),
    user_id String,
    timestamp DateTime64(3),
    raw String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (channel_id, user_id, timestamp)",
    )
    .execute()
    .await?;
    Ok(())
}
