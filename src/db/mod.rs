pub mod schema;
pub mod writer;

use crate::{
    error::Error,
    logs::schema::{ChannelLogDate, UserLogDate},
    web::schema::AvailableLogDate,
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

    if messages.is_empty() {
        return Err(Error::NotFound);
    }

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

    if messages.is_empty() {
        return Err(Error::NotFound);
    }

    debug!("Read {} messages from DB", messages.len());
    Ok(messages)
}

pub async fn read_available_channel_logs(
    db: &Client,
    channel_id: &str,
) -> Result<Vec<AvailableLogDate>> {
    let timestamps: Vec<i32> = db
        .query(
            "SELECT DISTINCT toDateTime(toStartOfDay(timestamp)) AS date FROM message WHERE channel_id = ? ORDER BY date DESC",
        )
        .bind(channel_id)
        .fetch_all().await?;

    let dates = timestamps
        .into_iter()
        .map(|timestamp| {
            let naive =
                NaiveDateTime::from_timestamp_opt(timestamp.into(), 0).expect("Invalid DateTime");

            AvailableLogDate {
                year: naive.year().to_string(),
                month: naive.month().to_string(),
                day: Some(naive.day().to_string()),
            }
        })
        .collect();

    Ok(dates)
}

pub async fn read_available_user_logs(
    db: &Client,
    channel_id: &str,
    user_id: &str,
) -> Result<Vec<AvailableLogDate>> {
    let timestamps: Vec<i32> = db
        .query("SELECT DISTINCT toDateTime(toStartOfMonth(timestamp)) AS date FROM message WHERE channel_id = ? AND user_id = ? ORDER BY date DESC")
        .bind(channel_id)
        .bind(user_id)
        .fetch_all().await?;

    let dates = timestamps
        .into_iter()
        .map(|timestamp| {
            let naive =
                NaiveDateTime::from_timestamp_opt(timestamp.into(), 0).expect("Invalid DateTime");

            AvailableLogDate {
                year: naive.year().to_string(),
                month: naive.month().to_string(),
                day: None,
            }
        })
        .collect();

    Ok(dates)
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
