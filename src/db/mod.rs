pub mod schema;
pub mod writer;

use crate::{
    error::Error,
    logs::{
        schema::{ChannelLogDate, UserLogDate},
        stream::LogsStream,
    },
    web::schema::AvailableLogDate,
    Result,
};
use chrono::{Datelike, NaiveDateTime};
use clickhouse::Client;
use rand::{seq::IteratorRandom, thread_rng};

pub async fn read_channel(
    db: &Client,
    channel_id: &str,
    log_date: ChannelLogDate,
) -> Result<LogsStream> {
    let cursor = db
        .query("SELECT raw FROM message WHERE channel_id = ? AND toStartOfDay(timestamp) = ? ORDER BY timestamp ASC")
        .bind(channel_id)
        .bind(log_date.to_string())
        .fetch()?;
    Ok(LogsStream::new_cursor(cursor))
}

pub async fn read_user(
    db: &Client,
    channel_id: &str,
    user_id: &str,
    log_date: UserLogDate,
) -> Result<LogsStream> {
    let cursor = db
        .query("SELECT raw FROM message WHERE channel_id = ? AND user_id = ? AND toStartOfMonth(timestamp) = ? ORDER BY timestamp ASC")
        .bind(channel_id)
        .bind(user_id)
        .bind(format!("{}-{:0>2}-1", log_date.year, log_date.month))
        .fetch()?;

    Ok(LogsStream::new_cursor(cursor))
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

pub async fn read_random_user_line(db: &Client, channel_id: &str, user_id: &str) -> Result<String> {
    let total_count = db
        .query("SELECT count(*) FROM message WHERE channel_id = ? AND user_id = ? ")
        .bind(channel_id)
        .bind(user_id)
        .fetch_one::<u64>()
        .await?;

    let offset = {
        let mut rng = thread_rng();
        (0..total_count).choose(&mut rng).ok_or(Error::NotFound)
    }?;

    let text = db
        .query(
            "WITH
            (SELECT timestamp FROM message WHERE channel_id = ? AND user_id = ? LIMIT 1 OFFSET ?)
            AS random_timestamp
            SELECT raw FROM message WHERE channel_id = ? AND user_id = ? AND timestamp = random_timestamp",
        )
        .bind(channel_id)
        .bind(user_id)
        .bind(offset)
        .bind(channel_id)
        .bind(user_id)
        .fetch_one::<String>()
        .await?;

    Ok(text)
}

pub async fn read_random_channel_line(db: &Client, channel_id: &str) -> Result<String> {
    let total_count = db
        .query("SELECT count(*) FROM message WHERE channel_id = ? ")
        .bind(channel_id)
        .fetch_one::<u64>()
        .await?;

    let offset = {
        let mut rng = thread_rng();
        (0..total_count).choose(&mut rng).ok_or(Error::NotFound)
    }?;

    let text = db
        .query(
            "WITH
            (SELECT timestamp FROM message WHERE channel_id = ? LIMIT 1 OFFSET ?)
            AS random_timestamp
            SELECT raw FROM message WHERE channel_id = ? AND timestamp = random_timestamp",
        )
        .bind(channel_id)
        .bind(offset)
        .bind(channel_id)
        .fetch_one::<String>()
        .await?;

    Ok(text)
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
