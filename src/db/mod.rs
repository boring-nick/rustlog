mod migrations;
pub mod schema;
pub mod writer;

pub use migrations::run as setup_db;

use crate::{
    error::Error,
    logs::{schema::LogRangeParams, stream::LogsStream},
    web::schema::AvailableLogDate,
    Result,
};
use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Utc};
use clickhouse::{query::RowCursor, Client};
use rand::{seq::IteratorRandom, thread_rng};
use tracing::debug;

const CHANNEL_MULTI_QUERY_SIZE_DAYS: i64 = 14;

pub async fn read_channel(
    db: &Client,
    channel_id: &str,
    params: &LogRangeParams,
) -> Result<LogsStream> {
    let suffix = if params.logs_params.reverse {
        "DESC"
    } else {
        "ASC"
    };

    let mut query = format!("SELECT raw FROM message WHERE channel_id = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp {suffix}");

    let interval = Duration::days(CHANNEL_MULTI_QUERY_SIZE_DAYS);
    if params.to - params.from > interval {
        let count = db
            .query("SELECT count() FROM (SELECT timestamp FROM message WHERE channel_id = ? AND timestamp >= ? AND timestamp < ? LIMIT 1)")
            .bind(channel_id)
            .bind(params.from.timestamp_millis() as f64 / 1000.0)
            .bind(params.to.timestamp_millis() as f64 / 1000.0)
            .fetch_one::<i32>().await?;
        if count == 0 {
            return Err(Error::NotFound);
        }

        let mut streams = Vec::with_capacity(1);

        let mut current_from = params.from;
        let mut current_to = current_from + interval;

        loop {
            let cursor = next_cursor(db, &query, channel_id, current_from, current_to)?;
            streams.push(cursor);

            current_from += interval;
            current_to += interval;

            if current_to > params.to {
                let cursor = next_cursor(db, &query, channel_id, current_from, params.to)?;
                streams.push(cursor);
                break;
            }
        }

        if params.logs_params.reverse {
            streams.reverse();
        }

        debug!("Using {} queries for multi-query stream", streams.len());

        LogsStream::new_multi_query(streams)
    } else {
        apply_limit_offset(
            &mut query,
            params.logs_params.limit,
            params.logs_params.offset,
        );

        let cursor = db
            .query(&query)
            .bind(channel_id)
            .bind(params.from.timestamp_millis() as f64 / 1000.0)
            .bind(params.to.timestamp_millis() as f64 / 1000.0)
            .fetch()?;
        LogsStream::new_cursor(cursor).await
    }
}

fn next_cursor(
    db: &Client,
    query: &str,
    channel_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<RowCursor<String>> {
    let cursor = db
        .query(query)
        .bind(channel_id)
        .bind(from.timestamp_millis() as f64 / 1000.0)
        .bind(to.timestamp_millis() as f64 / 1000.0)
        .fetch()?;
    Ok(cursor)
}

pub async fn read_user(
    db: &Client,
    channel_id: &str,
    user_id: &str,
    params: &LogRangeParams,
) -> Result<LogsStream> {
    let suffix = if params.logs_params.reverse {
        "DESC"
    } else {
        "ASC"
    };
    let mut query = format!("SELECT raw FROM message WHERE channel_id = ? AND user_id = ? AND timestamp >= ? AND timestamp < ? ORDER BY timestamp {suffix}");
    apply_limit_offset(
        &mut query,
        params.logs_params.limit,
        params.logs_params.offset,
    );

    let cursor = db
        .query(&query)
        .bind(channel_id)
        .bind(user_id)
        .bind(params.from.timestamp_millis() as f64 / 1000.0)
        .bind(params.to.timestamp_millis() as f64 / 1000.0)
        .fetch()?;
    LogsStream::new_cursor(cursor).await
}

pub async fn read_available_channel_logs(
    db: &Client,
    channel_id: &str,
) -> Result<Vec<AvailableLogDate>> {
    let timestamps: Vec<i32> = db
        .query(
            "SELECT toDateTime(toStartOfDay(timestamp)) AS date FROM message WHERE channel_id = ? GROUP BY date ORDER BY date DESC",
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
        .query("SELECT toDateTime(toStartOfMonth(timestamp)) AS date FROM message WHERE channel_id = ? AND user_id = ? GROUP BY date ORDER BY date DESC")
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

    if total_count == 0 {
        return Err(Error::NotFound);
    }

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
        .fetch_optional::<String>()
        .await?
        .ok_or(Error::NotFound)?;

    Ok(text)
}

pub async fn read_random_channel_line(db: &Client, channel_id: &str) -> Result<String> {
    let total_count = db
        .query("SELECT count(*) FROM message WHERE channel_id = ? ")
        .bind(channel_id)
        .fetch_one::<u64>()
        .await?;

    if total_count == 0 {
        return Err(Error::NotFound);
    }

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
        .fetch_optional::<String>()
        .await?
        .ok_or(Error::NotFound)?;

    Ok(text)
}

pub async fn delete_user_logs(_db: &Client, _user_id: &str) -> Result<()> {
    // info!("Deleting all logs for user {user_id}");
    // db.query("ALTER TABLE message DELETE WHERE user_id = ?")
    //     .bind(user_id)
    //     .execute()
    //     .await?;
    Ok(())
}

fn apply_limit_offset(query: &mut String, limit: Option<u64>, offset: Option<u64>) {
    if let Some(limit) = limit {
        *query = format!("{query} LIMIT {limit}");
    }
    if let Some(offset) = offset {
        *query = format!("{query} OFFSET {offset}");
    }
}
