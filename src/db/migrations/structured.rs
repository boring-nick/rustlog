use super::migratable::Migratable;
use crate::db::schema::{StructuredMessage, UnstructuredMessage, MESSAGES_STRUCTURED_TABLE};
use anyhow::Context;
use std::time::Duration;
use tracing::{error, info};

const INSERT_BATCH_SIZE: u64 = 10_000_000;

pub struct StructuredMigration<'a> {
    pub db_name: &'a str,
}

impl<'a> Migratable<'a> for StructuredMigration<'a> {
    async fn run(&self, db: &'a clickhouse::Client) -> anyhow::Result<()> {
        db.query(
            "
CREATE TABLE IF NOT EXISTS message_structured
(
    `channel_id` LowCardinality(String),
    `channel_login` LowCardinality(String),
    `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(5)),
    `id` UUID CODEC(ZSTD(5)),
    `message_type` UInt8 CODEC(ZSTD(5)),
    `user_id` String CODEC(ZSTD(5)),
    `user_login` String CODEC(ZSTD(5)),
    `display_name` String CODEC(ZSTD(5)),
    `color` Nullable(UInt32) CODEC(ZSTD(5)),
    `user_type` LowCardinality(String) CODEC(ZSTD(5)),
    `badges` Array(LowCardinality(String)) CODEC(ZSTD(5)),
    `badge_info` String CODEC(ZSTD(5)),
    `text` String CODEC(ZSTD(5)),
    `message_flags` UInt16 CODEC(ZSTD(5)),
    `extra_tags` Map(LowCardinality(String), String) CODEC(ZSTD(5)),
    PROJECTION channel_log_dates
    (
        SELECT
            channel_id,
            toDateTime(toStartOfDay(timestamp)) AS date
        GROUP BY
            channel_id,
            date
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (channel_id, user_id, timestamp)
        ",
        )
        .execute()
        .await?;

        let partitions = db
            .query("SELECT DISTINCT partition FROM system.parts WHERE database = ? AND table = 'message' ORDER BY partition ASC")
            .bind(self.db_name)
            .fetch_all::<String>()
            .await
            .context("Could not fetch partition list")?;

        info!(
            "Migrating {} partitions to new table structure",
            partitions.len()
        );

        let mut i = 1;

        for partition in partitions {
            info!("Migrating partition {partition}");

            let mut inserter = db
                .inserter(MESSAGES_STRUCTURED_TABLE)?
                .with_timeouts(
                    Some(Duration::from_secs(30)),
                    Some(Duration::from_secs(180)),
                )
                .with_max_entries(INSERT_BATCH_SIZE)
                .with_period(Some(Duration::from_secs(15)));

            let mut cursor = db
                .query("SELECT * FROM message WHERE toYYYYMM(timestamp) = ?")
                .bind(&partition)
                .fetch::<UnstructuredMessage>()?;

            while let Some(unstructured_msg) = cursor.next().await? {
                match StructuredMessage::from_unstructured(&unstructured_msg) {
                    Ok(msg) => {
                        // This is safe because despite the function signature,
                        // `inserter.write` only uses the value for serialization at the time of the method call, and not later
                        let msg: StructuredMessage<'static> = unsafe { std::mem::transmute(msg) };
                        inserter
                            .write(&msg)
                            .await
                            .context("Failed to write message")?;

                        i += 1;
                        if i % 100_000 == 0 {
                            info!("Processed {i} messages");
                        }
                    }
                    Err(err) => {
                        error!("Could not process message {unstructured_msg:?}: {err}");
                    }
                }
            }

            let stats = inserter.end().await?;
            info!(
                "Processed partition {partition} with {} messages",
                stats.entries
            );
        }

        Ok(())
    }
}
