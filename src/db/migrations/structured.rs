use super::migratable::Migratable;
use crate::db::schema::{StructuredMessage, UnstructuredMessage, MESSAGES_STRUCTURED_TABLE};
use anyhow::{bail, Context};
use std::{
    env,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tracing::{error, info};

const INSERT_BATCH_SIZE: u64 = 10_000_000;

pub struct StructuredMigration<'a> {
    pub db_name: &'a str,
}

impl<'a> Migratable<'a> for StructuredMigration<'a> {
    async fn run(&self, db: &'a clickhouse::Client) -> anyhow::Result<()> {
        db.query(
            "
CREATE TABLE message_structured
(
    `channel_id` LowCardinality(String) CODEC(ZSTD(8)),
    `channel_login` LowCardinality(String) CODEC(ZSTD(8)),
    `timestamp` DateTime64(3) CODEC(T64, ZSTD(5)),
    `id` UUID CODEC(ZSTD(1)),
    `message_type` UInt8 CODEC(ZSTD(8)),
    `user_id` String CODEC(ZSTD(8)),
    `user_login` String CODEC(ZSTD(8)),
    `display_name` String CODEC(ZSTD(8)),
    `color` Nullable(UInt32) CODEC(ZSTD(8)),
    `user_type` LowCardinality(String) CODEC(ZSTD(8)),
    `badges` Array(LowCardinality(String)) CODEC(ZSTD(8)),
    `badge_info` String CODEC(ZSTD(8)),
    `client_nonce` String CODEC(ZSTD(1)),
    `emotes` String CODEC(ZSTD(8)),
    `automod_flags` String CODEC(ZSTD(8)),
    `text` String CODEC(ZSTD(8)),
    `message_flags` UInt16 CODEC(ZSTD(8)),
    `extra_tags` Map(LowCardinality(String), String) CODEC(ZSTD(8)),
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

        if partitions.len() > 1
            && env::var("RUSTLOG_ACKNOWLEDGE_STRUCTURE_MIGRATION").as_deref() != Ok("1")
        {
            bail!(
                "The current version of rustlog needs to perform a migration to a new database structure. This process can take from a few minutes to several hours depending on the database size. \
                The database will also increase in size up to a factor of 1.5x in the process, but after it's done it will become smaller. \
                Set the environment variable RUSTLOG_ACKNOWLEDGE_STRUCTURE_MIGRATION=1 to confirm and run the migration, or downgrade to an older version if you don't want to run it right now."
            );
        }

        info!(
            "Migrating {} partitions to new table structure",
            partitions.len()
        );

        let i = Arc::new(AtomicU64::new(1));
        let semaphore = Arc::new(Semaphore::new(4));

        let started_at = Instant::now();

        let mut tasks = Vec::new();

        for partition in partitions {
            let _permit = semaphore.clone().acquire_owned().await?;

            let db = db.clone();
            let i = i.clone();
            let task = tokio::spawn(async move {
                let result = migrate_partition(partition, &db, i).await;
                drop(_permit);
                result
            });

            tasks.push(task);
        }

        for task in tasks {
            task.await.unwrap()?;
        }

        info!(
            "Migrated {} messages in {:?}",
            i.load(Ordering::SeqCst),
            started_at.elapsed()
        );

        info!("Dropping old table");
        if let Err(err) = db.query("DROP TABLE message").execute().await {
            error!("FAILED TO DROP OLD TABLE!!!! {err}");
            error!("Drop it manually with `DROP TABLE message` to save on space")
        }

        Ok(())
    }
}

async fn migrate_partition(
    partition: String,
    db: &clickhouse::Client,
    i: Arc<AtomicU64>,
) -> anyhow::Result<()> {
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

                let stats = inserter.commit().await.context("Could not commit")?;
                if stats.entries > 0 {
                    info!(
                        "Inserted {} messages from partition {partition}",
                        stats.entries
                    );
                }

                i.fetch_add(1, Ordering::Relaxed);
                let value = i.load(Ordering::Relaxed);
                if value % 1_000_000 == 0 {
                    info!("Processed {value} messages");
                }
            }
            Err(err) => {
                error!("Could not process message {unstructured_msg:?}: {err}");
            }
        }
    }

    inserter.end().await?;
    info!("Processed partition {partition}");

    Ok(())
}
