use crate::Result;
use clickhouse::Client;
use tracing::{debug, info};

pub async fn run(db: &Client) -> Result<()> {
    create_migrations_table(db).await?;

    run_migration(
        db,
        "1_create_message",
        "
CREATE TABLE IF NOT EXISTS message
(
    channel_id LowCardinality(String),
    user_id String CODEC(ZSTD(5)),
    timestamp DateTime64(3) CODEC (DoubleDelta, ZSTD(5)),
    raw String CODEC(ZSTD(5))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (channel_id, user_id, timestamp)",
    )
    .await?;

    run_migration(
        db,
        "2_add_channel_log_dates_projection",
        "
ALTER TABLE message
ADD PROJECTION channel_log_dates
(SELECT channel_id, toDateTime(toStartOfDay(timestamp)) as date GROUP BY channel_id, date)",
    )
    .await?;

    run_migration(
        db,
        "3_materialize_channel_log_dates_prokection",
        "
ALTER TABLE message
MATERIALIZE PROJECTION channel_log_dates",
    )
    .await?;

    Ok(())
}

async fn run_migration(db: &Client, name: &str, query: &str) -> Result<()> {
    let count = db
        .query("SELECT count(*) FROM __rustlog_migrations WHERE name = ?")
        .bind(name)
        .fetch_one::<u64>()
        .await?;

    if count == 0 {
        info!("Running migration {name}");
        db.query(query).execute().await?;
        db.query("INSERT INTO __rustlog_migrations VALUES (?, now())")
            .bind(name)
            .execute()
            .await?;
    } else {
        debug!("Skipping migration {name}");
    }

    Ok(())
}

async fn create_migrations_table(db: &Client) -> Result<()> {
    db.query(
        "
CREATE TABLE IF NOT EXISTS __rustlog_migrations
(
    name String,
    executed_at DateTime
)
ENGINE = MergeTree
ORDER BY name",
    )
    .execute()
    .await?;
    Ok(())
}
