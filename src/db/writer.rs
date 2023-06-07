use super::schema::Message;
use crate::{db::schema::MESSAGES_TABLE, ShutdownRx};
use anyhow::{anyhow, Context};
use clickhouse::Client;
use lazy_static::lazy_static;
use prometheus::{register_int_gauge, IntGauge};
use std::time::Duration;
use tokio::{
    sync::mpsc::{channel, Sender},
    task::JoinHandle,
    time::sleep,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{debug, error, info};

const FLUSH_INTERVAL_SECONDS: u64 = 10;
const CHUNK_CAPACITY: usize = 750_000;
const RETRY_COUNT: usize = 5;
const RETRY_INTERVAL_SECONDS: u64 = 5;

lazy_static! {
    static ref BATCH_MSG_COUNT_GAGUE: IntGauge = register_int_gauge!(
        "rustlog_messages_written_per_batch",
        "How many messages are written to the database per batch"
    )
    .unwrap();
}

pub async fn create_writer(
    db: Client,
    mut shutdown_rx: ShutdownRx,
) -> anyhow::Result<(Sender<Message<'static>>, JoinHandle<()>)> {
    let (tx, rx) = channel(CHUNK_CAPACITY);

    let chunks_stream = futures::StreamExt::enumerate(
        ReceiverStream::new(rx)
            .chunks_timeout(CHUNK_CAPACITY, Duration::from_secs(FLUSH_INTERVAL_SECONDS)),
    );
    let mut chunks_stream = Box::pin(chunks_stream);

    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some((i, messages)) = chunks_stream.next() => {
                    if let Err(err) = write_chunk_with_retry(&db, messages, i).await {
                        error!("Could not write messages: {err}");
                    }
                }
                Ok(()) = shutdown_rx.changed() => {
                    info!("Flushing database write buffer");

                    while let Some((i, chunk)) = chunks_stream.next().await {
                        if let Err(err) = write_chunk_with_retry(&db, chunk, i).await {
                            error!("Could not flush messages: {err}");
                        }
                    }

                    break;
                }
            }
        }
    });

    Ok((tx, handle))
}

async fn write_chunk_with_retry(
    db: &Client,
    messages: Vec<Message<'_>>,
    i: usize,
) -> anyhow::Result<()> {
    for attempt in 1..=RETRY_COUNT {
        match write_chunk(db, &messages, i).await {
            Ok(()) => {
                if attempt > 1 {
                    debug!("Insert succeeded on attempt {attempt}");
                }
                return Ok(());
            }
            Err(err) => {
                error!("Could not insert chunk: {err} (attempt {attempt}/{RETRY_COUNT}, retrying in {RETRY_INTERVAL_SECONDS} seconds)");
                sleep(Duration::from_secs(RETRY_INTERVAL_SECONDS)).await;
            }
        }
    }
    Err(anyhow!(
        "Inserting failed even after {RETRY_COUNT} attempts"
    ))
}

async fn write_chunk(db: &Client, messages: &[Message<'_>], i: usize) -> anyhow::Result<()> {
    let mut insert = db.insert(MESSAGES_TABLE)?;
    for message in messages {
        insert.write(message).await.context("Could not write row")?;
    }
    insert.end().await.context("Could not end insert")?;

    debug!("{} messages have been inserted (chunk {i})", messages.len());
    BATCH_MSG_COUNT_GAGUE.set(messages.len().try_into().unwrap());

    Ok(())
}
