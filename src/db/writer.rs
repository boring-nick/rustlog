use super::schema::StructuredMessage;
use crate::{db::schema::MESSAGES_STRUCTURED_TABLE, ShutdownRx};
use anyhow::{anyhow, Context};
use clickhouse::Client;
use lazy_static::lazy_static;
use prometheus::{register_int_gauge, IntGauge};
use std::{ops::Range, sync::Arc, time::Duration};
use tokio::{
    sync::{
        mpsc::{channel, Sender},
        RwLock,
    },
    task::JoinHandle,
    time::{sleep, Instant},
};
use tracing::{debug, error, info, trace};

const RETRY_COUNT: usize = 20;
const RETRY_INTERVAL_SECONDS: u64 = 5;

lazy_static! {
    static ref BATCH_MSG_COUNT_GAGUE: IntGauge = register_int_gauge!(
        "rustlog_messages_written_per_batch",
        "How many messages are written to the database per batch"
    )
    .unwrap();
}

#[derive(Default, Clone)]
pub struct FlushBuffer {
    messages: Arc<RwLock<Vec<StructuredMessage<'static>>>>,
}

impl FlushBuffer {
    pub async fn messages_by_channel(
        &self,
        time_range: Range<u64>,
        channel_id: &str,
    ) -> Vec<StructuredMessage<'static>> {
        let msgs = self
            .messages
            .read()
            .await
            .iter()
            .filter(|msg| time_range.contains(&msg.timestamp))
            .filter(|msg| msg.channel_id == channel_id)
            .cloned()
            .collect::<Vec<_>>();
        trace!("Read {} messages from flush buffer", msgs.len());
        msgs
    }

    pub async fn messages_by_channel_and_user(
        &self,
        time_range: Range<u64>,
        channel_id: &str,
        user_id: &str,
    ) -> Vec<StructuredMessage<'static>> {
        let msgs = self
            .messages
            .read()
            .await
            .iter()
            .filter(|msg| time_range.contains(&msg.timestamp))
            .filter(|msg| msg.channel_id == channel_id && msg.user_id == user_id)
            .cloned()
            .collect::<Vec<_>>();
        trace!("Read {} messages from flush buffer", msgs.len());
        msgs
    }
}

pub async fn create_writer(
    db: Client,
    mut shutdown_rx: ShutdownRx,
    flush_interval: u64,
) -> anyhow::Result<(
    Sender<StructuredMessage<'static>>,
    FlushBuffer,
    JoinHandle<()>,
)> {
    let (tx, mut rx) = channel(1000);

    let flush_buffer = FlushBuffer::default();
    let flush_buffer_clone = flush_buffer.clone();

    let handle = tokio::spawn(async move {
        let timeout = tokio::time::sleep(Duration::from_secs(flush_interval));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                _ = &mut timeout => {
                    timeout.as_mut().reset(Instant::now() + Duration::from_secs(flush_interval));
                    if let Err(err) = write_chunk_with_retry(&db, &flush_buffer).await {
                        error!("Could not write messages: {err}");
                    }
                }
                Some(msg) = rx.recv() => {
                    flush_buffer.messages.write().await.push(msg);
                }
                Ok(()) = shutdown_rx.changed() => {
                    info!("Flushing database write buffer");

                    if let Err(err) = write_chunk_with_retry(&db, &flush_buffer).await {
                        error!("Could not flush messages: {err}");
                    }

                    break;
                }
            }
        }
    });

    Ok((tx, flush_buffer_clone, handle))
}

async fn write_chunk_with_retry(db: &Client, buffer: &FlushBuffer) -> anyhow::Result<()> {
    for attempt in 1..=RETRY_COUNT {
        match write_chunk(db, buffer).await {
            Ok(()) => {
                if attempt > 1 {
                    debug!("Insert succeeded on attempt {attempt}");
                }
                return Ok(());
            }
            Err(err) => {
                error!("Could not insert chunk: {err:#} (attempt {attempt}/{RETRY_COUNT}, retrying in {RETRY_INTERVAL_SECONDS} seconds)");
                sleep(Duration::from_secs(RETRY_INTERVAL_SECONDS)).await;
            }
        }
    }
    Err(anyhow!(
        "Inserting failed even after {RETRY_COUNT} attempts"
    ))
}

async fn write_chunk(db: &Client, buffer: &FlushBuffer) -> anyhow::Result<()> {
    let messages_read_guard = buffer.messages.read().await;

    let started_at = Instant::now();

    let mut insert = db.insert(MESSAGES_STRUCTURED_TABLE)?;
    for message in messages_read_guard.iter() {
        insert.write(message).await.context("Could not write row")?;
    }
    drop(messages_read_guard);

    let mut messages_write_guard = buffer.messages.write().await;
    insert.end().await.context("Could not end insert")?;

    debug!(
        "{} messages have been inserted (took {}ms)",
        messages_write_guard.len(),
        started_at.elapsed().as_millis()
    );
    BATCH_MSG_COUNT_GAGUE.set(messages_write_guard.len().try_into().unwrap());
    messages_write_guard.clear();

    Ok(())
}
