use super::schema::Message;
use crate::ShutdownRx;
use clickhouse::{inserter::Inserter, Client};
use lazy_static::lazy_static;
use prometheus::{register_int_gauge, IntGauge};
use std::time::Duration;
use tokio::{
    sync::mpsc::{channel, Sender},
    task::JoinHandle,
};
use tracing::{debug, error, info};

const CHANNEL_SIZE: usize = 10_000;

lazy_static! {
    static ref BATCH_MSG_COUNT_GAGUE: IntGauge = register_int_gauge!(
        "rustlog_messages_written_per_batch",
        "How many messages are written to the database per batch"
    )
    .unwrap();
}

pub async fn create_writer(
    db: &Client,
    mut shutdown_rx: ShutdownRx,
) -> anyhow::Result<(Sender<Message<'static>>, JoinHandle<()>)> {
    let mut inserter = db
        .inserter("message")?
        .with_timeouts(Some(Duration::from_secs(10)), Some(Duration::from_secs(60)))
        .with_max_entries(750_000)
        .with_period(Some(Duration::from_secs(10)));

    let (tx, mut rx) = channel(CHANNEL_SIZE);

    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(message) = rx.recv() => {
                    if let Err(err) = write_message(&mut inserter, message).await {
                        error!("Could not write messages: {err}");
                    }
                }
                Ok(()) = shutdown_rx.changed() => {
                    info!("Flushing database write buffer");

                    match inserter.end().await {
                        Ok(stats) => {
                            if stats.entries > 0 {
                                info!("Flushed {} messages", stats.entries);
                            }
                        },
                        Err(err) => {
                            error!("Could not flush messages: {err}");
                        },
                    }
                    break;
                }
            }
        }
    });

    Ok((tx, handle))
}

async fn write_message(
    inserter: &mut Inserter<Message<'static>>,
    message: Message<'static>,
) -> anyhow::Result<()> {
    inserter.write(&message).await?;
    // This doesn't actually write anything to the db unless the thresholds were reached
    let stats = inserter.commit().await?;
    if stats.entries > 0 {
        debug!("{} messages have been inserted", stats.entries);
        BATCH_MSG_COUNT_GAGUE.set(stats.entries.try_into().unwrap());
    }
    Ok(())
}
