use super::schema::Message;
use clickhouse::{inserter::Inserter, Client};
use std::time::Duration;
use tokio::sync::mpsc::{channel, Sender};
use tracing::{error, info};

const CHANNEL_SIZE: usize = 10_000;

pub async fn create_writer(db: &Client) -> anyhow::Result<Sender<Message<'static>>> {
    let mut inserter = db
        .inserter("message")?
        .with_timeouts(Some(Duration::from_secs(10)), Some(Duration::from_secs(60)))
        .with_max_entries(750_000)
        .with_period(Some(Duration::from_secs(10)));

    let (tx, mut rx) = channel(CHANNEL_SIZE);

    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if let Err(err) = write_message(&mut inserter, message).await {
                error!("Could not write messages: {err}");
            }
        }
    });

    Ok(tx)
}

async fn write_message(
    inserter: &mut Inserter<Message<'static>>,
    message: Message<'static>,
) -> anyhow::Result<()> {
    inserter.write(&message).await?;
    // This doesn't actually write anything to the db unless the thresholds were reached
    let stats = inserter.commit().await?;
    if stats.entries > 0 {
        info!("{} messages have been inserted", stats.entries,);
    }

    Ok(())
}
