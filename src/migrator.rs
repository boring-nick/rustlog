use crate::{
    app::App,
    logs::{schema::ChannelLogDateMap, COMPRESSED_CHANNEL_FILE, UNCOMPRESSED_CHANNEL_FILE},
    reindexer::reindex_channel,
};
use flate2::bufread::GzDecoder;
use std::{
    fs::{self, read_dir, DirEntry, File, ReadDir},
    io::{self, BufReader, BufWriter, Write},
    time::Instant,
};
use tracing::{debug, info};

// All of the IO operations are blocking (except reindex) as opposed to the rest of the prorgam to use `io::copy` from the decompressor

pub async fn run(app: &App<'_>, channels: &[String], cleanup: bool) -> anyhow::Result<()> {
    let started_at = Instant::now();

    for channel_id in channels {
        let available_logs = app
            .logs
            .get_available_channel_logs(channel_id, true)
            .await?;

        decompress(app, &available_logs, channel_id, cleanup).await?;
        reindex_channel(app, &available_logs, channel_id).await?;

        if cleanup {
            remove_user_logs(app, channel_id).await?;
        }
    }

    let elapsed = started_at.elapsed();
    info!("Migration finished in {elapsed:?}");

    Ok(())
}

async fn decompress(
    app: &App<'_>,
    available_logs: &ChannelLogDateMap,
    channel_id: &str,
    cleanup: bool,
) -> anyhow::Result<()> {
    for (year, months) in available_logs {
        for (month, days) in months {
            for day in days {
                let day_path = app
                    .logs
                    .root_path
                    .join(channel_id)
                    .join(&year.to_string())
                    .join(&month.to_string())
                    .join(&day.to_string());

                let compressed_file_path = day_path.join(COMPRESSED_CHANNEL_FILE);
                let uncompressed_file_path = day_path.join(UNCOMPRESSED_CHANNEL_FILE);

                if !compressed_file_path.exists() {
                    debug!("{compressed_file_path:?} doesn't exist, skipping");
                    continue;
                }

                info!("Decompressing logs for channel {channel_id}, {year}-{month}-{day}");

                let compressed_file = File::open(&compressed_file_path)?;
                let compressed_reader = BufReader::new(compressed_file);

                let mut gz = GzDecoder::new(compressed_reader);

                let uncompressed_file = File::create(&uncompressed_file_path)?;
                let mut uncompressed_writer = BufWriter::new(uncompressed_file);

                io::copy(&mut gz, &mut uncompressed_writer)?;

                uncompressed_writer.flush()?;

                if cleanup {
                    drop(gz);

                    info!("Deleting compressed file");
                    fs::remove_file(&compressed_file_path)?;
                }
            }
        }
    }

    Ok(())
}

async fn remove_user_logs(app: &App<'_>, channel_id: &str) -> anyhow::Result<()> {
    let channel_path = app.logs.root_path.join(channel_id);

    let years = filter_dir_entries(read_dir(channel_path)?);
    for year_entry in years {
        let months = filter_dir_entries(read_dir(year_entry.path())?);

        for month_entry in months {
            let potential_user_entries = read_dir(month_entry.path())?;

            for potential_user_entry in potential_user_entries {
                let potential_user_entry = potential_user_entry?;
                let name = potential_user_entry
                    .file_name()
                    .into_string()
                    .expect("invalid day/user entry file name");
                let metadata = potential_user_entry.metadata()?;

                if metadata.is_file() && (name.ends_with(".txt") || name.ends_with(".txt.gz")) {
                    info!(
                        "Removing unneeded user entry {:?}",
                        potential_user_entry.path()
                    );
                    fs::remove_file(potential_user_entry.path())?;
                }
            }
        }
    }

    Ok(())
}

fn filter_dir_entries(read_dir: ReadDir) -> impl Iterator<Item = DirEntry> {
    read_dir
        .filter_map(|result| result.ok())
        .filter(|entry| entry.metadata().map_or(false, |metadata| metadata.is_dir()))
}
