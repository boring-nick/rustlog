use crate::{logs::stream::LogsStream, Result};
use futures::{stream::TryChunks, Future, Stream, StreamExt, TryStreamExt};
use std::{
    fmt::Write,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::pin;

const CHUNK_SIZE: usize = 3000;
const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

pub struct TextLogsStream {
    inner: TryChunks<LogsStream>,
}

impl TextLogsStream {
    pub fn new(stream: LogsStream) -> Self {
        let inner = stream.try_chunks(CHUNK_SIZE);
        Self { inner }
    }
}

impl Stream for TextLogsStream {
    type Item = Result<String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let fut = self.inner.next();
        pin!(fut);

        fut.poll(cx).map(|item| {
            item.map(|result| match result {
                Ok(chunk) => {
                    let mut output = String::with_capacity(chunk.len() * 16);

                    for msg in chunk.into_iter().flatten() {
                        let timestamp =
                            chrono::DateTime::from_timestamp_millis(msg.timestamp as i64)
                                .unwrap_or_default()
                                .format(TIMESTAMP_FORMAT);
                        let text = msg.user_friendly_text();
                        let channel = &msg.channel_login;
                        let username = &msg.user_login;

                        if !username.is_empty() {
                            let _ =
                                write!(output, "[{timestamp}] #{channel} {username}: {text}\r\n");
                        } else {
                            let _ = write!(output, "[{timestamp}] #{channel} {text}\r\n");
                        }
                    }

                    Ok(output)
                }
                Err(err) => Err(err.1),
            })
        })
    }
}
