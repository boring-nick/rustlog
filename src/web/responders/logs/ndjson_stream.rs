use crate::{
    logs::{
        schema::message::{BasicMessage, ResponseMessage},
        stream::LogsStream,
    },
    Result,
};
use futures::{stream::TryChunks, Future, Stream, StreamExt, TryStreamExt};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::pin;
use tracing::error;

/// Rough estimation of how big a single message is in JSON format
const JSON_MESSAGE_SIZE: usize = 1024;
const CHUNK_SIZE: usize = 3000;

pub struct NdJsonLogsStream {
    inner: TryChunks<LogsStream>,
}

impl NdJsonLogsStream {
    pub fn new(stream: LogsStream) -> Self {
        let inner = stream.try_chunks(CHUNK_SIZE);
        Self { inner }
    }
}

impl Stream for NdJsonLogsStream {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let fut = self.inner.next();
        pin!(fut);

        fut.poll(cx).map(|maybe_result| {
            maybe_result.map(|result| match result {
                Ok(chunk) => {
                    let messages: Vec<BasicMessage> = chunk
                        .iter()
                        .flatten()
                        .filter_map(|msg| match BasicMessage::from_structured(msg) {
                            Ok(parsed) => Some(parsed),
                            Err(err) => {
                                error!("Could not parse message {msg:?} from DB: {err}");
                                None
                            }
                        })
                        .collect();

                    let mut buf = Vec::with_capacity(JSON_MESSAGE_SIZE * messages.len());

                    let serialized_messages: Vec<_> = messages
                        .into_par_iter()
                        .map(|message| {
                            let mut message_buf = Vec::with_capacity(JSON_MESSAGE_SIZE);
                            serde_json::to_writer(&mut message_buf, &message).unwrap();
                            message_buf
                        })
                        .collect();

                    for message_buf in serialized_messages {
                        buf.extend(message_buf);
                        buf.extend(b"\r\n");
                    }

                    Ok(buf)
                }
                Err(err) => Err(err.1),
            })
        })
    }
}
