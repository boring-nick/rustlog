use crate::{
    db::schema::StructuredMessage,
    logs::{
        schema::message::{BasicMessage, FullMessage, ResponseMessage},
        stream::LogsStream,
    },
    Result,
};
use futures::{stream::TryChunks, Future, Stream, StreamExt, TryStreamExt};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::pin;
use tracing::error;

const HEADER: &str = r#"{"messages":["#;
const FOOTER: &str = r#"]}"#;
/// Rough estimation of how big a single message is in JSON format
const JSON_MESSAGE_SIZE: usize = 1024;
const CHUNK_SIZE: usize = 3000;

pub enum JsonResponseType {
    Basic,
    Full,
}

pub struct JsonLogsStream {
    inner: TryChunks<LogsStream>,
    is_start: bool,
    is_end: bool,
    response_type: JsonResponseType,
}

impl JsonLogsStream {
    pub fn new(stream: LogsStream, response_type: JsonResponseType) -> Self {
        let inner = stream.try_chunks(CHUNK_SIZE);
        Self {
            inner,
            is_start: true,
            is_end: false,
            response_type,
        }
    }

    fn serialize_chunk<'a, T: ResponseMessage<'a>>(
        &mut self,
        messages: &'a [StructuredMessage<'a>],
    ) -> Vec<u8> {
        let mut messages: VecDeque<T> = messages
            .iter()
            .filter_map(|msg| match T::from_structured(msg) {
                Ok(parsed) => Some(parsed),
                Err(err) => {
                    error!("Could not parse message {msg:?} from DB: {err}");
                    None
                }
            })
            .collect();

        let mut buf = Vec::with_capacity(JSON_MESSAGE_SIZE * messages.len());

        if self.is_start {
            buf.extend_from_slice(HEADER.as_bytes());
            self.is_start = false;

            if let Some(message) = messages.pop_front() {
                serde_json::to_writer(&mut buf, &message).unwrap();
            }
        }

        let serialized_messages: Vec<_> = messages
            .into_par_iter()
            .map(|message| {
                let mut message_buf = Vec::with_capacity(JSON_MESSAGE_SIZE);
                serde_json::to_writer(&mut message_buf, &message).unwrap();
                message_buf
            })
            .collect();

        for message_buf in serialized_messages {
            buf.push(b',');
            buf.extend(message_buf);
        }

        buf
    }
}

impl Stream for JsonLogsStream {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_end {
            return Poll::Ready(None);
        }

        let fut = self.inner.next();
        pin!(fut);

        match fut.poll(cx) {
            Poll::Ready(Some(result)) => match result {
                Ok(chunk) => {
                    let buf = match self.response_type {
                        JsonResponseType::Basic => self.serialize_chunk::<BasicMessage>(&chunk),
                        JsonResponseType::Full => self.serialize_chunk::<FullMessage>(&chunk),
                    };

                    Poll::Ready(Some(Ok(buf)))
                }
                Err(err) => Poll::Ready(Some(Err(err.1))),
            },
            Poll::Ready(None) => {
                self.is_end = true;
                // No lines were retrieved
                if self.is_start {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(FOOTER.as_bytes().to_vec())))
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
