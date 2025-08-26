use super::FlushBufferResponse;
use crate::{db::schema::StructuredMessage, Result};
use clickhouse::query::RowCursor;
use futures::{Future, Stream};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::pin;

pub struct MultiQueryStream {
    current: usize,
    cursors: Vec<RowCursor<StructuredMessage<'static>>>,
    buffer_response: Option<FlushBufferResponse>,
    limit: Option<usize>,
    offset: Option<usize>,
    /// How many messages were already processed
    count: usize,
}

impl MultiQueryStream {
    pub fn new(
        cursors: Vec<RowCursor<StructuredMessage<'static>>>,
        buffer_response: FlushBufferResponse,
    ) -> Self {
        let limit = buffer_response.params.limit.map(|value| value as usize);
        let offset = buffer_response.params.offset.map(|value| value as usize);

        Self {
            cursors,
            current: 0,
            buffer_response: Some(buffer_response),
            limit,
            offset,
            count: 0,
        }
    }
}

impl Stream for MultiQueryStream {
    type Item = Result<Vec<StructuredMessage<'static>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(buffer_response) = self.buffer_response.take() {
            if buffer_response.is_at_start() {
                if !buffer_response.messages.is_empty() {
                    self.count += buffer_response.len();
                    return Poll::Ready(Some(Ok(buffer_response.messages)));
                }
            } else {
                // Put it back if not used
                self.buffer_response = Some(buffer_response);
            }
        }

        if let Some(limit) = self.limit {
            if self.count >= limit {
                return Poll::Ready(None);
            }
        }

        let current = self.current;
        match self.cursors.get_mut(current) {
            Some(cursor) => {
                let next_line_poll = {
                    let fut = cursor.next();
                    pin!(fut);
                    fut.poll(cx)
                };

                match next_line_poll {
                    Poll::Ready(Ok(Some(msg))) => {
                        if let Some(offset) = self.offset {
                            if self.count < offset {
                                return self.poll_next(cx);
                            }
                        }

                        self.count += 1;
                        Poll::Ready(Some(Ok(vec![msg])))
                    }
                    Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err.into()))),
                    Poll::Ready(Ok(None)) => {
                        self.current += 1;
                        self.poll_next(cx)
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            None => {
                let response = self
                    .buffer_response
                    .take()
                    .filter(|buffer| !buffer.is_at_start() && !buffer.is_empty())
                    .map(|buffer| match self.limit {
                        Some(limit) => buffer
                            .messages
                            .into_iter()
                            .take(limit.saturating_sub(self.count))
                            .collect(),
                        None => buffer.messages,
                    })
                    .map(Ok);
                Poll::Ready(response)
            }
        }
    }
}
