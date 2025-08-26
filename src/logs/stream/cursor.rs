use super::buffer_response::FlushBufferResponse;
use crate::{db::schema::StructuredMessage, error::Error, Result};
use clickhouse::query::RowCursor;
use futures::{Future, Stream};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::pin;

pub struct CursorStream {
    cursor: RowCursor<StructuredMessage<'static>>,
    first_item: Option<StructuredMessage<'static>>,
    buffer_response: Option<FlushBufferResponse>,
    limit: Option<usize>,
    /// How many messages were already processed
    count: usize,
}

impl CursorStream {
    pub async fn new(
        mut cursor: RowCursor<StructuredMessage<'static>>,
        buffer_response: FlushBufferResponse,
    ) -> Result<Self> {
        let first_item = if buffer_response.is_empty() {
            // Prefetch the first row to check that the response is not empty
            Some(cursor.next().await?.ok_or_else(|| Error::NotFound)?)
        } else {
            None
        };
        let count = first_item.is_some() as usize;
        let limit = buffer_response.params.limit.map(|value| value as usize);

        Ok(Self {
            cursor,
            first_item,
            buffer_response: Some(buffer_response),
            limit,
            count,
        })
    }
}

impl Stream for CursorStream {
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

        if let Some(item) = self.first_item.take() {
            self.count += 1;
            return Poll::Ready(Some(Ok(vec![item])));
        }

        let poll_result = {
            let fut = self.cursor.next();
            pin!(fut);
            fut.poll(cx)
        };

        match poll_result {
            Poll::Ready(Ok(Some(msg))) => {
                self.count += 1;
                Poll::Ready(Some(Ok(vec![msg])))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(None)) => {
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
