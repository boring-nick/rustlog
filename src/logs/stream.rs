use crate::{
    db::{schema::StructuredMessage, writer::FlushBuffer},
    error::Error,
    Result,
};
use clickhouse::query::RowCursor;
use futures::{Future, Stream};
use std::{
    ops::{DerefMut, Range},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::pin;

use super::schema::LogRangeParams;

pub struct FlushBufferResponse {
    pub buffer: Option<FlushBuffer>,
    pub channel_id: String,
    pub user_id: Option<String>,
    pub params: LogRangeParams,
}

impl FlushBufferResponse {
    fn timestamp_range(&self) -> Range<u64> {
        (self.params.from.timestamp_millis() as u64)..(self.params.to.timestamp_millis() as u64)
    }

    async fn take_messages(&mut self) -> Option<Vec<StructuredMessage<'static>>> {
        if self.params.logs_params.limit.is_some() || self.params.logs_params.offset.is_some() {
            return None;
        }

        match &self.buffer {
            Some(buffer) => {
                let mut messages = if let Some(user_id) = &self.user_id {
                    buffer
                        .messages_by_channel_and_user(
                            self.timestamp_range(),
                            &self.channel_id,
                            user_id,
                        )
                        .await
                } else {
                    buffer
                        .messages_by_channel(self.timestamp_range(), &self.channel_id)
                        .await
                };

                if self.params.logs_params.reverse {
                    messages.reverse();
                }

                self.buffer = None;
                Some(messages)
            }
            None => None,
        }
    }
}

pub enum LogsStream {
    Cursor {
        cursor: RowCursor<StructuredMessage<'static>>,
        first_item: Option<StructuredMessage<'static>>,
        flush_params: FlushBufferResponse,
    },
    MultiQuery {
        cursors: Vec<RowCursor<StructuredMessage<'static>>>,
        current: usize,
        flush_params: FlushBufferResponse,
    },
    Provided(Option<Vec<StructuredMessage<'static>>>),
}

impl LogsStream {
    pub async fn new_cursor(
        mut cursor: RowCursor<StructuredMessage<'static>>,
        flush_params: FlushBufferResponse,
    ) -> Result<Self> {
        // Prefetch the first row to check that the response is not empty
        let first_item = cursor.next().await?.ok_or_else(|| Error::NotFound)?;
        Ok(Self::Cursor {
            cursor,
            first_item: Some(first_item),
            flush_params,
        })
    }

    pub fn new_provided(messages: Vec<StructuredMessage<'static>>) -> Result<Self> {
        if messages.is_empty() {
            Err(Error::NotFound)
        } else {
            Ok(Self::Provided(Some(messages)))
        }
    }

    pub fn new_multi_query(
        cursors: Vec<RowCursor<StructuredMessage<'static>>>,
        flush_params: FlushBufferResponse,
    ) -> Result<Self> {
        // if streams.is_empty() {
        //     return Err(Error::NotFound);
        // }

        Ok(Self::MultiQuery {
            cursors,
            current: 0,
            flush_params,
        })
    }
}

impl Stream for LogsStream {
    type Item = Result<Vec<StructuredMessage<'static>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut() {
            LogsStream::Cursor {
                cursor,
                first_item,
                flush_params,
            } => {
                if flush_params.params.logs_params.reverse {
                    let fut = flush_params.take_messages();
                    pin!(fut);
                    match fut.poll(cx) {
                        Poll::Ready(Some(messages)) => {
                            if !messages.is_empty() {
                                return Poll::Ready(Some(Ok(messages)));
                            }
                        }
                        Poll::Ready(None) => (),
                        Poll::Pending => return Poll::Pending,
                    }
                }

                if let Some(item) = first_item.take() {
                    Poll::Ready(Some(Ok(vec![item])))
                } else {
                    let fut = cursor.next();
                    pin!(fut);

                    match fut.poll(cx) {
                        Poll::Ready(Ok(Some(msg))) => Poll::Ready(Some(Ok(vec![msg]))),
                        Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err.into()))),
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(Ok(None)) => {
                            let fut = flush_params.take_messages();
                            pin!(fut);
                            fut.poll(cx).map(|option| {
                                Ok(option.filter(|messages| !messages.is_empty())).transpose()
                            })
                        }
                    }
                }
            }
            LogsStream::Provided(msgs) => Poll::Ready(msgs.take().map(Ok)),
            LogsStream::MultiQuery {
                cursors,
                current,
                flush_params,
            } => {
                if flush_params.params.logs_params.reverse {
                    let fut = flush_params.take_messages();
                    pin!(fut);
                    match fut.poll(cx) {
                        Poll::Ready(Some(messages)) => {
                            if !messages.is_empty() {
                                return Poll::Ready(Some(Ok(messages)));
                            }
                        }
                        Poll::Ready(None) => (),
                        Poll::Pending => return Poll::Pending,
                    }
                }

                match cursors.get_mut(*current) {
                    Some(cursor) => {
                        let next_line_poll = {
                            let fut = cursor.next();
                            pin!(fut);
                            fut.poll(cx)
                        };

                        if let Poll::Ready(Ok(None)) = next_line_poll {
                            *current += 1;
                            self.poll_next(cx)
                        } else {
                            next_line_poll.map(|result| {
                                result
                                    .map(|option| option.map(|msg| vec![msg]))
                                    .map_err(|err| err.into())
                                    .transpose()
                            })
                        }
                    }
                    None => {
                        let fut = flush_params.take_messages();
                        pin!(fut);
                        fut.poll(cx).map(|option| {
                            Ok(option.filter(|messages| !messages.is_empty())).transpose()
                        })
                    }
                }
            }
        }
    }
}
