use crate::{db::schema::StructuredMessage, error::Error, Result};
use clickhouse::query::RowCursor;
use futures::{
    stream::{self, Iter},
    Future, Stream,
};
use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    vec::IntoIter,
};
use tokio::pin;

pub enum LogsStream {
    Cursor {
        cursor: RowCursor<StructuredMessage<'static>>,
        first_item: Option<StructuredMessage<'static>>,
    },
    MultiQuery {
        cursors: Vec<RowCursor<StructuredMessage<'static>>>,
        current: usize,
    },
    Provided(Iter<IntoIter<StructuredMessage<'static>>>),
}

impl LogsStream {
    pub async fn new_cursor(mut cursor: RowCursor<StructuredMessage<'static>>) -> Result<Self> {
        // Prefetch the first row to check that the response is not empty
        let first_item = cursor.next().await?.ok_or_else(|| Error::NotFound)?;
        Ok(Self::Cursor {
            cursor,
            first_item: Some(first_item),
        })
    }

    pub fn new_provided(iter: Vec<StructuredMessage<'static>>) -> Result<Self> {
        if iter.is_empty() {
            Err(Error::NotFound)
        } else {
            Ok(Self::Provided(stream::iter(iter)))
        }
    }

    pub fn new_multi_query(cursors: Vec<RowCursor<StructuredMessage<'static>>>) -> Result<Self> {
        // if streams.is_empty() {
        //     return Err(Error::NotFound);
        // }

        Ok(Self::MultiQuery {
            cursors,
            current: 0,
        })
    }
}

impl Stream for LogsStream {
    type Item = Result<StructuredMessage<'static>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut() {
            LogsStream::Cursor { cursor, first_item } => {
                if let Some(item) = first_item.take() {
                    Poll::Ready(Some(Ok(item)))
                } else {
                    let fut = cursor.next();
                    pin!(fut);
                    fut.poll(cx)
                        .map(|result| result.map_err(|err| err.into()).transpose())
                }
            }
            LogsStream::Provided(iter) => Pin::new(iter).poll_next(cx).map(|item| item.map(Ok)),
            LogsStream::MultiQuery { cursors, current } => match cursors.get_mut(*current) {
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
                        next_line_poll.map(|result| result.map_err(|err| err.into()).transpose())
                    }
                }
                None => Poll::Ready(None),
            },
        }
    }
}
