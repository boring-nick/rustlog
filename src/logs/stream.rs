mod buffer_response;
mod cursor;
mod multi_query;

pub use buffer_response::FlushBufferResponse;
use cursor::CursorStream;
use multi_query::MultiQueryStream;

use crate::{db::schema::StructuredMessage, error::Error, Result};
use clickhouse::query::RowCursor;
use futures::{Stream, StreamExt};
use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

pub enum LogsStream {
    Cursor(CursorStream),
    MultiQuery(MultiQueryStream),
    Provided(Option<Vec<StructuredMessage<'static>>>),
}

impl LogsStream {
    pub async fn new_cursor(
        cursor: RowCursor<StructuredMessage<'static>>,
        buffer_response: FlushBufferResponse,
    ) -> Result<Self> {
        Ok(Self::Cursor(
            CursorStream::new(cursor, buffer_response).await?,
        ))
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
        buffer_response: FlushBufferResponse,
    ) -> Result<Self> {
        // if streams.is_empty() {
        //     return Err(Error::NotFound);
        // }

        Ok(Self::MultiQuery(MultiQueryStream::new(
            cursors,
            buffer_response,
        )))
    }
}

impl Stream for LogsStream {
    type Item = Result<Vec<StructuredMessage<'static>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut() {
            LogsStream::Cursor(stream) => stream.poll_next_unpin(cx),
            LogsStream::MultiQuery(stream) => stream.poll_next_unpin(cx),
            LogsStream::Provided(values) => Poll::Ready(values.take().map(Ok)),
        }
    }
}
