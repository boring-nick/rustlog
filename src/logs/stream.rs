use crate::Result;
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
    Cursor(RowCursor<String>),
    Provided(Iter<IntoIter<String>>),
}

impl LogsStream {
    pub fn new_cursor(cursor: RowCursor<String>) -> Self {
        Self::Cursor(cursor)
    }

    pub fn new_provided(iter: Vec<String>) -> Self {
        Self::Provided(stream::iter(iter))
    }
}

impl Stream for LogsStream {
    type Item = Result<String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut() {
            LogsStream::Cursor(cursor) => {
                let fut = cursor.next();
                pin!(fut);
                match fut.poll(cx) {
                    Poll::Ready(result) => match result {
                        Ok(value) => Poll::Ready(value.map(Ok)),
                        Err(err) => Poll::Ready(Some(Err(err.into()))),
                    },
                    Poll::Pending => Poll::Pending,
                }
            }
            LogsStream::Provided(iter) => Pin::new(iter).poll_next(cx).map(|item| item.map(Ok)),
        }
    }
}
