mod basic;
mod full;

pub use basic::BasicMessage;
pub use full::FullMessage;

use serde::Serialize;

use crate::db::schema::StructuredMessage;

pub trait ResponseMessage<'a>: Sized + Send + Serialize + Unpin {
    fn from_structured(msg: &'a StructuredMessage<'a>) -> anyhow::Result<Self>;

    fn from_irc_message(msg: &'a tmi::IrcMessageRef<'_>) -> anyhow::Result<Self>;

    fn unescape_tags(&mut self);
}
