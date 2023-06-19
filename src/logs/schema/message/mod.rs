mod basic;
mod full;

pub use basic::BasicMessage;
pub use full::{FullMessage, MessageType};

use serde::Serialize;

pub trait ResponseMessage<'a>: Sized + Send + Serialize + Unpin {
    fn from_irc_message(msg: &'a twitch::Message) -> anyhow::Result<Self>;

    fn unescape_tags(&mut self);
}
