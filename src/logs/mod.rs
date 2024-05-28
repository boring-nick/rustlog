pub mod extract;
pub mod schema;
pub mod stream;

use self::schema::message::ResponseMessage;
use rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use tracing::warn;

pub fn parse_raw(lines: &[String]) -> Vec<tmi::IrcMessageRef<'_>> {
    lines
        .into_par_iter()
        .filter_map(|raw| match tmi::IrcMessageRef::parse(raw) {
            Some(msg) => Some(msg),
            None => {
                warn!("Could not parse message `{raw}`");
                None
            }
        })
        .collect()
}

pub fn parse_messages<'a, T: ResponseMessage<'a>>(
    irc_messages: &'a [tmi::IrcMessageRef<'a>],
) -> impl ParallelIterator<Item = T> + 'a {
    irc_messages
        .par_iter()
        .filter_map(|irc_message| match T::from_irc_message(irc_message) {
            Ok(message) => Some(message),
            Err(err) => {
                warn!("Could not parse message: {err}, irc: {:?}", irc_message);
                None
            }
        })
}
