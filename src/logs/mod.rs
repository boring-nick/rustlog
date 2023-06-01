pub mod schema;

use twitch_irc::message::IRCMessage;

pub fn extract_channel_and_user_from_raw(
    raw_msg: &'_ IRCMessage,
) -> Option<(&'_ str, Option<&'_ str>)> {
    let tags = &raw_msg.tags.0;
    tags.get("room-id")
        .and_then(|item| item.as_deref())
        .map(|channel_id| {
            let user_id = tags
                .get("user-id")
                .and_then(|user_id| user_id.as_deref())
                .or_else(|| {
                    tags.get("target-user-id")
                        .and_then(|user_id| user_id.as_deref())
                });
            (channel_id, user_id)
        })
}

pub fn extract_timestamp(raw_msg: &'_ IRCMessage) -> Option<u64> {
    let tags = &raw_msg.tags.0;
    tags.get("tmi-sent-ts").and_then(|item| {
        let raw_timestamp = item.as_deref()?;
        raw_timestamp.parse().ok()
    })
}
