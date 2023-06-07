use twitch::Tag;
use twitch_irc::message::IRCMessage;

pub trait MessageWithTags {
    fn get_tag(&self, key: &Tag) -> Option<&str>;
}

impl MessageWithTags for IRCMessage {
    fn get_tag(&self, key: &Tag) -> Option<&str> {
        self.tags
            .0
            .get(key.as_str())
            .and_then(|value| value.as_deref())
    }
}

impl MessageWithTags for twitch::Message {
    fn get_tag(&self, key: &Tag) -> Option<&str> {
        self.tags().and_then(|tags| tags.get(key).copied())
    }
}

pub fn extract_channel_and_user_from_raw<'a, T: MessageWithTags>(
    msg: &'a T,
) -> Option<(&str, Option<&str>)> {
    msg.get_tag(&Tag::RoomId).map(|channel_id| {
        let user_id = msg
            .get_tag(&Tag::UserId)
            .or_else(|| msg.get_tag(&Tag::TargetUserId));
        (channel_id, user_id)
    })
}

pub fn extract_raw_timestamp<'a, T: MessageWithTags>(msg: &'a T) -> Option<u64> {
    msg.get_tag(&Tag::TmiSentTs)
        .and_then(|raw_timestamp| raw_timestamp.parse().ok())
}
