use anyhow::Context;
use bitflags::bitflags;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::borrow::Cow;
use strum::EnumString;
use tmi::{unescape, IrcMessageRef, Tag};
use uuid::Uuid;

pub const MESSAGES_TABLE: &str = "message";
pub const MESSAGES_STRUCTURED_TABLE: &str = "message_structured";

bitflags! {
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
    #[serde(transparent)]
    pub struct MessageFlags: u16 {
        const SUBSCRIBER        = 1;
        const VIP               = 2;
        const MOD               = 4;
        const TURBO             = 8;
        const FIRST_MSG         = 16;
        const RETURNING_CHATTER = 32;
        const EMOTE_ONLY        = 64;
        const R9K               = 128;
        const SUBS_ONLY         = 256;
        const SLOW_MODE         = 512;
    }
}

impl MessageFlags {
    pub fn from_tag(tag: &Tag) -> Option<Self> {
        let value = match tag {
            Tag::Subscriber => Self::SUBSCRIBER,
            Tag::Vip => Self::VIP,
            Tag::Mod => Self::MOD,
            Tag::Turbo => Self::TURBO,
            Tag::FirstMsg => Self::FIRST_MSG,
            Tag::ReturningChatter => Self::RETURNING_CHATTER,
            Tag::EmoteOnly => Self::EMOTE_ONLY,
            Tag::R9K => Self::R9K,
            Tag::SubsOnly => Self::SUBS_ONLY,
            Tag::Slow => Self::SLOW_MODE,
            _ => return None,
        };
        Some(value)
    }
}

#[derive(Row, Serialize, Deserialize, Debug, PartialEq)]
pub struct StructuredMessage<'a> {
    pub channel_id: Cow<'a, str>,
    pub channel_login: Cow<'a, str>,
    pub timestamp: u64,
    #[serde(with = "clickhouse::serde::uuid")]
    pub id: Uuid,
    pub message_type: MessageType,
    pub user_id: Cow<'a, str>,
    pub user_login: Cow<'a, str>,
    pub display_name: Cow<'a, str>,
    pub color: Option<u32>,
    pub user_type: Cow<'a, str>,
    pub badges: Vec<Cow<'a, str>>,
    pub badge_info: Cow<'a, str>,
    pub text: Cow<'a, str>,
    pub message_flags: MessageFlags,
    pub extra_tags: Vec<(Cow<'a, str>, Cow<'a, str>)>,
}

#[derive(Row, Serialize, Deserialize, Debug)]
pub struct UnstructuredMessage<'a> {
    pub channel_id: Cow<'a, str>,
    pub user_id: Cow<'a, str>,
    pub timestamp: u64,
    pub raw: Cow<'a, str>,
}

impl<'a> StructuredMessage<'a> {
    pub fn from_unstructured(message: &'a UnstructuredMessage<'a>) -> anyhow::Result<Self> {
        let irc_message = IrcMessageRef::parse(message.raw.trim().trim_matches('\0'))
            .context("Could not parse message")?;

        let channel_login = irc_message
            .channel()
            .unwrap_or_default()
            .trim_start_matches('#');
        let mut user_login = irc_message
            .prefix()
            .and_then(|prefix| prefix.nick)
            .map(Cow::Borrowed)
            .unwrap_or_default();

        let message_type = MessageType::from_tmi_command(irc_message.command())
            .with_context(|| format!("Unknown message type {}", irc_message.command()))?;

        let text = match message_type {
            MessageType::PrivMsg => {
                if let Some(raw_text) = irc_message.params() {
                    Cow::Borrowed(extract_message_text(raw_text))
                } else {
                    Cow::default()
                }
            }
            MessageType::ClearChat => match irc_message.params() {
                Some(cleared_user_login) => {
                    let cleared_user_login = extract_message_text(cleared_user_login);
                    user_login = Cow::Borrowed(cleared_user_login);

                    let text = match irc_message.tag(Tag::BanDuration) {
                        Some(ban_duration) => {
                            format!("{cleared_user_login} has been timed out for {ban_duration} seconds")
                        }
                        None => {
                            format!("{cleared_user_login} has been banned")
                        }
                    };
                    Cow::Owned(text)
                }
                None => Cow::Borrowed("Chat has been cleared"),
            },
            MessageType::UserNotice => {
                let system_message = irc_message
                    .tag(Tag::SystemMsg)
                    .context("System message tag missing")?;
                let system_message = tmi::unescape(system_message);

                if let Some(user_message) = irc_message.params() {
                    let user_message = extract_message_text(user_message);
                    Cow::Owned(format!("{system_message} {user_message}"))
                } else {
                    Cow::Owned(system_message)
                }
            }
            _ => Cow::default(),
        };

        let mut message_flags = MessageFlags::empty();
        let mut extra_tags = Vec::with_capacity(irc_message.tags().count());
        let mut id = Uuid::nil();
        let mut display_name = String::new();
        let mut color = None;
        let mut user_type = "";
        let mut badges = Vec::new();
        let mut badge_info = String::new();

        for (tag, value) in irc_message.tags() {
            match tag {
                Tag::MessageId | Tag::Id => {
                    if let Ok(uuid) = Uuid::parse_str(value) {
                        id = uuid;
                    }
                }
                Tag::Login => {
                    user_login = Cow::Borrowed(value);
                }
                Tag::DisplayName => {
                    display_name = unescape(value);
                }
                Tag::Color => {
                    let raw_color = value.trim_start_matches('#');
                    color = u32::from_str_radix(raw_color, 16).ok();
                }
                Tag::UserType => {
                    user_type = value;
                }
                Tag::Badges => {
                    badges = value.split(',').map(Cow::Borrowed).collect();
                }
                Tag::BadgeInfo => {
                    badge_info = unescape(value);
                }
                Tag::RoomId | Tag::UserId | Tag::TmiSentTs => (),
                _ => {
                    if let Some(flag) = MessageFlags::from_tag(&tag) {
                        if value == "1" {
                            message_flags.insert(flag);
                        }
                    } else {
                        extra_tags.push((
                            Cow::Borrowed(tag.as_str()),
                            Cow::Owned(tmi::unescape(value)),
                        ))
                    }
                }
            }
        }

        Ok(Self {
            channel_id: Cow::Borrowed(&message.channel_id),
            channel_login: Cow::Borrowed(channel_login),
            timestamp: message.timestamp,
            id,
            message_type,
            message_flags,
            user_id: Cow::Borrowed(&message.user_id),
            user_login,
            display_name: Cow::Owned(display_name),
            color,
            user_type: Cow::Borrowed(user_type),
            badges,
            badge_info: Cow::Owned(badge_info),
            text,
            extra_tags,
        })
    }
}

#[derive(Serialize_repr, Deserialize_repr, EnumString, Debug, PartialEq)]
#[repr(u8)]
#[strum(serialize_all = "UPPERCASE")]
pub enum MessageType {
    Whisper = 0,
    PrivMsg = 1,
    ClearChat = 2,
    RoomState = 3,
    UserNotice = 4,
    UserState = 5,
    Notice = 6,
    Join = 7,
    Part = 8,
    Reconnect = 9,
    Names = 10,
    Ping = 11,
    Pong = 12,
    ClearMsg = 13,
    GlobalUserState = 14,
}

impl MessageType {
    fn from_tmi_command(cmd: tmi::Command) -> Option<Self> {
        use MessageType::*;
        let msg_type = match cmd {
            tmi::Command::Ping => Ping,
            tmi::Command::Pong => Pong,
            tmi::Command::Join => Join,
            tmi::Command::Part => Part,
            tmi::Command::Privmsg => PrivMsg,
            tmi::Command::Whisper => Whisper,
            tmi::Command::ClearChat => ClearChat,
            tmi::Command::ClearMsg => ClearMsg,
            tmi::Command::GlobalUserState => GlobalUserState,
            tmi::Command::Notice => Notice,
            tmi::Command::Reconnect => Reconnect,
            tmi::Command::RoomState => RoomState,
            tmi::Command::UserNotice => UserNotice,
            tmi::Command::UserState => UserState,
            _ => return None,
        };
        Some(msg_type)
    }
}

fn extract_message_text(message_text: &str) -> &str {
    let message_text = message_text.trim_start();
    let mut message_text = message_text.strip_prefix(':').unwrap_or(message_text);

    let is_action =
        message_text.starts_with("\u{0001}ACTION ") && message_text.ends_with('\u{0001}');
    if is_action {
        // remove the prefix and suffix
        message_text = &message_text[8..message_text.len() - 1]
    }

    message_text
}

#[cfg(test)]
mod tests {
    use crate::db::schema::MessageFlags;

    use super::{MessageType, StructuredMessage, UnstructuredMessage};
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    #[test]
    fn from_unstructured_privmsg() {
        let raw = "@returning-chatter=0;user-id=68136884;user-type=;badges=vip/1,subscriber/60;mod=0;display-name=Supibot;room-id=22484632;flags=;emotes=;first-msg=0;vip=1;tmi-sent-ts=1709251274940;id=272e342c-5864-4c59-b730-25908cdb7f57;subscriber=1;turbo=0;color=#1E90FF;badge-info=subscriber/65 :supibot!supibot@supibot.tmi.twitch.tv PRIVMSG #forsen :+join 󠀀";
        let unstructured = UnstructuredMessage {
            channel_id: "22484632".into(),
            user_id: "68136884".into(),
            timestamp: 1709251274940,
            raw: raw.into(),
        };

        let message = StructuredMessage::from_unstructured(&unstructured).unwrap();

        let expected_message = StructuredMessage {
            channel_id: "22484632".into(),
            channel_login: "forsen".into(),
            timestamp: 1709251274940,
            user_id: "68136884".into(),
            id: Uuid::parse_str("272e342c-5864-4c59-b730-25908cdb7f57").unwrap(),
            message_type: MessageType::PrivMsg,
            user_login: "supibot".into(),
            display_name: "Supibot".into(),
            message_flags: MessageFlags::VIP | MessageFlags::SUBSCRIBER,
            color: Some(0x1E90FF),
            user_type: "".into(),
            badges: vec!["vip/1".into(), "subscriber/60".into()],
            badge_info: "subscriber/65".into(),
            text: "+join 󠀀".into(),
            extra_tags: [("flags", ""), ("emotes", "")]
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        };

        assert_eq!(expected_message, message);
    }
}
