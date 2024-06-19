use anyhow::Context;
use bitflags::bitflags;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::fmt::Write;
use std::{borrow::Cow, fmt::Debug};
use strum::{Display, EnumString};
use tmi::{IrcMessageRef, Tag};
use uuid::Uuid;

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

    pub fn as_tags(&self) -> impl Iterator<Item = (Tag, &'static str)> {
        [
            Tag::Subscriber,
            Tag::Vip,
            Tag::Mod,
            Tag::Turbo,
            Tag::FirstMsg,
            Tag::ReturningChatter,
            Tag::EmoteOnly,
            Tag::R9K,
            Tag::SubsOnly,
            Tag::Slow,
        ]
        .into_iter()
        .filter_map(|tag| {
            let expected_flag = Self::from_tag(&tag).unwrap();
            if self.contains(expected_flag) {
                Some((tag, "1"))
            } else {
                None
            }
        })
    }
}

#[derive(Row, Serialize, Deserialize, Debug, PartialEq)]
pub struct StructuredMessage<'a> {
    pub channel_id: Cow<'a, str>,
    pub channel_login: Cow<'a, str>,
    pub timestamp: u64,
    #[serde(with = "clickhouse::serde::uuid")]
    id: Uuid,
    pub message_type: MessageType,
    pub user_id: Cow<'a, str>,
    pub user_login: Cow<'a, str>,
    display_name: Cow<'a, str>,
    pub color: Option<u32>,
    pub user_type: Cow<'a, str>,
    pub badges: Vec<Cow<'a, str>>,
    pub badge_info: Cow<'a, str>,
    pub client_nonce: Cow<'a, str>,
    pub emotes: Cow<'a, str>,
    pub automod_flags: Cow<'a, str>,
    text: Cow<'a, str>,
    pub message_flags: MessageFlags,
    pub extra_tags: Vec<(Cow<'a, str>, Cow<'a, str>)>,
}

#[derive(Row, Serialize, Deserialize, Debug)]
pub struct UnstructuredMessage<'a> {
    pub channel_id: &'a str,
    pub user_id: &'a str,
    pub timestamp: u64,
    pub raw: &'a str,
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

        let mut text = irc_message
            .params()
            .unwrap_or_default()
            .trim_start_matches(' ');

        match message_type {
            MessageType::PrivMsg | MessageType::UserNotice => {
                text = text.strip_prefix(':').unwrap_or(text);
            }
            MessageType::ClearChat => {
                if let Some(cleared_user_login) = irc_message.params() {
                    let cleared_user_login = extract_message_text(cleared_user_login);
                    user_login = Cow::Borrowed(cleared_user_login);
                }
            }
            _ => (),
        }
        let text = Cow::Borrowed(text);

        let mut message_flags = MessageFlags::empty();
        let mut extra_tags = Vec::new();
        let mut id = Uuid::nil();
        let mut display_name = Cow::default();
        let mut color = None;
        let mut user_type = Cow::default();
        let mut client_nonce = Cow::default();
        let mut emotes = Cow::default();
        let mut automod_flags = Cow::default();
        let mut badges = Vec::new();
        let mut badge_info = Cow::default();

        for (tag, value) in irc_message.tags() {
            let tag = Tag::parse(tag);
            match tag {
                Tag::Id => {
                    if let Ok(uuid) = Uuid::parse_str(value) {
                        id = uuid;
                    } else {
                        extra_tags
                            .push((Cow::Borrowed(Tag::Id.as_str()), tmi::maybe_unescape(value)));
                    }
                }
                Tag::Login => {
                    user_login = Cow::Borrowed(value);
                }
                Tag::DisplayName => {
                    display_name = tmi::maybe_unescape(value);
                }
                Tag::Color => {
                    let raw_color = value.trim_start_matches('#');
                    color = u32::from_str_radix(raw_color, 16).ok();
                }
                Tag::UserType => {
                    user_type = tmi::maybe_unescape(value);
                }
                Tag::Badges => {
                    badges = value.split(',').map(Cow::Borrowed).collect();
                }
                Tag::BadgeInfo => {
                    badge_info = tmi::maybe_unescape(value);
                }
                Tag::Emotes => {
                    emotes = tmi::maybe_unescape(value);
                }
                Tag::ClientNonce => {
                    client_nonce = tmi::maybe_unescape(value);
                }
                Tag::Flags => {
                    automod_flags = tmi::maybe_unescape(value);
                }
                Tag::RoomId | Tag::UserId | Tag::TmiSentTs | Tag::SentTs => (),
                _ => {
                    if let Some(flag) = MessageFlags::from_tag(&tag) {
                        if value == "1" {
                            message_flags.insert(flag);
                        }
                    } else {
                        extra_tags.push((Cow::Borrowed(tag.as_str()), tmi::maybe_unescape(value)))
                    }
                }
            }
        }

        Ok(Self {
            channel_id: Cow::Borrowed(message.channel_id),
            channel_login: Cow::Borrowed(channel_login),
            timestamp: message.timestamp,
            id,
            message_type,
            message_flags,
            user_id: Cow::Borrowed(message.user_id),
            user_login,
            display_name,
            color,
            user_type,
            badges,
            badge_info,
            client_nonce,
            automod_flags,
            emotes,
            text,
            extra_tags,
        })
    }

    pub fn user_friendly_text(&self) -> Cow<'_, str> {
        match self.message_type {
            MessageType::PrivMsg => Cow::Borrowed(extract_message_text(&self.text)),
            MessageType::ClearChat => match self.text.is_empty() {
                false => {
                    let cleared_user_login = extract_message_text(&self.text);

                    let text = match self
                        .extra_tags
                        .iter()
                        .find(|(tag, _)| tag == Tag::BanDuration.as_str())
                        .map(|(_, value)| value)
                    {
                        Some(ban_duration) => {
                            format!("{cleared_user_login} has been timed out for {ban_duration} seconds")
                        }
                        None => {
                            format!("{cleared_user_login} has been banned")
                        }
                    };
                    Cow::Owned(text)
                }
                true => Cow::Borrowed("Chat has been cleared"),
            },
            MessageType::UserNotice => {
                if let Some(system_message) = self
                    .extra_tags
                    .iter()
                    .find(|(tag, _)| tag == Tag::SystemMsg.as_str())
                    .map(|(_, value)| value)
                {
                    if !self.text.is_empty() {
                        let user_message = extract_message_text(&self.text);
                        Cow::Owned(format!("{system_message} {user_message}"))
                    } else {
                        Cow::Borrowed(system_message)
                    }
                } else {
                    Cow::default()
                }
            }
            _ => Cow::default(),
        }
    }

    pub fn id(&self) -> Option<String> {
        if self.id.is_nil() {
            None
        } else {
            Some(self.id.to_string())
        }
    }

    pub fn display_name(&self) -> &str {
        if !self.display_name.is_empty() {
            &self.display_name
        } else {
            &self.user_login
        }
    }

    pub fn all_tags(&self) -> Vec<(Tag, Cow<'_, str>)> {
        let mut tags = Vec::with_capacity(16);

        tags.push((Tag::TmiSentTs, Cow::Owned(self.timestamp.to_string())));

        tags.extend(
            self.message_flags
                .as_tags()
                .map(|(tag, value)| (tag, Cow::Borrowed(value))),
        );

        if !self.id.is_nil() {
            tags.push((Tag::Id, Cow::Owned(self.id.hyphenated().to_string())));
        }
        if !self.channel_id.is_empty() {
            tags.push((Tag::RoomId, Cow::Borrowed(self.channel_id.as_ref())));
        }
        if !self.user_id.is_empty() {
            tags.push((Tag::UserId, Cow::Borrowed(self.user_id.as_ref())));
        }
        if !self.user_login.is_empty() && self.message_type == MessageType::UserNotice {
            tags.push((Tag::Login, Cow::Borrowed(self.user_login.as_ref())));
        }
        if !self.client_nonce.is_empty() {
            tags.push((Tag::ClientNonce, escape_tag(&self.client_nonce)));
        }
        if !self.display_name.is_empty() {
            tags.push((Tag::DisplayName, escape_tag(&self.display_name)));
        }

        tags.push((
            Tag::Badges,
            Cow::Owned(
                self.badges
                    .iter()
                    .map(|value| escape_tag(value))
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ));
        tags.push((Tag::BadgeInfo, escape_tag(&self.badge_info)));

        if let Some(color) = self.color {
            tags.push((Tag::Color, Cow::Owned(format!("#{color:04X}"))));
        }

        tags.extend([
            (Tag::Flags, escape_tag(&self.automod_flags)),
            (Tag::UserType, Cow::Borrowed(self.user_type.as_ref())),
            (Tag::Emotes, escape_tag(&self.emotes)),
        ]);

        for (tag, value) in &self.extra_tags {
            tags.push((Tag::parse(tag), escape_tag(value)));
        }

        tags
    }

    pub fn to_raw_irc(&self) -> String {
        let tags = self.all_tags();

        let mut out = String::with_capacity(self.text.len() + tags.len() * 4);
        out.push('@');

        for (i, (tag, value)) in tags.iter().enumerate() {
            if i > 0 {
                out.push(';');
            }
            let _ = write!(out, "{tag}={value}");
        }

        match self.message_type {
            MessageType::PrivMsg => {
                let _ = write!(
                    out,
                    " :{name}!{name}@{name}.tmi.twitch.tv",
                    name = self.user_login,
                );
            }
            _ => {
                let _ = write!(out, " :tmi.twitch.tv");
            }
        }

        let _ = write!(
            out,
            " {message_type} #{channel}",
            message_type = self.message_type,
            channel = self.channel_login,
        );

        match self.message_type {
            MessageType::PrivMsg | MessageType::UserNotice => {
                let _ = write!(out, " :{}", self.text);
            }
            _ => {
                if !self.text.is_empty() {
                    let _ = write!(out, " {}", self.text);
                }
            }
        }

        out
    }

    pub fn into_owned(self) -> StructuredMessage<'static> {
        StructuredMessage {
            channel_id: Cow::Owned(self.channel_id.into_owned()),
            channel_login: Cow::Owned(self.channel_login.into_owned()),
            timestamp: self.timestamp,
            id: self.id,
            message_type: self.message_type,
            user_id: Cow::Owned(self.user_id.into_owned()),
            user_login: Cow::Owned(self.user_login.into_owned()),
            display_name: Cow::Owned(self.display_name.into_owned()),
            color: self.color,
            user_type: Cow::Owned(self.user_type.into_owned()),
            badges: self
                .badges
                .into_iter()
                .map(|value| Cow::Owned(value.into_owned()))
                .collect(),
            badge_info: Cow::Owned(self.badge_info.into_owned()),
            client_nonce: Cow::Owned(self.client_nonce.into_owned()),
            emotes: Cow::Owned(self.emotes.into_owned()),
            automod_flags: Cow::Owned(self.automod_flags.into_owned()),
            text: Cow::Owned(self.text.into_owned()),
            message_flags: self.message_flags,
            extra_tags: self
                .extra_tags
                .into_iter()
                .map(|(k, v)| (Cow::Owned(k.into_owned()), Cow::Owned(v.into_owned())))
                .collect(),
        }
    }
}

fn escape_tag(value: &str) -> Cow<'_, str> {
    fn escape(value: &str) -> String {
        let mut out = String::with_capacity(value.len());
        for char in value.chars() {
            match char {
                ';' => out.push_str("\\:"),
                ' ' => out.push_str("\\s"),
                '\\' => out.push_str("\\\\"),
                '\r' => out.push_str("\\r"),
                '\n' => out.push_str("\\n"),
                _ => out.push(char),
            }
        }
        out
    }

    if value.contains(|c| c == ';' || c == ' ' || c == '\\' || c == '\r' || c == '\n') {
        Cow::Owned(escape(value))
    } else {
        Cow::Borrowed(value)
    }
}

#[derive(Serialize_repr, Deserialize_repr, EnumString, Debug, PartialEq, Display, Clone, Copy)]
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

fn extract_message_text(mut message_text: &str) -> &str {
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
    use super::{MessageType, StructuredMessage, UnstructuredMessage};
    use crate::db::schema::MessageFlags;
    use pretty_assertions::assert_eq;
    use tmi::{IrcMessageRef, Tag};
    use uuid::Uuid;

    #[test]
    fn from_unstructured_privmsg() {
        let raw = "@returning-chatter=0;user-id=68136884;user-type=;badges=vip/1,subscriber/60;mod=0;display-name=Supibot;room-id=22484632;flags=;emotes=;first-msg=0;vip=1;tmi-sent-ts=1709251274940;id=272e342c-5864-4c59-b730-25908cdb7f57;subscriber=1;turbo=0;color=#1E90FF;badge-info=subscriber/65 :supibot!supibot@supibot.tmi.twitch.tv PRIVMSG #forsen :+join 󠀀";
        let unstructured = UnstructuredMessage {
            channel_id: "22484632",
            user_id: "68136884",
            timestamp: 1709251274940,
            raw,
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
            client_nonce: "".into(),
            emotes: "".into(),
            automod_flags: "".into(),
            text: "+join 󠀀".into(),
            extra_tags: vec![],
        };

        assert_eq!(expected_message, message);
    }

    #[test]
    fn roundtrip_tags() {
        let raw = "@returning-chatter=0;user-id=68136884;user-type=;badges=vip/1,subscriber/60;mod=0;display-name=Supibot;room-id=22484632;flags=;emotes=;first-msg=0;vip=1;tmi-sent-ts=1709251274940;id=272e342c-5864-4c59-b730-25908cdb7f57;subscriber=1;turbo=0;color=#1E90FF;badge-info=subscriber/65 :supibot!supibot@supibot.tmi.twitch.tv PRIVMSG #forsen :+join 󠀀";
        let unstructured = UnstructuredMessage {
            channel_id: "22484632",
            user_id: "68136884",
            timestamp: 1709251274940,
            raw,
        };

        let message = StructuredMessage::from_unstructured(&unstructured).unwrap();
        let tags = message.all_tags();

        let irc_msg = IrcMessageRef::parse(raw).unwrap();

        for (original_tag, original_value) in irc_msg.tags() {
            let original_tag = Tag::parse(original_tag);
            if MessageFlags::from_tag(&original_tag).is_some() && original_value == "0" {
                continue;
            }

            let value = tags
                .iter()
                .find(|(tag, _)| *tag == original_tag)
                .map(|(_, value)| value)
                .unwrap_or_else(|| panic!("Could not find tag {}", original_tag));
            assert_eq!(original_value, value);
        }
    }

    fn assert_roundtrip(unstructured: UnstructuredMessage) {
        let message = StructuredMessage::from_unstructured(&unstructured).unwrap();
        let converted = message.to_raw_irc();

        let original = IrcMessageRef::parse(unstructured.raw).unwrap();
        let converted = IrcMessageRef::parse(&converted).unwrap();

        for (original_tag, original_value) in original.tags() {
            let original_tag = Tag::parse(original_tag);
            if MessageFlags::from_tag(&original_tag).is_some() && original_value == "0" {
                continue;
            }

            let value = converted
                .tag(original_tag.clone())
                .unwrap_or_else(|| panic!("Could not find tag {}", original_tag));
            println!("Comparing tag {original_tag}");
            assert_eq!(original_value, value);
        }

        assert_eq!(original.prefix(), converted.prefix());
        assert_eq!(original.params(), converted.params());
    }

    #[test]
    fn roundtrip_privmsg() {
        let unstructured = UnstructuredMessage {
            channel_id: "22484632",
            user_id: "68136884",
            timestamp: 1709251274940,
            raw: "@returning-chatter=0;user-id=68136884;user-type=;badges=vip/1,subscriber/60;mod=0;display-name=Supibot;room-id=22484632;flags=;emotes=;first-msg=0;vip=1;tmi-sent-ts=1709251274940;id=272e342c-5864-4c59-b730-25908cdb7f57;subscriber=1;turbo=0;color=#1E90FF;badge-info=subscriber/65 :supibot!supibot@supibot.tmi.twitch.tv PRIVMSG #forsen :+join 󠀀",
        };
        assert_roundtrip(unstructured);
    }

    #[test]
    fn roundtrip_usernotice_sub() {
        let unstructured = UnstructuredMessage {
            channel_id: "22484632",
            user_id: "444158477",
            timestamp: 1686947117960,
            raw: r"@mod=0;id=0a4b7b50-052e-473e-99ee-441f05ce52a7;login=daney___;msg-param-multimonth-duration=0;display-name=daney___;msg-param-sub-plan-name=Channel\sSubscription\s(forsenlol);msg-param-was-gifted=false;subscriber=1;msg-param-cumulative-months=19;flags=;color=#8A2BE2;msg-param-months=0;user-id=444158477;badges=subscriber/12;user-type=;msg-param-should-share-streak=0;msg-id=resub;emotes=;msg-param-sub-plan=1000;room-id=22484632;system-msg=daney___\ssubscribed\sat\sTier\s1.\sThey've\ssubscribed\sfor\s19\smonths!;tmi-sent-ts=1686947117960;msg-param-multimonth-tenure=0;badge-info=subscriber/19 :tmi.twitch.tv USERNOTICE #forsen :Still here? LULE",
        };
        assert_roundtrip(unstructured);
    }

    #[test]
    fn roundtrip_roomstate() {
        let unstructured = UnstructuredMessage {
            channel_id: "118353866",
            user_id: "",
            timestamp: 1686947117960,
            raw: r"@emote-only=0;followers-only=-1;slow=0;subs-only=0;room-id=118353866;r9k=0 :tmi.twitch.tv ROOMSTATE #twitchmedia_qs_1",
        };
        assert_roundtrip(unstructured);
    }
}
