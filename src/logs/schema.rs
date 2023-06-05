use anyhow::{anyhow, Context};
use chrono::TimeZone;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_repr::Serialize_repr;
use std::str::FromStr;
use std::{collections::HashMap, fmt::Display};
use strum::EnumString;
use twitch_irc::message::{IRCMessage, IRCPrefix};

const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ChannelLogDate {
    pub year: u32,
    pub month: u32,
    pub day: u32,
}

impl Display for ChannelLogDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{:0>2}-{:0>2}", self.year, self.month, self.day)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct UserLogDate {
    pub year: u32,
    pub month: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserIdentifier<'a> {
    User(&'a str),
    UserId(&'a str),
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelIdentifier<'a> {
    Channel(&'a str),
    ChannelId(&'a str),
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Message<'a> {
    pub text: &'a str,
    pub username: &'a str,
    pub display_name: &'a str,
    pub channel: &'a str,
    #[schemars(with = "String")]
    pub timestamp: DateTime<Utc>,
    pub id: &'a str,
    pub raw: &'a str,
    #[schemars(with = "i8")]
    pub r#type: MessageType,
    pub tags: HashMap<&'a str, &'a str>,
}

#[derive(Serialize_repr, EnumString)]
#[repr(i8)]
#[strum(serialize_all = "UPPERCASE")]
pub enum MessageType {
    // Whisper = 0,
    PrivMsg = 1,
    ClearChat = 2,
    // RoomState = 3,
    UserNotice = 4,
    // UserState = 5,
    // Notice = 6,
    ClearMsg = 13,
}

impl<'a> Message<'a> {
    // pub fn parse_from_raw_irc(raw: String) -> anyhow::Result<Self> {
    //     let irc_message = IRCMessage::parse(&raw)
    //         .with_context(|| format!("Could not parse {raw} as an irc message"))?;
    //     let tags = irc_message
    //         .tags
    //         .clone()
    //         .0
    //         .into_iter()
    //         .map(|(key, value)| (key, value.unwrap_or_default()))
    //         .collect();

    //     // TODO: dont use ServerMessage parsing, manually get required tags and match message type
    //     // Needed because some old logs arent valid PrivmsgMessages in modern twitch-irc-rs
    //     let server_message = ServerMessage::try_from(irc_message).with_context(|| {
    //         format!("Could not parse irc message from {raw} as a server message")
    //     })?;

    //     match server_message {
    //         ServerMessage::Privmsg(pm) => Ok(Message {
    //             text: pm.message_text,
    //             username: pm.sender.login,
    //             display_name: pm.sender.name,
    //             channel: pm.channel_login,
    //             timestamp: pm.server_timestamp,
    //             id: pm.message_id,
    //             raw,
    //             r#type: MessageType::PrivMsg,
    //             tags,
    //         }),
    //         ServerMessage::ClearChat(clear_chat) => {
    //             let (text, username) = match clear_chat.action {
    //                 ClearChatAction::ChatCleared => {
    //                     ("chat has been cleared".to_owned(), String::new())
    //                 }
    //                 ClearChatAction::UserBanned { user_login, .. } => {
    //                     (format!("{user_login} has been banned"), user_login)
    //                 }
    //                 ClearChatAction::UserTimedOut {
    //                     user_login,
    //                     timeout_length,
    //                     ..
    //                 } => {
    //                     let seconds = timeout_length.as_secs();
    //                     (
    //                         format!("{user_login} has been timed out for {seconds} seconds"),
    //                         user_login,
    //                     )
    //                 }
    //             };

    //             Ok(Message {
    //                 text,
    //                 display_name: username.clone(),
    //                 username,
    //                 channel: clear_chat.channel_login,
    //                 timestamp: clear_chat.server_timestamp,
    //                 id: String::new(),
    //                 raw,
    //                 r#type: MessageType::ClearChat,
    //                 tags,
    //             })
    //         }
    //         ServerMessage::ClearMsg(clear_msg) => {
    //             let message = &clear_msg.message_text;
    //             let sender = &clear_msg.sender_login;
    //             let text = format!("message `{message}` from `{sender}` has been deleted");

    //             Ok(Message {
    //                 text,
    //                 username: clear_msg.sender_login,
    //                 display_name: String::new(),
    //                 channel: clear_msg.channel_login,
    //                 timestamp: clear_msg.server_timestamp,
    //                 id: clear_msg.message_id,
    //                 raw,
    //                 r#type: MessageType::ClearMsg,
    //                 tags,
    //             })
    //         }
    //         ServerMessage::UserNotice(user_notice) => {
    //             let mut text = user_notice.system_message;
    //             if let Some(user_message) = &user_notice.message_text {
    //                 text.push(' ');
    //                 text.push_str(user_message);
    //             }

    //             Ok(Message {
    //                 text,
    //                 username: user_notice.sender.login,
    //                 display_name: user_notice.sender.name,
    //                 channel: user_notice.channel_login,
    //                 timestamp: user_notice.server_timestamp,
    //                 id: user_notice.message_id,
    //                 raw,
    //                 r#type: MessageType::UserNotice,
    //                 tags,
    //             })
    //         }
    //         other => Err(anyhow!(
    //             "Unsupported message type: {}",
    //             other.source().command
    //         )),
    //     }
    // }

    pub fn from_irc_message(irc_message: &'a IRCMessage, raw: &'a str) -> anyhow::Result<Self> {
        let tags = irc_message
            .tags
            .0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_deref().unwrap_or_default()))
            .collect::<HashMap<_, _>>();

        let message_type =
            MessageType::from_str(&irc_message.command).context("Unrecognized command")?;

        let channel = irc_message
            .params
            .get(0)
            .map(|item| item.as_str())
            .unwrap_or("<missing channel param>");
        let text = irc_message
            .params
            .get(1)
            .map(|item| item.as_str())
            .unwrap_or("<missing text param>");

        let display_name = extract_tag(&tags, "display-name");
        let username = try_get_prefix_nickname(irc_message)?;
        let id = extract_tag(&tags, "id");

        let raw_timestamp = extract_tag(&tags, "tmi-sent-ts")
            .parse::<i64>()
            .context("Invalid timestamp")?;
        let timestamp = Utc
            .timestamp_millis_opt(raw_timestamp)
            .single()
            .context("Invalid timestamp")?;

        Ok(Self {
            text,
            username,
            display_name,
            channel,
            timestamp,
            id,
            raw,
            r#type: message_type,
            tags,
        })
    }
}

impl<'a> Display for Message<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let timestamp = self.timestamp.format(TIMESTAMP_FORMAT);
        let channel = &self.channel;
        let username = &self.username;
        let text = &self.text;

        if !username.is_empty() {
            write!(f, "[{timestamp}] {channel} {username}: {text}")
        } else {
            write!(f, "[{timestamp}] {channel} {text}")
        }
    }
}

fn extract_tag<'a>(tags: &HashMap<&'a str, &'a str>, key: &str) -> &'a str {
    tags.get(key).copied().unwrap_or_default()
}

fn try_get_prefix_nickname(msg: &IRCMessage) -> anyhow::Result<&str> {
    match &msg.prefix {
        None => Err(anyhow!("Missing prefix on a message")),
        Some(IRCPrefix::HostOnly { host: _ }) => Err(anyhow!("Missing nickname on a message")),
        Some(IRCPrefix::Full {
            nick,
            user: _,
            host: _,
        }) => Ok(nick),
    }
}
