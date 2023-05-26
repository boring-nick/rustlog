use anyhow::anyhow;
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_repr::Serialize_repr;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
};
use twitch_irc::message::{ClearChatAction, IRCMessage, ServerMessage};

pub type ChannelLogDateMap = BTreeMap<u32, BTreeMap<u32, Vec<u32>>>;
pub type UserLogDateMap = BTreeMap<u32, Vec<u32>>;

const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub text: String,
    pub username: String,
    pub display_name: String,
    pub channel: String,
    pub timestamp: DateTime<Utc>,
    pub id: String,
    pub raw: String,
    pub r#type: MessageType,
    pub tags: HashMap<String, String>,
}

#[derive(Serialize_repr)]
#[repr(i8)]
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

impl Message {
    pub fn parse_from_raw_irc(raw: String) -> anyhow::Result<Self> {
        let irc_message = IRCMessage::parse(&raw)
            .with_context(|| format!("Could not parse {raw} as an irc message"))?;
        let tags = irc_message
            .tags
            .clone()
            .0
            .into_iter()
            .map(|(key, value)| (key, value.unwrap_or_default()))
            .collect();

        // TODO: dont use ServerMessage parsing, manually get required tags and match message type
        // Needed because some old logs arent valid PrivmsgMessages in modern twitch-irc-rs
        let server_message = ServerMessage::try_from(irc_message).with_context(|| {
            format!("Could not parse irc message from {raw} as a server message")
        })?;

        match server_message {
            ServerMessage::Privmsg(pm) => Ok(Message {
                text: pm.message_text,
                username: pm.sender.login,
                display_name: pm.sender.name,
                channel: pm.channel_login,
                timestamp: pm.server_timestamp,
                id: pm.message_id,
                raw,
                r#type: MessageType::PrivMsg,
                tags,
            }),
            ServerMessage::ClearChat(clear_chat) => {
                let (text, username) = match clear_chat.action {
                    ClearChatAction::ChatCleared => {
                        ("chat has been cleared".to_owned(), String::new())
                    }
                    ClearChatAction::UserBanned { user_login, .. } => {
                        (format!("{user_login} has been banned"), user_login)
                    }
                    ClearChatAction::UserTimedOut {
                        user_login,
                        timeout_length,
                        ..
                    } => {
                        let seconds = timeout_length.as_secs();
                        (
                            format!("{user_login} has been timed out for {seconds} seconds"),
                            user_login,
                        )
                    }
                };

                Ok(Message {
                    text,
                    username,
                    display_name: String::new(),
                    channel: clear_chat.channel_login,
                    timestamp: clear_chat.server_timestamp,
                    id: String::new(),
                    raw,
                    r#type: MessageType::ClearChat,
                    tags,
                })
            }
            ServerMessage::ClearMsg(clear_msg) => {
                let message = &clear_msg.message_text;
                let sender = &clear_msg.sender_login;
                let text = format!("message `{message}` from `{sender}` has been deleted");

                Ok(Message {
                    text,
                    username: clear_msg.sender_login,
                    display_name: String::new(),
                    channel: clear_msg.channel_login,
                    timestamp: clear_msg.server_timestamp,
                    id: clear_msg.message_id,
                    raw,
                    r#type: MessageType::ClearMsg,
                    tags,
                })
            }
            ServerMessage::UserNotice(user_notice) => {
                let mut text = user_notice.system_message;
                if let Some(user_message) = &user_notice.message_text {
                    text.push(' ');
                    text.push_str(user_message);
                }

                Ok(Message {
                    text,
                    username: user_notice.sender.login,
                    display_name: user_notice.sender.name,
                    channel: user_notice.channel_login,
                    timestamp: user_notice.server_timestamp,
                    id: user_notice.message_id,
                    raw,
                    r#type: MessageType::UserNotice,
                    tags,
                })
            }
            _ => Err(anyhow!("Unsupported message type")),
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let timestamp = self.timestamp.format(TIMESTAMP_FORMAT);
        let channel = &self.channel;
        let username = &self.username;
        let text = &self.text;

        let base = format!("[{timestamp}] #{channel}");

        if !username.is_empty() {
            write!(f, "{base} {username}: {text}")
        } else {
            write!(f, "{base} {text}")
        }
    }
}
