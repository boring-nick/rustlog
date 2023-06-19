use super::{BasicMessage, ResponseMessage};
use anyhow::{anyhow, Context};
use schemars::JsonSchema;
use serde::Serialize;
use serde_repr::Serialize_repr;
use std::fmt::Display;
use strum::EnumString;
use twitch::{Command, Tag};

const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[derive(Serialize, JsonSchema, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FullMessage<'a> {
    #[serde(flatten)]
    pub basic: BasicMessage<'a>,
    pub username: &'a str,
    pub channel: &'a str,
    pub raw: &'a str,
    #[schemars(with = "i8")]
    pub r#type: MessageType,
}

#[derive(Serialize_repr, EnumString, Debug, PartialEq)]
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

impl<'a> ResponseMessage<'a> for FullMessage<'a> {
    fn from_irc_message(irc_message: &'a twitch::Message) -> anyhow::Result<Self> {
        let channel = irc_message
            .channel()
            .context("Missing channel")?
            .trim_start_matches('#');

        let basic = BasicMessage::from_irc_message(irc_message)?;

        match irc_message.command() {
            Command::Privmsg => {
                let username = irc_message
                    .prefix()
                    .context("Message has no prefix")?
                    .nick
                    .context("Missing nickname")?;

                Ok(Self {
                    basic,
                    username,
                    channel,
                    raw: irc_message.raw(),
                    r#type: MessageType::PrivMsg,
                })
            }
            Command::Clearchat => {
                let username = irc_message.params().unwrap_or_default();

                Ok(Self {
                    basic,
                    username,
                    channel,
                    raw: irc_message.raw(),
                    r#type: MessageType::ClearChat,
                })
            }
            Command::UserNotice => {
                let username = irc_message.tag(Tag::Login).context("Missing login tag")?;

                Ok(Self {
                    basic,
                    username,
                    channel,
                    raw: irc_message.raw(),
                    r#type: MessageType::UserNotice,
                })
            }
            other => Err(anyhow!("Unsupported message type: {other:?}")),
        }
    }

    fn unescape_tags(&mut self) {
        self.basic.unescape_tags();
    }
}

impl<'a> Display for FullMessage<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let timestamp = self.basic.timestamp.format(TIMESTAMP_FORMAT);
        let channel = &self.channel;
        let username = &self.username;
        let text = &self.basic.text;

        if !username.is_empty() {
            write!(f, "[{timestamp}] #{channel} {username}: {text}")
        } else {
            write!(f, "[{timestamp}] #{channel} {text}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FullMessage, MessageType};
    use crate::logs::schema::message::{BasicMessage, ResponseMessage};
    use chrono::{TimeZone, Utc};
    use pretty_assertions::assert_eq;
    use std::borrow::Cow;

    #[test]
    fn parse_old_message() {
        let data = "@badges=;color=;display-name=Snusbot;emotes=;mod=0;room-id=22484632;subscriber=0;tmi-sent-ts=1489263601000;turbo=0;user-id=62541963;user-type= :snusbot!snusbot@snusbot.tmi.twitch.tv PRIVMSG #forsen :prasoc won 10 points in roulette and now has 2838 points! forsenPls";
        let irc_message = twitch::Message::parse(data).unwrap();
        let message = FullMessage::from_irc_message(&irc_message).unwrap();
        let expected_message = FullMessage {
            basic: BasicMessage {
                text: Cow::Borrowed(
                    "prasoc won 10 points in roulette and now has 2838 points! forsenPls",
                ),
                display_name: "Snusbot",
                timestamp: Utc.timestamp_millis_opt(1489263601000).unwrap(),
                id: "",
                tags: [
                    ("mod", "0"),
                    ("color", ""),
                    ("badges", ""),
                    ("turbo", "0"),
                    ("subscriber", "0"),
                    ("user-id", "62541963"),
                    ("tmi-sent-ts", "1489263601000"),
                    ("room-id", "22484632"),
                    ("user-type", ""),
                    ("display-name", "Snusbot"),
                    ("emotes", ""),
                ]
                .into_iter()
                .map(|(k, v)| (k, Cow::Borrowed(v)))
                .collect(),
            },
            raw: data,
            r#type: MessageType::PrivMsg,
            username: "snusbot",
            channel: "forsen",
        };

        assert_eq!(message, expected_message);
    }
}
