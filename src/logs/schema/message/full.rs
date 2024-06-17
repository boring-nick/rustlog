use super::{BasicMessage, ResponseMessage};
use crate::db::schema::{MessageType, StructuredMessage};
use schemars::JsonSchema;
use serde::Serialize;

#[derive(Serialize, JsonSchema, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FullMessage<'a> {
    #[serde(flatten)]
    pub basic: BasicMessage<'a>,
    pub username: &'a str,
    pub channel: &'a str,
    pub raw: String,
    #[schemars(with = "i8")]
    pub r#type: MessageType,
}

impl<'a> ResponseMessage<'a> for FullMessage<'a> {
    fn from_structured(msg: &'a StructuredMessage<'a>) -> anyhow::Result<Self> {
        let basic = BasicMessage::from_structured(msg)?;
        Ok(Self {
            basic,
            username: &msg.user_login,
            channel: &msg.channel_login,
            raw: msg.to_raw_irc(),
            r#type: msg.message_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{FullMessage, MessageType};
    use crate::{
        db::schema::{StructuredMessage, UnstructuredMessage},
        logs::schema::message::{BasicMessage, ResponseMessage},
    };
    use chrono::{TimeZone, Utc};
    use pretty_assertions::assert_eq;
    use std::borrow::Cow;

    #[test]
    fn parse_old_message() {
        let data = "@badges=;color=;display-name=Snusbot;emotes=;mod=0;room-id=22484632;subscriber=0;tmi-sent-ts=1489263601000;turbo=0;user-id=62541963;user-type= :snusbot!snusbot@snusbot.tmi.twitch.tv PRIVMSG #forsen :prasoc won 10 points in roulette and now has 2838 points! forsenPls";

        let unstructured = UnstructuredMessage {
            channel_id: "22484632",
            user_id: "62541963",
            timestamp: 1489263601000,
            raw: data,
        };
        let structured = StructuredMessage::from_unstructured(&unstructured).unwrap();

        let message = FullMessage::from_structured(&structured).unwrap();
        let expected_message = FullMessage {
            basic: BasicMessage {
                text: Cow::Borrowed(
                    "prasoc won 10 points in roulette and now has 2838 points! forsenPls",
                ),
                display_name: "Snusbot",
                timestamp: Utc.timestamp_millis_opt(1489263601000).unwrap(),
                id: "".into(),
                tags: [
                    ("display-name", "Snusbot"),
                    ("badges", ""),
                    ("badge-info", ""),
                    ("emotes", ""),
                    ("flags", ""),
                    ("login", "snusbot"),
                    ("user-id", "62541963"),
                    ("tmi-sent-ts", "1489263601000"),
                    ("room-id", "22484632"),
                    ("user-type", ""),
                ]
                .into_iter()
                .map(|(k, v)| (k, Cow::Borrowed(v)))
                .collect(),
            },
            raw: "@tmi-sent-ts=1489263601000;room-id=22484632;user-id=62541963;login=snusbot;display-name=Snusbot;badges=;badge-info=;flags=;user-type=;emotes= :snusbot!snusbot@snusbot.tmi.twitch.tv PRIVMSG #forsen :prasoc won 10 points in roulette and now has 2838 points! forsenPls".to_owned(),
            r#type: MessageType::PrivMsg,
            username: "snusbot",
            channel: "forsen",
        };

        let mut expected_tags = expected_message.basic.tags.iter().collect::<Vec<_>>();
        expected_tags.sort_unstable();

        let mut actual_tags = message.basic.tags.iter().collect::<Vec<_>>();
        actual_tags.sort_unstable();

        assert_eq!(expected_tags, actual_tags);
        assert_eq!(expected_message, message);
    }
}
