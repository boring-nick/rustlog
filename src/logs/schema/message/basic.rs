use anyhow::anyhow;
use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};
use schemars::JsonSchema;
use serde::Serialize;
use std::{borrow::Cow, collections::HashMap};
use tmi::{Command, Tag};

use super::ResponseMessage;

#[derive(Serialize, JsonSchema, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BasicMessage<'a> {
    pub text: Cow<'a, str>,
    pub display_name: &'a str,
    #[schemars(with = "String")]
    pub timestamp: DateTime<Utc>,
    pub id: &'a str,
    pub tags: HashMap<&'a str, Cow<'a, str>>,
}

impl<'a> ResponseMessage<'a> for BasicMessage<'a> {
    fn from_irc_message(irc_message: &'a tmi::IrcMessageRef<'_>) -> anyhow::Result<Self> {
        let raw_timestamp = irc_message
            .tag(Tag::TmiSentTs)
            .context("Missing timestamp tag")?
            .parse::<i64>()
            .context("Invalid timestamp")?;
        let timestamp = Utc
            .timestamp_millis_opt(raw_timestamp)
            .single()
            .context("Invalid timestamp")?;

        let response_tags = irc_message
            .tags()
            .map(|(key, value)| (key.as_str(), Cow::Borrowed(value)))
            .collect();

        match irc_message.command() {
            Command::Privmsg => {
                let raw_text = irc_message.params().context("Privmsg has no params")?;
                let text = extract_message_text(raw_text);

                let display_name = irc_message
                    .tag(Tag::DisplayName)
                    .context("Missing display name tag")?;
                let id = irc_message.tag(Tag::Id).unwrap_or_default();

                Ok(Self {
                    text: Cow::Borrowed(text),
                    display_name,
                    timestamp,
                    id,
                    tags: response_tags,
                })
            }
            Command::ClearChat => {
                let mut username = None;

                let text = match irc_message.params() {
                    Some(user_login) => {
                        let user_login = extract_message_text(user_login);
                        username = Some(user_login);

                        match irc_message.tag(Tag::BanDuration) {
                            Some(ban_duration) => {
                                format!(
                                    "{user_login} has been timed out for {ban_duration} seconds"
                                )
                            }
                            None => {
                                format!("{user_login} has been banned")
                            }
                        }
                    }
                    None => "Chat has been cleared".to_owned(),
                };

                Ok(Self {
                    text: Cow::Owned(text),
                    display_name: username.unwrap_or_default(),
                    timestamp,
                    id: "",
                    tags: response_tags,
                })
            }
            Command::UserNotice => {
                let system_message = irc_message
                    .tag(Tag::SystemMsg)
                    .context("System message tag missing")?;
                let system_message = tmi::unescape(system_message);

                let text = if let Some(user_message) = irc_message.params() {
                    let user_message = extract_message_text(user_message);
                    Cow::Owned(format!("{system_message} {user_message}"))
                } else {
                    Cow::Owned(system_message)
                };

                let display_name = irc_message
                    .tag(Tag::DisplayName)
                    .context("Missing display name tag")?;
                let id = irc_message.tag(Tag::Id).context("Missing message id tag")?;

                let response_tags = response_tags
                    .into_iter()
                    .map(|(key, value)| (key, Cow::Owned(tmi::unescape(&value))))
                    .collect();

                Ok(Self {
                    text,
                    display_name,
                    timestamp,
                    id,
                    tags: response_tags,
                })
            }
            other => Err(anyhow!("Unsupported message type: {other:?}")),
        }
    }

    fn unescape_tags(&mut self) {
        for value in self.tags.values_mut() {
            let new_value = tmi::unescape(value);
            *value = Cow::Owned(new_value);
        }
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
