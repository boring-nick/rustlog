use anyhow::{anyhow, Context};
use axum::{
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use twitch_irc::message::{IRCMessage, ServerMessage};

pub enum LogsResponse {
    Plain(Vec<String>),
    Json(Vec<Message>),
}

#[derive(Serialize)]
pub struct Message {
    pub text: String,
    pub username: String,
    pub display_name: String,
    pub channel: String,
    pub timestamp: DateTime<Utc>,
    pub id: String,
    // pub r#type:
    pub raw: String,
    pub tags: HashMap<String, String>,
}

impl IntoResponse for LogsResponse {
    fn into_response(self) -> Response {
        match self {
            LogsResponse::Plain(lines) => lines.join("\n").into_response(),
            LogsResponse::Json(messages) => Json(json!({ "messages": messages })).into_response(),
        }
    }
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
                id: pm.channel_id,
                raw,
                tags,
            }),
            _ => Err(anyhow!(
                "Given server message {server_message:?} could not be converted"
            )),
        }
    }
}
