mod join_iter;

use crate::{error::Error, logs::schema::Message};
use aide::OperationOutput;
use axum::{
    body::StreamBody,
    response::{IntoResponse, Response},
    Json,
};
use futures::stream;
use indexmap::IndexMap;
use join_iter::JoinIter;
use rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use schemars::JsonSchema;
use serde::Serialize;
use std::time::Instant;
use tracing::{debug, warn};
use twitch_irc::message::IRCMessage;

pub struct LogsResponse {
    pub response_type: LogsResponseType,
    pub reverse: bool,
}

pub enum LogsResponseType {
    Raw(Vec<String>),
    Processed(ProcessedLogs),
}

pub struct ProcessedLogs {
    pub messages: Vec<(IRCMessage, String)>,
    pub logs_type: ProcessedLogsType,
}

#[derive(Serialize, JsonSchema)]
pub struct JsonLogsResponse<'a> {
    pub messages: Vec<Message<'a>>,
}

impl ProcessedLogs {
    pub fn parse_raw(lines: Vec<String>, logs_type: ProcessedLogsType) -> Self {
        let messages = lines
            .into_par_iter()
            .filter_map(|raw| match IRCMessage::parse(&raw) {
                Ok(msg) => Some((msg, raw)),
                Err(err) => {
                    warn!("Could not parse message: {err:#}");
                    None
                }
            })
            .collect();

        Self {
            messages,
            logs_type,
        }
    }
}

pub enum ProcessedLogsType {
    Text,
    Json,
}

impl IntoResponse for LogsResponse {
    fn into_response(self) -> Response {
        match self.response_type {
            LogsResponseType::Raw(mut lines) => {
                if self.reverse {
                    lines.reverse();
                }

                let lines = lines
                    .into_iter()
                    .flat_map(|line| vec![Ok::<_, Error>(line), Ok("\n".to_owned())]);

                let stream = stream::iter(lines);
                StreamBody::new(stream).into_response()
            }
            LogsResponseType::Processed(processed_logs) => {
                let mut irc_messages = processed_logs.messages;
                if self.reverse {
                    irc_messages.reverse();
                }

                let messages = irc_messages
                    .par_iter()
                    .filter_map(|(irc_message, raw)| {
                        match Message::from_irc_message(irc_message, raw) {
                            Ok(message) => Some(message),
                            Err(err) => {
                                warn!("Could not parse message: {err}, raw irc: {raw}");
                                None
                            }
                        }
                    })
                    .collect::<Vec<_>>();

                match processed_logs.logs_type {
                    ProcessedLogsType::Text => {
                        let started_at = Instant::now();

                        let text = messages.iter().join('\n').to_string();

                        debug!(
                            "Collecting messages into a response took {}ms",
                            started_at.elapsed().as_millis()
                        );

                        text.into_response()
                    }
                    ProcessedLogsType::Json => Json(JsonLogsResponse { messages }).into_response(),
                }
            }
        }
    }
}

impl OperationOutput for LogsResponse {
    type Inner = Self;

    fn operation_response(
        ctx: &mut aide::gen::GenContext,
        operation: &mut aide::openapi::Operation,
    ) -> Option<aide::openapi::Response> {
        let mut content = IndexMap::with_capacity(2);

        let json_operation_response =
            Json::<JsonLogsResponse>::operation_response(ctx, operation).unwrap();
        content.extend(json_operation_response.content);

        let plain_response = String::operation_response(ctx, operation).unwrap();
        content.extend(plain_response.content);

        Some(aide::openapi::Response {
            description: "Logs response".into(),
            content,
            ..Default::default()
        })
    }

    fn inferred_responses(
        ctx: &mut aide::gen::GenContext,
        operation: &mut aide::openapi::Operation,
    ) -> Vec<(Option<u16>, aide::openapi::Response)> {
        let res = Self::operation_response(ctx, operation).unwrap();

        vec![(Some(200), res)]
    }
}
