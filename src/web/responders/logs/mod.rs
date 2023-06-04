mod join_iter;

use crate::logs::schema::Message;
use aide::OperationOutput;
use axum::{
    http::HeaderValue,
    response::{IntoResponse, Response},
    Json,
};
use indexmap::IndexMap;
use join_iter::JoinIter;
use mime_guess::mime;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use reqwest::header;
use schemars::JsonSchema;
use serde::Serialize;
use std::time::Instant;
use tracing::{debug, warn};

/// Rough estimation of how big a single message is in JSON format
const JSON_MESSAGE_SIZE: usize = 1024 * 1024;

pub struct LogsResponse {
    pub response_type: LogsResponseType,
    pub reverse: bool,
}

pub enum LogsResponseType {
    Raw(Vec<String>),
    Processed(ProcessedLogs),
}

pub struct ProcessedLogs {
    pub messages: Vec<Message>,
    pub logs_type: ProcessedLogsType,
}

#[derive(Serialize, JsonSchema)]
pub struct JsonLogsResponse {
    pub messages: Vec<Message>,
}

impl ProcessedLogs {
    pub fn parse_raw(lines: Vec<String>, logs_type: ProcessedLogsType) -> Self {
        let messages = lines
            .into_par_iter()
            .filter_map(|line| match Message::parse_from_raw_irc(line) {
                Ok(msg) => Some(msg),
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

                lines.into_iter().join('\n').to_string().into_response()
            }
            LogsResponseType::Processed(processed_logs) => {
                let mut messages = processed_logs.messages;
                if self.reverse {
                    messages.reverse();
                }

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
                    ProcessedLogsType::Json => {
                        let started_at = Instant::now();

                        let json_response = JsonLogsResponse { messages };

                        let mut buf =
                            Vec::with_capacity(JSON_MESSAGE_SIZE * json_response.messages.len());
                        serde_json::to_writer(&mut buf, &json_response)
                            .expect("Serialization error");

                        let response = (
                            [(
                                header::CONTENT_TYPE,
                                HeaderValue::from_static(mime::APPLICATION_JSON.as_ref()),
                            )],
                            buf,
                        )
                            .into_response();

                        debug!(
                            "Building JSON response took {}ms",
                            started_at.elapsed().as_millis()
                        );

                        response
                    }
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
