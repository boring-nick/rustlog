mod join_iter;
mod json_stream;
mod text_stream;

use self::{json_stream::JsonLogsStream, text_stream::TextLogsStream};
use crate::logs::{schema::Message, stream::LogsStream};
use aide::OperationOutput;
use axum::{
    body::StreamBody,
    response::{IntoResponse, Response},
    Json,
};
use futures::TryStreamExt;
use indexmap::IndexMap;
use schemars::JsonSchema;

pub struct LogsResponse {
    pub stream: LogsStream,
    pub response_type: LogsResponseType,
}

pub enum LogsResponseType {
    Raw,
    Text,
    Json,
}

/// Used for schema only, actual serialization is manual
#[derive(JsonSchema)]
pub struct JsonLogsResponse<'a> {
    pub messages: Vec<Message<'a>>,
}

impl IntoResponse for LogsResponse {
    fn into_response(self) -> Response {
        match self.response_type {
            LogsResponseType::Raw => {
                let stream = self.stream.map_ok(|mut line| {
                    line.push('\n');
                    line
                });
                StreamBody::new(stream).into_response()
            }
            LogsResponseType::Text => {
                let text_stream = TextLogsStream::new(self.stream);
                StreamBody::new(text_stream).into_response()
            }
            LogsResponseType::Json => {
                let json_stream = JsonLogsStream::new(self.stream);
                StreamBody::new(json_stream).into_response()
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
