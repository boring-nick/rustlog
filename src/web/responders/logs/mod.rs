mod json_stream;
mod ndjson_stream;
mod text_stream;

pub use json_stream::JsonResponseType;

use self::{
    json_stream::JsonLogsStream, ndjson_stream::NdJsonLogsStream, text_stream::TextLogsStream,
};
use crate::logs::{schema::message::FullMessage, stream::LogsStream};
use aide::OperationOutput;
use axum::{
    body::Body,
    http::HeaderValue,
    response::{IntoResponse, IntoResponseParts, Response},
    Json,
};
use futures::TryStreamExt;
use indexmap::IndexMap;
use mime_guess::mime::{APPLICATION_JSON, TEXT_PLAIN_UTF_8};
use reqwest::header::CONTENT_TYPE;
use schemars::JsonSchema;

pub struct LogsResponse {
    pub stream: LogsStream,
    pub response_type: LogsResponseType,
}

pub enum LogsResponseType {
    Raw,
    Text,
    Json(JsonResponseType),
    NdJson,
}

/// Used for schema only, actual serialization is manual
#[derive(JsonSchema)]
pub struct JsonLogsResponse<'a> {
    pub messages: Vec<FullMessage<'a>>,
}

impl IntoResponse for LogsResponse {
    fn into_response(self) -> Response {
        match self.response_type {
            LogsResponseType::Raw => {
                let stream = self.stream.map_ok(|msg| {
                    let mut line = msg.to_raw_irc();
                    line.push_str("\r\n");
                    line
                });

                (
                    set_content_type(&TEXT_PLAIN_UTF_8),
                    Body::from_stream(stream),
                )
                    .into_response()
            }
            LogsResponseType::Text => {
                let stream = TextLogsStream::new(self.stream);
                (
                    set_content_type(&TEXT_PLAIN_UTF_8),
                    Body::from_stream(stream),
                )
                    .into_response()
            }
            LogsResponseType::Json(response_type) => {
                let stream = JsonLogsStream::new(self.stream, response_type);
                (
                    set_content_type(&APPLICATION_JSON),
                    Body::from_stream(stream),
                )
                    .into_response()
            }
            LogsResponseType::NdJson => {
                let stream = NdJsonLogsStream::new(self.stream);
                (
                    set_content_type(&"application/x-ndjson"),
                    Body::from_stream(stream),
                )
                    .into_response()
            }
        }
    }
}

fn set_content_type(content_type: &'static impl AsRef<str>) -> impl IntoResponseParts {
    [(
        CONTENT_TYPE,
        HeaderValue::from_static(content_type.as_ref()),
    )]
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
