use crate::logs::schema::Message;
use axum::{
    response::{IntoResponse, Response},
    Json,
};
use itertools::Itertools;
use serde_json::json;

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

                lines.join("\n").into_response()
            }
            LogsResponseType::Processed(processed_logs) => {
                let mut messages = processed_logs.messages;
                if self.reverse {
                    messages.reverse();
                }

                match processed_logs.logs_type {
                    ProcessedLogsType::Text => messages.into_iter().join("\n").into_response(),
                    ProcessedLogsType::Json => Json(json!({
                        "messages": messages,
                    }))
                    .into_response(),
                }
            }
        }
    }
}
