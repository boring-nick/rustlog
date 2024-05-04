use aide::{openapi::MediaType, OperationOutput};
use axum::response::{IntoResponse, Response};
use reqwest::StatusCode;
use std::num::ParseIntError;
use thiserror::Error;
use tracing::error;
use twitch_api::helix::ClientRequestError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Twitch API error: {0}")]
    Helix(#[from] ClientRequestError<reqwest::Error>),
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Int parse error: {0}")]
    ParseInt(#[from] ParseIntError),
    #[error("Invalid param: {0}")]
    InvalidParam(String),
    #[error("Internal error")]
    Internal,
    #[error("Database error")]
    Clickhouse(#[from] clickhouse::error::Error),
    #[error("The requested channel has opted out of being logged")]
    ChannelOptedOut,
    #[error("The requested user has opted out of being logged")]
    UserOptedOut,
    #[error("Not found")]
    NotFound,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status_code = match &self {
            Error::Helix(_) | Error::Io(_) | Error::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Clickhouse(error) => {
                error!("DB error: {error}");
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Error::ParseInt(_) | Error::InvalidParam(_) => StatusCode::BAD_REQUEST,
            Error::ChannelOptedOut | Error::UserOptedOut => StatusCode::FORBIDDEN,
            Error::NotFound => StatusCode::NOT_FOUND,
        };

        (status_code, self.to_string()).into_response()
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        error!("Error: {err}");
        Self::Internal
    }
}

impl OperationOutput for Error {
    type Inner = Self;

    fn operation_response(
        _: &mut aide::gen::GenContext,
        _: &mut aide::openapi::Operation,
    ) -> Option<aide::openapi::Response> {
        Some(aide::openapi::Response {
            description: "Error response".into(),
            content: [("text/plain".into(), MediaType::default())]
                .into_iter()
                .collect(),
            ..Default::default()
        })
    }

    fn inferred_responses(
        ctx: &mut aide::gen::GenContext,
        operation: &mut aide::openapi::Operation,
    ) -> Vec<(Option<u16>, aide::openapi::Response)> {
        if let Some(res) = Self::operation_response(ctx, operation) {
            vec![
                (
                    Some(400),
                    aide::openapi::Response {
                        description: "The request is invalid".to_owned(),
                        ..res.clone()
                    },
                ),
                (
                    Some(403),
                    aide::openapi::Response {
                        description: "Channel or user has opted out".to_owned(),
                        ..res.clone()
                    },
                ),
                (
                    Some(404),
                    aide::openapi::Response {
                        description: "The requested data was not found".to_owned(),
                        ..res.clone()
                    },
                ),
                (
                    Some(500),
                    aide::openapi::Response {
                        description: "An internal server error occured".to_owned(),
                        ..res
                    },
                ),
            ]
        } else {
            Vec::new()
        }
    }
}
