use std::num::ParseIntError;

use axum::response::{IntoResponse, Response};
use reqwest::StatusCode;
use thiserror::Error;
use tracing::error;
use twitch_api2::helix::ClientRequestError;

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
    #[error("Not found")]
    NotFound,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status_code = match &self {
            Error::Helix(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ParseInt(_) => StatusCode::BAD_REQUEST,
            Error::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Error::NotFound => StatusCode::NOT_FOUND,
            Error::InvalidParam(_) => StatusCode::BAD_REQUEST,
            Error::Clickhouse(error) => {
                error!("DB error: {error}");
                StatusCode::INTERNAL_SERVER_ERROR
            }
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
