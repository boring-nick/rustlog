use std::num::ParseIntError;

use axum::response::{IntoResponse, Response};
use reqwest::StatusCode;
use thiserror::Error;
use twitch_api2::helix::ClientRequestError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Twitch API error: {0}")]
    HelixError(#[from] ClientRequestError<reqwest::Error>),
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Int parse error: {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("Not found")]
    NotFound,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status_code = match self {
            Error::HelixError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::IoError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ParseIntError(_) => StatusCode::BAD_REQUEST,
            Error::NotFound => StatusCode::NOT_FOUND,
        };

        (status_code, self.to_string()).into_response()
    }
}
