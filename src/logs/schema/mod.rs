pub mod message;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::web::schema::LogsParams;

#[derive(Deserialize, JsonSchema, Clone, Copy)]
pub struct LogRangeParams {
    #[schemars(with = "String")]
    /// RFC 3339 start date
    pub from: DateTime<Utc>,
    #[schemars(with = "String")]
    /// RFC 3339 end date
    pub to: DateTime<Utc>,
    #[serde(flatten)]
    pub logs_params: LogsParams,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserIdentifier<'a> {
    User(&'a str),
    UserId(&'a str),
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelIdentifier<'a> {
    Channel(&'a str),
    ChannelId(&'a str),
}
