pub mod message;

use chrono::{DateTime, Datelike, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::web::schema::LogsParams;

#[derive(Deserialize, JsonSchema)]
pub struct ChannelLogParams {
    #[schemars(with = "String")]
    pub from: DateTime<Utc>,
    #[schemars(with = "String")]
    pub to: DateTime<Utc>,
    #[serde(flatten)]
    pub logs_params: LogsParams,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct UserLogDate {
    pub year: u32,
    pub month: u32,
}

impl UserLogDate {
    pub fn is_current_month(&self) -> bool {
        let current = Utc::now().date_naive();
        current.year() as u32 == self.year && current.month() == self.month
    }
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
