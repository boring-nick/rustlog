use crate::{error::Error, logs::schema::ChannelLogDate};
use serde::{Deserialize, Serialize};
use std::num::ParseIntError;

#[derive(Serialize)]
pub struct ChannelsList {
    pub channels: Vec<Channel>,
}

#[derive(Serialize)]
pub struct Channel {
    pub name: String,
    #[serde(rename = "userID")]
    pub user_id: String,
}

#[derive(Debug, Deserialize)]
pub enum ChannelIdType {
    #[serde(rename = "channel")]
    Name,
    #[serde(rename = "channelid")]
    Id,
}

#[derive(Deserialize)]
pub struct UserLogsPath {
    pub channel_id_type: ChannelIdType,
    pub channel: String,
    pub user: String,
    pub year: String,
    pub month: String,
}

#[derive(Deserialize)]
pub struct ChannelLogsPath {
    pub channel_id_type: ChannelIdType,
    pub channel: String,
    pub year: String,
    pub month: String,
    pub day: String,
}

#[derive(Deserialize)]
pub struct LogsParams {
    #[serde(default = "default_log_param")]
    pub json: String,
    #[serde(default = "default_log_param")]
    pub raw: String,
    #[serde(default = "default_log_param")]
    pub reverse: String,
}

impl LogsParams {
    pub fn is_json(&self) -> Result<bool, Error> {
        parse_bool(&self.json)
    }

    pub fn is_raw(&self) -> Result<bool, Error> {
        parse_bool(&self.raw)
    }

    pub fn is_reverse(&self) -> Result<bool, Error> {
        parse_bool(&self.reverse)
    }
}

fn default_log_param() -> String {
    "0".to_owned()
}

fn parse_bool(s: &str) -> Result<bool, Error> {
    if s == "1" || s == "true" {
        Ok(true)
    } else if s == "0" || s == "false" {
        Ok(false)
    } else {
        Err(Error::InvalidParam(
            "could not parse param as bool".to_owned(),
        ))
    }
}

impl TryFrom<&ChannelLogsPath> for ChannelLogDate {
    type Error = ParseIntError;

    fn try_from(params: &ChannelLogsPath) -> Result<Self, Self::Error> {
        Ok(Self {
            year: params.year.parse()?,
            month: params.month.parse()?,
            day: params.day.parse()?,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AvailableLogs {
    pub available_logs: Vec<AvailableLogDate>,
}

#[derive(Serialize)]
pub struct AvailableLogDate {
    pub year: String,
    pub month: String,
}

#[derive(Deserialize)]
pub struct AvailableLogsParams {
    #[serde(flatten)]
    pub channel: ChannelParam,
    #[serde(flatten)]
    pub user: UserParam,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserParam {
    User(String),
    UserId(String),
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelParam {
    Channel(String),
    ChannelId(String),
}
