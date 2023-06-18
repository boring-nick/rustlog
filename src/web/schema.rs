use super::responders::logs::LogsResponseType;
use crate::logs::schema::{ChannelLogDate, UserLogDate};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use std::{fmt::Display, num::ParseIntError};

#[derive(Serialize, JsonSchema)]
pub struct ChannelsList {
    pub channels: Vec<Channel>,
}

#[derive(Serialize, JsonSchema)]
pub struct Channel {
    pub name: String,
    #[serde(rename = "userID")]
    pub user_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub enum ChannelIdType {
    #[serde(rename = "channel")]
    Name,
    #[serde(rename = "channelid")]
    Id,
}

impl Display for ChannelIdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ChannelIdType::Name => "channel",
            ChannelIdType::Id => "channelid",
        };
        f.write_str(s)
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct UserLogsPath {
    #[serde(flatten)]
    pub channel_info: LogsPathChannel,
    pub user: String,
    pub year: String,
    pub month: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct ChannelLogsPath {
    #[serde(flatten)]
    pub channel_info: LogsPathChannel,
    #[serde(flatten)]
    pub date: ChannelLogDatePath,
}

#[derive(Deserialize, JsonSchema)]
pub struct ChannelLogDatePath {
    pub year: String,
    pub month: String,
    pub day: String,
}

impl TryFrom<ChannelLogDatePath> for ChannelLogDate {
    type Error = ParseIntError;

    fn try_from(value: ChannelLogDatePath) -> Result<Self, Self::Error> {
        Ok(Self {
            year: value.year.parse()?,
            month: value.month.parse()?,
            day: value.day.parse()?,
        })
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct LogsPathChannel {
    pub channel_id_type: ChannelIdType,
    pub channel: String,
}

#[derive(Deserialize, Debug, JsonSchema)]
pub struct LogsParams {
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub json: bool,
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub raw: bool,
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub reverse: bool,
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub ndjson: bool,
}

impl LogsParams {
    pub fn response_type(&self) -> LogsResponseType {
        if self.raw {
            LogsResponseType::Raw
        } else if self.json {
            LogsResponseType::Json
        } else if self.ndjson {
            LogsResponseType::NdJson
        } else {
            LogsResponseType::Text
        }
    }
}

fn deserialize_bool_param<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    match opt.as_deref() {
        Some("1") | Some("true") | Some("") => Ok(true),
        _ => Ok(false),
    }
}

impl TryFrom<&UserLogsPath> for UserLogDate {
    type Error = ParseIntError;

    fn try_from(params: &UserLogsPath) -> Result<Self, Self::Error> {
        Ok(Self {
            year: params.year.parse()?,
            month: params.month.parse()?,
        })
    }
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AvailableLogs {
    pub available_logs: Vec<AvailableLogDate>,
}

#[derive(Serialize, JsonSchema, Clone)]
pub struct AvailableLogDate {
    pub year: String,
    pub month: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
pub struct AvailableLogsParams {
    #[serde(flatten)]
    pub channel: ChannelParam,
    #[serde(flatten)]
    pub user: Option<UserParam>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum UserParam {
    User(String),
    UserId(String),
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ChannelParam {
    Channel(String),
    ChannelId(String),
}

#[derive(Deserialize, JsonSchema)]
pub struct UserLogPathParams {
    #[serde(flatten)]
    pub channel_id_type: ChannelIdType,
    pub channel: String,
    pub user: String,
}
