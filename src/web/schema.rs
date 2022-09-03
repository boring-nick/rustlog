use crate::logs::schema::ChannelLogDate;
use serde::{Deserialize, Deserializer, Serialize};
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

#[derive(Deserialize, Debug)]
pub struct LogsParams {
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub json: bool,
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub raw: bool,
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub reverse: bool,
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
