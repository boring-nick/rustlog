use super::responders::logs::{JsonResponseType, LogsResponseType};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::Display;

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
pub struct ChannelLogsByDatePath {
    #[serde(flatten)]
    pub channel_info: LogsPathChannel,
    #[serde(flatten)]
    pub date: LogsPathDate,
}

#[derive(Deserialize, JsonSchema)]
pub struct LogsPathDate {
    pub year: String,
    pub month: String,
    pub day: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct LogsPathChannel {
    pub channel_id_type: ChannelIdType,
    pub channel: String,
}

#[derive(Deserialize, Debug, JsonSchema, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct LogsParams {
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub json: bool,
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub json_basic: bool,
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub raw: bool,
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub reverse: bool,
    #[serde(default, deserialize_with = "deserialize_bool_param")]
    pub ndjson: bool,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

impl LogsParams {
    pub fn response_type(&self) -> LogsResponseType {
        if self.raw {
            LogsResponseType::Raw
        } else if self.json_basic {
            LogsResponseType::Json(JsonResponseType::Basic)
        } else if self.json {
            LogsResponseType::Json(JsonResponseType::Full)
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
    Ok(Option::<&str>::deserialize(deserializer)?.is_some())
}

#[derive(Deserialize, Debug, JsonSchema)]
pub struct SearchParams {
    pub q: String,
    #[serde(flatten)]
    pub logs_params: LogsParams,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AvailableLogs {
    pub available_logs: Vec<AvailableLogDate>,
}

#[derive(Serialize, JsonSchema)]
pub struct AvailableLogDate {
    pub year: String,
    pub month: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day: Option<String>,
}

impl Display for AvailableLogDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.year, self.month)?;

        if let Some(day) = &self.day {
            write!(f, "/{day}")?;
        }

        Ok(())
    }
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
    pub channel_id_type: ChannelIdType,
    pub channel: String,
    pub user: String,
}
