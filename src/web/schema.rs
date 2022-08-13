use crate::logs::schema::ChannelLogDate;
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

#[derive(Debug, Deserialize)]
pub enum UserIdType {
    #[serde(rename = "user")]
    Name,
    #[serde(rename = "userid")]
    Id,
}

#[derive(Deserialize)]
pub struct UserLogsParams {
    pub channel_id_type: ChannelIdType,
    pub channel: String,
    pub user_id_type: UserIdType,
    pub user: String,
    pub year: String,
    pub month: String,
}

#[derive(Deserialize)]
pub struct ChannelLogsParams {
    pub channel_id_type: ChannelIdType,
    pub channel: String,
    pub year: String,
    pub month: String,
    pub day: String,
}

impl TryFrom<&ChannelLogsParams> for ChannelLogDate {
    type Error = ParseIntError;

    fn try_from(params: &ChannelLogsParams) -> Result<Self, Self::Error> {
        Ok(Self {
            year: params.year.parse()?,
            month: params.month.parse()?,
            day: params.day.parse()?,
        })
    }
}
