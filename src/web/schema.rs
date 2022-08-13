use serde::{Deserialize, Serialize};

use crate::logs::schema::ChannelLogDate;

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
    #[serde(flatten)]
    pub channel_log_date: ChannelLogDate,
}
