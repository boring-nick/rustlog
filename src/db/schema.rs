use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

pub const MESSAGES_TABLE: &str = "message";

#[derive(Row, Serialize, Deserialize, Debug)]
pub struct Message<'a> {
    pub channel_id: Cow<'a, str>,
    pub user_id: Cow<'a, str>,
    pub timestamp: u64,
    pub raw: Cow<'a, str>,
}
