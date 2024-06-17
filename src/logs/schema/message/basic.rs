use anyhow::Context;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::Serialize;
use std::{borrow::Cow, collections::HashMap};

use crate::db::schema::StructuredMessage;

use super::ResponseMessage;

#[derive(Serialize, JsonSchema, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BasicMessage<'a> {
    pub text: Cow<'a, str>,
    pub display_name: &'a str,
    #[schemars(with = "String")]
    pub timestamp: DateTime<Utc>,
    pub id: Cow<'a, str>,
    pub tags: HashMap<&'a str, Cow<'a, str>>,
}

impl<'a> ResponseMessage<'a> for BasicMessage<'a> {
    fn from_structured(msg: &'a StructuredMessage<'a>) -> anyhow::Result<Self> {
        Ok(Self {
            text: msg.user_friendly_text(),
            display_name: msg.display_name(),
            timestamp: chrono::DateTime::from_timestamp_millis(msg.timestamp.try_into()?)
                .context("Invalid timestamp")?,
            id: Cow::Owned(msg.id().unwrap_or_default()),
            tags: msg
                .all_tags()
                .into_iter()
                .map(|(tag, value)| (tag.as_str(), value))
                .collect(),
        })
    }
}
