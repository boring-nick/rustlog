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
                .all_tags(false)
                .into_iter()
                .map(|(tag, value)| (tag.as_str(), value))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::{StructuredMessage, UnstructuredMessage},
        logs::schema::message::ResponseMessage,
    };

    use super::BasicMessage;

    #[test]
    fn avoid_escaping_tags() {
        let unstructured = UnstructuredMessage {
            channel_id: "22484632",
            user_id: "444158477",
            timestamp: 1686947117960,
            raw: r"@mod=0;id=0a4b7b50-052e-473e-99ee-441f05ce52a7;login=daney___;msg-param-multimonth-duration=0;display-name=daney___;msg-param-sub-plan-name=Channel\sSubscription\s(forsenlol);msg-param-was-gifted=false;subscriber=1;msg-param-cumulative-months=19;flags=;color=#8A2BE2;msg-param-months=0;user-id=444158477;badges=subscriber/12;user-type=;msg-param-should-share-streak=0;msg-id=resub;emotes=;msg-param-sub-plan=1000;room-id=22484632;system-msg=daney___\ssubscribed\sat\sTier\s1.\sThey've\ssubscribed\sfor\s19\smonths!;tmi-sent-ts=1686947117960;msg-param-multimonth-tenure=0;badge-info=subscriber/19 :tmi.twitch.tv USERNOTICE #forsen :Still here? LULE",
        };
        let structured = StructuredMessage::from_unstructured(&unstructured).unwrap();
        let basic = BasicMessage::from_structured(&structured).unwrap();
        assert_eq!(
            "daney___ subscribed at Tier 1. They've subscribed for 19 months!",
            basic.tags.get("system-msg").unwrap()
        );
    }
}
