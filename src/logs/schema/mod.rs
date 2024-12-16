pub mod message;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Deserialize, JsonSchema, Clone, Copy, Debug)]
pub struct LogRangeParams {
    #[schemars(with = "String")]
    /// RFC 3339 start date
    pub from: Option<DateTime<Utc>>,
    #[schemars(with = "String")]
    /// RFC 3339 end date
    pub to: Option<DateTime<Utc>>,
}

impl LogRangeParams {
    pub fn range(&self) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        self.from.zip(self.to)
    }
}
