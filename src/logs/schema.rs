use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};

pub type ChannelLogDateMap = HashMap<u32, HashMap<u32, Vec<u32>>>;

#[derive(Serialize, Deserialize)]
pub struct ChannelLogDate {
    pub year: u32,
    pub month: u32,
    pub day: u32,
}

impl Display for ChannelLogDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}-{}", self.year, self.month, self.day)
    }
}

/*impl ChannelLogDate {
    pub fn from_map(map: ChannelLogDateMap) -> Vec<Self> {
        let mut results = Vec::new();
        for (year, months) in map {
            for (month, days) in months {
                for day in days {
                    let log_date = ChannelLogDate { year, month, day };
                    results.push(log_date);
                }
            }
        }
        results
    }
}*/
