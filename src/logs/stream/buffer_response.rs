use crate::{
    db::{schema::StructuredMessage, writer::FlushBuffer},
    logs::schema::LogRangeParams,
};

#[derive(Debug)]
pub struct FlushBufferResponse {
    pub messages: Vec<StructuredMessage<'static>>,
    pub params: LogRangeParams,
}

impl FlushBufferResponse {
    pub fn empty(params: LogRangeParams) -> Self {
        Self {
            messages: vec![],
            params,
        }
    }

    pub async fn new(
        buffer: &FlushBuffer,
        channel_id: &str,
        user_id: Option<&str>,
        params: LogRangeParams,
    ) -> Self {
        let timestamp_range =
            (params.from.timestamp_millis() as u64)..(params.to.timestamp_millis() as u64);

        let mut messages = if let Some(user_id) = user_id {
            buffer
                .messages_by_channel_and_user(timestamp_range, channel_id, user_id)
                .await
        } else {
            buffer
                .messages_by_channel(timestamp_range, channel_id)
                .await
        };

        if params.logs_params.reverse {
            messages.reverse();
        }

        if let Some(offset) = params.logs_params.offset {
            if offset as usize > messages.len() {
                messages.clear();
            } else {
                messages = messages.into_iter().skip(offset as usize).collect();
            }
        }

        Self { messages, params }
    }

    pub fn normalized_limit(&self) -> Option<u64> {
        let count = self.messages.len() as u64;
        let limit = self.params.logs_params.limit;

        if self.params.logs_params.reverse {
            limit.map(|limit| limit.saturating_sub(count))
        } else {
            limit
        }
    }

    pub fn normalized_offset(&self) -> Option<u64> {
        let count = self.len() as u64;
        let offset = self.params.logs_params.offset;

        if self.params.logs_params.reverse {
            offset.map(|offset| offset.saturating_sub(count))
        } else {
            offset
        }
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_at_start(&self) -> bool {
        self.params.logs_params.reverse
    }
}
