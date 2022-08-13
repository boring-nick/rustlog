use super::schema::{
    Channel, ChannelIdType, ChannelLogsParams, ChannelsList, UserIdType, UserLogsParams,
};
use crate::{app::App, config::Config, error::Error};
use axum::{extract::Path, Extension, Json};
use std::sync::Arc;

pub async fn get_channels(
    app: Extension<App<'_>>,
    config: Extension<Arc<Config>>,
) -> Json<ChannelsList> {
    let channels = app
        .get_users(config.channels.clone(), vec![])
        .await
        .unwrap();

    Json(ChannelsList {
        channels: channels
            .into_iter()
            .map(|(user_id, name)| Channel { name, user_id })
            .collect(),
    })
}

pub async fn get_channel_logs(
    app: Extension<App<'_>>,
    Path(ChannelLogsParams {
        channel_id_type,
        channel,
        channel_log_date,
    }): Path<ChannelLogsParams>,
) -> Result<String, Error> {
    let channel_id = match channel_id_type {
        ChannelIdType::Name => {
            let (id, _) = app
                .get_users(vec![], vec![channel])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| Error::NotFound)?;
            id
        }

        ChannelIdType::Id => channel,
    };

    Ok(app
        .logs
        .read_channel(&channel_id, channel_log_date)
        .await?
        .join("\n"))
}

pub async fn get_user_logs(
    app: Extension<App<'_>>,
    Path(UserLogsParams {
        channel_id_type,
        channel,
        user_id_type,
        user,
        year,
        month,
    }): Path<UserLogsParams>,
) -> Result<String, Error> {
    let channel_id = match channel_id_type {
        ChannelIdType::Name => {
            let (id, _) = app
                .get_users(vec![], vec![channel])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| Error::NotFound)?;
            id
        }

        ChannelIdType::Id => channel,
    };

    let user_id = match user_id_type {
        UserIdType::Name => {
            let (id, _) = app
                .get_users(vec![], vec![user])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| Error::NotFound)?;
            id
        }

        UserIdType::Id => user,
    };

    Ok(app
        .logs
        .read_user(&channel_id, &user_id, &year, &month)
        .await?)
}
